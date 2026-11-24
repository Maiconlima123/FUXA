/**
 * 'runtime.redisBridge': opt-in publisher of runtime events to Redis Streams.
 *
 * Generic, disabled by default and free of any application specific schema
 * beyond a small versioned envelope. External consumers (dashboards,
 * maintenance systems, log pipelines, ...) can read plain JSON records from a
 * stream without shipping a FUXA client.
 *
 * Configuration precedence: persisted server settings > environment > defaults.
 *
 * Persisted settings (editor > Settings > Redis):
 *   enabled, host, port, username, password, tls, db, stream, maxlen, url
 *
 * Environment (headless deployments):
 *   FUXA_REDIS_ENABLED  'true' to enable (default 'false')
 *   FUXA_REDIS_URL      Redis URL (default 'redis://127.0.0.1:6379/0')
 *   FUXA_REDIS_STREAM   stream key (default 'stream:fuxa:events')
 *   FUXA_REDIS_MAXLEN   optional approximate cap for the stream (XADD MAXLEN ~)
 *
 * Capacity options (high tag counts, ~1k-2k changes/s):
 *   FUXA_REDIS_BATCH_ENABLED     'true' to coalesce tag changes (default 'false')
 *   FUXA_REDIS_BATCH_WINDOW_MS   coalescing window, clamped 100..500 (default 200)
 *   FUXA_REDIS_BATCH_MAX         max changes per batch event (default 1000)
 *   FUXA_REDIS_VALUES_ENABLED    maintain the live values key (default 'true')
 *   FUXA_REDIS_VALUES_KEY        live values key (default 'fuxa:catalog:values')
 *   FUXA_REDIS_VALUES_KEY_TYPE   'hash' (default) or 'string' (legacy JSON doc)
 *   FUXA_REDIS_VALUES_FLUSH_MS   values write interval, 100..60000 (default 1000)
 *   FUXA_REDIS_VALUES_MAX_TAGS   in-memory bound for the values map (default 20000)
 *
 * Tag changes:
 *  - batching disabled (default): one `tag.changed` event per tag, exactly as
 *    before this option existed;
 *  - batching enabled: every change is kept in a per-(device,tag) map (last
 *    write wins) and flushed once per window as one event:
 *
 *      { version, eventId, type: 'tag.changed.batch', ts, source: 'fuxa',
 *        payload: { count, changes: [
 *          { deviceId, tagId, value, timestamp, quality }, ... ] } }
 *
 *    A batch never exceeds FUXA_REDIS_BATCH_MAX changes; bigger bursts are
 *    split into several events inside the same window.
 *
 * Live values key:
 *  - `fuxa:catalog:values` is written as a Redis hash (field = tagId, value =
 *    JSON {value,timestamp,quality,deviceId}) at most once per flush interval,
 *    with one HSET per 500 fields, so 10k tags never produce 10k commands;
 *  - `FUXA_REDIS_VALUES_KEY_TYPE=string` keeps the older shape
 *    ({version,count,values:{tagId:{...}}}) as a single SET, for consumers that
 *    still read the key with GET;
 *  - the whole map is rewritten after a (re)connect so a Redis restart does not
 *    lose the snapshot. There is no periodic full rewrite.
 *
 * This bridge only talks to an external Redis-compatible service. It never
 * starts or embeds Redis.
 *
 * Redis is an observability side channel only: every failure is caught and
 * logged, never propagated to the runtime.
 */

'use strict';

const crypto = require('crypto');

const DEFAULT_URL = 'redis://127.0.0.1:6379/0';
const DEFAULT_STREAM = 'stream:fuxa:events';
const DEFAULT_SOURCE = 'fuxa';
const DEFAULT_TEST_TIMEOUT_MS = 3000;
const ERROR_LOG_THROTTLE_MS = 30000;

const DEFAULT_BATCH_WINDOW_MS = 200;
const MIN_BATCH_WINDOW_MS = 100;
const MAX_BATCH_WINDOW_MS = 500;
const DEFAULT_BATCH_MAX = 1000;
const MAX_BATCH_MAX = 100000;

const DEFAULT_VALUES_KEY = 'fuxa:catalog:values';
const DEFAULT_VALUES_FLUSH_MS = 1000;
const MIN_VALUES_FLUSH_MS = 100;
const MAX_VALUES_FLUSH_MS = 60000;
const DEFAULT_VALUES_MAX_TAGS = 20000;
// one HSET carries up to 500 fields (1000 arguments): big enough to keep 10k
// tags down to ~20 commands, small enough to stay well under client limits.
const VALUES_CHUNK_FIELDS = 500;

function isFilled(value) {
    return value !== undefined && value !== null && value !== '';
}

function parseBoolean(value, fallback) {
    if (value === undefined || value === null || value === '') {
        return !!fallback;
    }
    if (typeof value === 'boolean') {
        return value;
    }
    const normalized = String(value).trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) {
        return true;
    }
    if (['0', 'false', 'no', 'off'].includes(normalized)) {
        return false;
    }
    return !!fallback;
}

function parseMaxlen(value) {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

/**
 * Parse an integer and clamp it into [min, max]. Unparseable values fall back
 * to the default, so a typo in an env var never disables a safety limit.
 */
function clampInteger(value, min, max, fallback) {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed)) {
        return fallback;
    }
    if (parsed < min) {
        return min;
    }
    if (parsed > max) {
        return max;
    }
    return parsed;
}

function parseValuesKeyType(value) {
    const normalized = String(value === undefined || value === null ? '' : value).trim().toLowerCase();
    if (normalized === 'string' || normalized === 'json') {
        return 'string';
    }
    return 'hash';
}

function generateEventId() {
    try {
        if (typeof crypto.randomUUID === 'function') {
            return crypto.randomUUID();
        }
    } catch (err) {
        // fall through to the manual id
    }
    return `fuxa-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function buildEvent(type, payload, source) {
    return {
        version: 1,
        eventId: generateEventId(),
        type: type,
        ts: new Date().toISOString(),
        source: source || DEFAULT_SOURCE,
        payload: payload
    };
}

function loadRedisConstructor() {
    const mod = require('ioredis');
    // ioredis v6 exposes both a default and the named Redis export
    return mod.Redis || mod.default || mod;
}

/**
 * Keep only the known persisted keys and normalize their types. Empty values
 * are dropped on purpose so they fall back to environment/defaults.
 */
function normalizeSettings(raw) {
    const input = (raw && typeof raw === 'object') ? raw : {};
    const out = {};
    if (input.enabled !== undefined && input.enabled !== null) {
        out.enabled = parseBoolean(input.enabled);
    }
    if (isFilled(input.host)) {
        out.host = String(input.host).trim();
    }
    if (isFilled(input.port)) {
        out.port = Number.parseInt(input.port, 10);
    }
    if (isFilled(input.username)) {
        out.username = String(input.username).trim();
    }
    if (input.password !== undefined && input.password !== null) {
        out.password = String(input.password);
    }
    if (input.tls !== undefined && input.tls !== null) {
        out.tls = parseBoolean(input.tls);
    }
    if (isFilled(input.db)) {
        out.db = Number.parseInt(input.db, 10);
    }
    if (isFilled(input.stream)) {
        out.stream = String(input.stream).trim();
    }
    if (isFilled(input.maxlen)) {
        out.maxlen = Number.parseInt(input.maxlen, 10);
    }
    if (isFilled(input.url)) {
        out.url = String(input.url).trim();
    }
    return out;
}

/**
 * Validate user supplied settings. Returns a list of human readable errors
 * (empty when valid). Environment values are not validated here.
 */
function validateSettings(raw) {
    const input = (raw && typeof raw === 'object') ? raw : {};
    const errors = [];
    if (isFilled(input.port)) {
        const port = Number(input.port);
        if (!Number.isInteger(port) || port < 1 || port > 65535) {
            errors.push('port must be an integer between 1 and 65535');
        }
    }
    if (isFilled(input.db)) {
        const db = Number(input.db);
        if (!Number.isInteger(db) || db < 0 || db > 65535) {
            errors.push('database must be an integer between 0 and 65535');
        }
    }
    if (isFilled(input.maxlen)) {
        const maxlen = Number(input.maxlen);
        if (!Number.isInteger(maxlen) || maxlen < 0) {
            errors.push('max length must be a non negative integer');
        }
    }
    if (isFilled(input.host) && /[\/@\s]/.test(String(input.host))) {
        errors.push('host must not contain a scheme, credentials or spaces');
    }
    if (isFilled(input.url) && !/^rediss?:\/\//i.test(String(input.url).trim())) {
        errors.push('url must start with redis:// or rediss://');
    }
    return errors;
}

/**
 * Merge incoming settings with the current ones, preserving a stored password
 * when the incoming password is empty.
 */
function mergeSettings(current, incoming) {
    const base = normalizeSettings(current);
    const next = normalizeSettings(incoming);
    if (!next.password && base.password) {
        next.password = base.password;
    }
    // a full URL with credentials is never sent back to the client; keep the
    // stored one when the incoming form has neither a URL nor a host
    if (!next.url && !next.host && base.url) {
        next.url = base.url;
    }
    return next;
}

function redactUrl(url) {
    try {
        const parsed = new URL(url);
        if (parsed.username || parsed.password) {
            parsed.username = parsed.username ? '***' : '';
            parsed.password = parsed.password ? '***' : '';
            return parsed.toString();
        }
        return url;
    } catch (err) {
        return String(url).replace(/\/\/[^@/]+@/, '//***@');
    }
}

function urlHasCredentials(url) {
    try {
        const parsed = new URL(url);
        return !!(parsed.username || parsed.password);
    } catch (err) {
        return /\/\/[^@/]+@/.test(String(url));
    }
}

/**
 * Safe representation for the settings API: never exposes the password.
 */
function toPublicSettings(raw) {
    const out = normalizeSettings(raw);
    const hasPassword = !!out.password || (!!out.url && urlHasCredentials(out.url));
    const hasUrl = !!out.url && urlHasCredentials(out.url);
    delete out.password;
    if (hasUrl) {
        // never round-trip a URL that embeds credentials
        delete out.url;
    }
    if (hasUrl) {
        out.hasUrl = true;
    }
    out.hasPassword = hasPassword;
    return out;
}

function buildRedisUrl(config) {
    config = config || {};
    const protocol = parseBoolean(config.tls) ? 'rediss' : 'redis';
    let host = isFilled(config.host) ? String(config.host).trim() : '127.0.0.1';
    if (host.includes(':') && !host.startsWith('[')) {
        host = `[${host}]`;
    }
    const port = Number.isInteger(Number(config.port)) && Number(config.port) > 0 ? Number(config.port) : 6379;
    const db = Number.isInteger(Number(config.db)) && Number(config.db) >= 0 ? Number(config.db) : 0;
    let auth = '';
    const username = isFilled(config.username) ? String(config.username) : '';
    const password = isFilled(config.password) ? String(config.password) : '';
    if (username || password) {
        auth = encodeURIComponent(username);
        if (password) {
            auth += ':' + encodeURIComponent(password);
        }
        auth += '@';
    }
    return `${protocol}://${auth}${host}:${port}/${db}`;
}

/**
 * Resolve the effective configuration: persisted > env > defaults.
 */
function resolveConfig(settings, env) {
    env = env || {};
    const persisted = normalizeSettings(settings);
    const enabled = persisted.enabled !== undefined
        ? persisted.enabled
        : parseBoolean(env.FUXA_REDIS_ENABLED, false);
    const stream = persisted.stream
        || (isFilled(env.FUXA_REDIS_STREAM) ? String(env.FUXA_REDIS_STREAM).trim() : DEFAULT_STREAM);
    const maxlen = persisted.maxlen !== undefined
        ? parseMaxlen(persisted.maxlen)
        : parseMaxlen(env.FUXA_REDIS_MAXLEN);
    let url;
    if (persisted.host) {
        url = buildRedisUrl(persisted);
    } else if (persisted.url) {
        url = persisted.url;
    } else if (isFilled(env.FUXA_REDIS_URL)) {
        url = String(env.FUXA_REDIS_URL).trim();
    } else {
        url = DEFAULT_URL;
    }
    return {
        enabled: enabled,
        url: url,
        stream: stream,
        maxlen: maxlen,
        source: DEFAULT_SOURCE,
        // capacity options: env only (not persisted in the editor settings yet)
        batchEnabled: parseBoolean(env.FUXA_REDIS_BATCH_ENABLED, false),
        batchWindowMs: clampInteger(env.FUXA_REDIS_BATCH_WINDOW_MS,
            MIN_BATCH_WINDOW_MS, MAX_BATCH_WINDOW_MS, DEFAULT_BATCH_WINDOW_MS),
        batchMax: clampInteger(env.FUXA_REDIS_BATCH_MAX, 1, MAX_BATCH_MAX, DEFAULT_BATCH_MAX),
        valuesEnabled: parseBoolean(env.FUXA_REDIS_VALUES_ENABLED, true),
        valuesKey: isFilled(env.FUXA_REDIS_VALUES_KEY)
            ? String(env.FUXA_REDIS_VALUES_KEY).trim()
            : DEFAULT_VALUES_KEY,
        valuesKeyType: parseValuesKeyType(env.FUXA_REDIS_VALUES_KEY_TYPE),
        valuesFlushMs: clampInteger(env.FUXA_REDIS_VALUES_FLUSH_MS,
            MIN_VALUES_FLUSH_MS, MAX_VALUES_FLUSH_MS, DEFAULT_VALUES_FLUSH_MS),
        valuesMaxTags: clampInteger(env.FUXA_REDIS_VALUES_MAX_TAGS, 1, 1000000, DEFAULT_VALUES_MAX_TAGS)
    };
}

function sanitizeError(err, secret) {
    let message = err && (err.message || String(err));
    if (!message) {
        message = 'unknown error';
    }
    message = String(message).replace(/(rediss?:\/\/)[^\s/]*@/gi, '$1***@');
    if (secret) {
        message = message.split(String(secret)).join('***');
    }
    return message;
}

function withTimeout(promise, timeoutMs) {
    let timer = null;
    const timeout = new Promise(function (resolve, reject) {
        timer = setTimeout(function () {
            reject(new Error('Redis connection timed out'));
        }, timeoutMs);
    });
    return Promise.race([promise, timeout]).finally(function () {
        if (timer) {
            clearTimeout(timer);
        }
    });
}

/**
 * Default client factory. Uses lazy connection and no offline queue so a
 * missing Redis never buffers commands nor blocks the event loop.
 */
function createRedisClient(config, logger) {
    const Redis = loadRedisConstructor();
    const client = new Redis(config.url, {
        lazyConnect: true,
        enableOfflineQueue: false,
        maxRetriesPerRequest: 1,
        connectionName: 'fuxa-runtime-bridge',
        retryStrategy: function (times) {
            return Math.min(times * 1000, 30000);
        }
    });
    return client;
}

/**
 * Open a temporary connection and PING the server. Never throws and never
 * returns connection secrets.
 *
 * @param {object} config resolved configuration ({ url })
 * @param {object} [options] { timeoutMs, createClient(url, options) }
 * @returns {Promise<{ok: boolean, latencyMs: number, error?: string}>}
 */
async function testConnection(config, options) {
    options = options || {};
    const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : DEFAULT_TEST_TIMEOUT_MS;
    const createClient = options.createClient || function (url, clientOptions) {
        const Redis = loadRedisConstructor();
        return new Redis(url, clientOptions);
    };
    const started = Date.now();
    let client = null;
    try {
        client = createClient((config && config.url) || DEFAULT_URL, {
            lazyConnect: true,
            enableOfflineQueue: false,
            maxRetriesPerRequest: 1,
            connectTimeout: timeoutMs,
            retryStrategy: function () {
                return null;
            }
        });
        if (client && typeof client.on === 'function') {
            // ioredis emits 'error': an unhandled listener would crash the process
            client.on('error', function () {});
        }
        await withTimeout(Promise.resolve(client.connect()), timeoutMs);
        const pingStart = Date.now();
        await withTimeout(Promise.resolve(client.ping()), timeoutMs);
        return { ok: true, latencyMs: Date.now() - pingStart };
    } catch (err) {
        return {
            ok: false,
            latencyMs: Date.now() - started,
            error: sanitizeError(err, config && config.password)
        };
    } finally {
        try {
            if (client && typeof client.disconnect === 'function') {
                client.disconnect();
            }
        } catch (err) {
            // ignore
        }
    }
}

/**
 * Create a bridge instance.
 *
 * @param {object} options
 * @param {EventEmitter} options.events runtime events emitter
 * @param {object} [options.logger] runtime logger (info/error)
 * @param {object|function} [options.settings] persisted settings (or provider)
 * @param {object} [options.env] environment source (defaults to process.env)
 * @param {function} [options.createClient] client factory, for tests
 * @returns bridge with start(), stop(), publish() and helpers
 */
function create(options) {
    options = options || {};

    const events = options.events;
    const logger = options.logger || {};
    const env = options.env || process.env;
    const settingsSource = options.settings;
    const settingsProvider = typeof settingsSource === 'function'
        ? settingsSource
        : function () { return settingsSource; };
    const createClient = options.createClient || createRedisClient;

    let client = null;
    let started = false;
    let activeConfig = null;
    let lastErrorAt = 0;
    let readyHandler = null;
    const boundListeners = [];

    // tag.changed coalescing (opt-in)
    let pendingBatch = new Map();
    let batchTimer = null;
    // latest value per tag + the subset still to be written to the values key
    let latestValues = new Map();
    let dirtyValues = new Map();
    let valuesTimer = null;

    function currentConfig() {
        const config = resolveConfig(settingsProvider(), env);
        if (options.url) {
            config.url = options.url;
        }
        if (options.stream) {
            config.stream = options.stream;
        }
        if (options.maxlen !== undefined) {
            config.maxlen = parseMaxlen(options.maxlen);
        }
        if (options.enabled !== undefined) {
            config.enabled = parseBoolean(options.enabled);
        }
        // explicit options win over env (used by tests and embedders) and stay
        // inside the documented ranges, so a bad option cannot disable batching
        // silently or flood Redis with sub-100ms windows.
        if (options.batchEnabled !== undefined) {
            config.batchEnabled = parseBoolean(options.batchEnabled);
        }
        if (options.batchWindowMs !== undefined) {
            config.batchWindowMs = clampInteger(options.batchWindowMs,
                MIN_BATCH_WINDOW_MS, MAX_BATCH_WINDOW_MS, config.batchWindowMs);
        }
        if (options.batchMax !== undefined) {
            config.batchMax = clampInteger(options.batchMax, 1, MAX_BATCH_MAX, config.batchMax);
        }
        if (options.valuesEnabled !== undefined) {
            config.valuesEnabled = parseBoolean(options.valuesEnabled);
        }
        if (options.valuesKey) {
            config.valuesKey = String(options.valuesKey).trim();
        }
        if (options.valuesKeyType) {
            config.valuesKeyType = parseValuesKeyType(options.valuesKeyType);
        }
        if (options.valuesFlushMs !== undefined) {
            config.valuesFlushMs = clampInteger(options.valuesFlushMs,
                MIN_VALUES_FLUSH_MS, MAX_VALUES_FLUSH_MS, config.valuesFlushMs);
        }
        if (options.valuesMaxTags !== undefined) {
            config.valuesMaxTags = clampInteger(options.valuesMaxTags, 1, 1000000, config.valuesMaxTags);
        }
        return config;
    }

    function logInfo(message) {
        if (logger && typeof logger.info === 'function') {
            logger.info('redisBridge: ' + message);
        }
    }

    function logError(message, err) {
        if (!logger || typeof logger.error !== 'function') {
            return;
        }
        const now = Date.now();
        if (now - lastErrorAt < ERROR_LOG_THROTTLE_MS) {
            return;
        }
        lastErrorAt = now;
        let detail = '';
        if (err) {
            detail = ': ' + (err.stack || err.message || String(err));
        }
        logger.error('redisBridge: ' + message + detail);
    }

    function notifyReady() {
        if (typeof readyHandler !== 'function') {
            return;
        }
        try {
            readyHandler();
        } catch (err) {
            logError('ready handler failed', err);
        }
    }

    /**
     * Append one event to the stream. Fire and forget: returns false when the
     * event could not even be handed to Redis.
     */
    function publish(type, payload) {
        if (!client || !activeConfig) {
            return false;
        }
        // With enableOfflineQueue:false, XADD while connecting/reconnecting throws.
        // Fire-and-forget bridge: drop now; the ready handler republishes snapshots.
        if (client.status && client.status !== 'ready') {
            return false;
        }
        try {
            const event = buildEvent(type, payload, activeConfig.source);
            const message = JSON.stringify(event);
            let result;
            if (activeConfig.maxlen) {
                result = client.xadd(activeConfig.stream, 'MAXLEN', '~', activeConfig.maxlen,
                    '*', 'event', message, 'type', type);
            } else {
                result = client.xadd(activeConfig.stream, '*', 'event', message, 'type', type);
            }
            if (result && typeof result.catch === 'function') {
                result.catch(function (err) {
                    logError('failed to publish ' + type, err);
                });
            }
            return true;
        } catch (err) {
            logError('failed to publish ' + type, err);
            return false;
        }
    }

    function isClientReady() {
        return !!(client && activeConfig && (!client.status || client.status === 'ready'));
    }

    function clearBatchTimer() {
        if (batchTimer) {
            clearTimeout(batchTimer);
            batchTimer = null;
        }
    }

    /**
     * Remember the latest value of one tag and mark it for the values key.
     * Bounded: when the map is full the oldest tag is dropped instead of growing
     * without limit on a project with many transient/unknown tag ids.
     */
    function rememberValue(tagId, change) {
        if (!activeConfig || !activeConfig.valuesEnabled) {
            return;
        }
        const record = {
            value: change.value,
            timestamp: change.timestamp,
            quality: change.quality,
            deviceId: change.deviceId
        };
        latestValues.set(tagId, record);
        dirtyValues.set(tagId, record);
        const maxTags = activeConfig.valuesMaxTags;
        while (latestValues.size > maxTags) {
            const oldest = latestValues.keys().next();
            if (oldest.done) {
                break;
            }
            latestValues.delete(oldest.value);
            dirtyValues.delete(oldest.value);
        }
    }

    /**
     * Queue one change for the next batch (last write wins per device+tag).
     */
    function enqueueChange(change) {
        pendingBatch.set(String(change.deviceId) + '\u0000' + String(change.tagId), change);
        if (pendingBatch.size >= activeConfig.batchMax) {
            // never let a burst grow past one batch event; flush early instead.
            flushBatch();
            return;
        }
        if (batchTimer) {
            return;
        }
        batchTimer = setTimeout(function () {
            batchTimer = null;
            flushBatch();
        }, activeConfig.batchWindowMs);
        if (batchTimer && typeof batchTimer.unref === 'function') {
            // never keep the process alive just to publish a batch
            batchTimer.unref();
        }
    }

    /**
     * Publish the queued changes as `tag.changed.batch` events (max
     * FUXA_REDIS_BATCH_MAX changes each) and clear the queue.
     *
     * @returns {boolean} true when at least one event was handed to Redis
     */
    function flushBatch() {
        clearBatchTimer();
        if (pendingBatch.size === 0) {
            return false;
        }
        const changes = Array.from(pendingBatch.values());
        pendingBatch.clear();
        const batchMax = activeConfig && activeConfig.batchMax ? activeConfig.batchMax : DEFAULT_BATCH_MAX;
        let published = false;
        for (let offset = 0; offset < changes.length; offset += batchMax) {
            const slice = changes.slice(offset, offset + batchMax);
            if (publish('tag.changed.batch', { count: slice.length, changes: slice })) {
                published = true;
            }
        }
        return published;
    }

    /**
     * Re-queue entries whose write failed, but only when no newer value for the
     * same tag arrived in the meantime (that newer value is already queued).
     */
    function requeueValues(entries) {
        for (const entry of entries) {
            if (latestValues.get(entry[0]) === entry[1]) {
                dirtyValues.set(entry[0], entry[1]);
            }
        }
    }

    function writeValuesHash(entries) {
        if (typeof client.hset !== 'function') {
            return false;
        }
        // capture the key: a late rejection must not read a cleared activeConfig
        const key = activeConfig.valuesKey;
        let written = false;
        for (let offset = 0; offset < entries.length; offset += VALUES_CHUNK_FIELDS) {
            const slice = entries.slice(offset, offset + VALUES_CHUNK_FIELDS);
            const args = [key];
            for (const entry of slice) {
                args.push(String(entry[0]), JSON.stringify(entry[1]));
            }
            try {
                const result = client.hset.apply(client, args);
                written = true;
                if (result && typeof result.catch === 'function') {
                    result.catch((function (failed) {
                        return function (err) {
                            requeueValues(failed);
                            logError('failed to update ' + key, err);
                        };
                    })(slice));
                }
            } catch (err) {
                requeueValues(slice);
                logError('failed to update ' + key, err);
            }
        }
        return written;
    }

    function writeValuesString(entries) {
        if (typeof client.set !== 'function') {
            return false;
        }
        const key = activeConfig.valuesKey;
        const values = {};
        latestValues.forEach(function (record, tagId) {
            values[tagId] = record;
        });
        const body = JSON.stringify({
            version: 1,
            updatedAt: new Date().toISOString(),
            count: Object.keys(values).length,
            values: values
        });
        try {
            const result = client.set(key, body);
            if (result && typeof result.catch === 'function') {
                result.catch(function (err) {
                    requeueValues(entries);
                    logError('failed to update ' + key, err);
                });
            }
            return true;
        } catch (err) {
            logError('failed to update ' + key, err);
            return false;
        }
    }

    /**
     * Write the latest values snapshot. Only dirty tags are written, in chunks,
     * so 10k tags cost a handful of commands per interval instead of 10k HSETs.
     *
     * @param {object} [flushOptions] { force: rewrite every known value }
     * @returns {boolean} true when the write was handed to Redis
     */
    function flushValues(flushOptions) {
        const force = !!(flushOptions && flushOptions.force);
        if (!activeConfig || !activeConfig.valuesEnabled) {
            return false;
        }
        if (!isClientReady()) {
            return false;
        }
        if (!force && dirtyValues.size === 0) {
            return true;
        }
        const entries = force
            ? Array.from(latestValues.entries())
            : Array.from(dirtyValues.entries());
        if (!force) {
            dirtyValues.clear();
        }
        if (entries.length === 0) {
            return true;
        }
        try {
            const written = activeConfig.valuesKeyType === 'string'
                ? writeValuesString(entries)
                : writeValuesHash(entries);
            if (!written) {
                requeueValues(entries);
            }
            return written;
        } catch (err) {
            requeueValues(entries);
            logError('failed to flush values', err);
            return false;
        }
    }

    function clearValues() {
        latestValues = new Map();
        dirtyValues = new Map();
    }

    function startValuesTimer() {
        stopValuesTimer();
        if (!activeConfig || !activeConfig.valuesEnabled) {
            return;
        }
        valuesTimer = setInterval(function () {
            if (started) {
                flushValues();
            }
        }, activeConfig.valuesFlushMs);
        if (valuesTimer && typeof valuesTimer.unref === 'function') {
            valuesTimer.unref();
        }
    }

    function stopValuesTimer() {
        if (valuesTimer) {
            clearInterval(valuesTimer);
            valuesTimer = null;
        }
    }

    const handlers = {
        'device-value:changed': function (event) {
            if (!event || event.id === undefined || event.id === null) {
                return;
            }
            if (!event.values || typeof event.values !== 'object') {
                return;
            }
            const batching = !!(activeConfig && activeConfig.batchEnabled);
            for (const tagId of Object.keys(event.values)) {
                const tag = event.values[tagId];
                if (!tag || typeof tag !== 'object') {
                    continue;
                }
                const change = {
                    deviceId: event.id,
                    tagId: tagId,
                    value: tag.value === undefined ? null : tag.value,
                    timestamp: tag.timestamp === undefined ? null : tag.timestamp,
                    quality: tag.quality === undefined ? null : tag.quality
                };
                // The values key always keeps the latest value, batched or not.
                rememberValue(tagId, change);
                if (batching) {
                    enqueueChange(change);
                } else {
                    publish('tag.changed', change);
                }
            }
        },
        'device-status:changed': function (event) {
            if (!event || event.id === undefined || event.id === null) {
                return;
            }
            publish('device.updated', {
                deviceId: event.id,
                status: event.status === undefined ? null : event.status
            });
        },
        'alarms-status:changed': function (event) {
            // aggregate payload of unknown shape: publish as-is
            publish('alarm.status', event === undefined ? null : event);
        },
        'project-device:change': function (event) {
            if (!event || typeof event !== 'object') {
                return;
            }
            const device = event.device && typeof event.device === 'object' ? event.device : null;
            const deviceId = event.id !== undefined ? event.id
                : (event.deviceId !== undefined ? event.deviceId : (device ? device.id : undefined));
            if (deviceId === undefined || deviceId === null || deviceId === '') {
                return;
            }
            const status = event.status !== undefined ? event.status : (device ? device.status : undefined);
            publish('device.updated', {
                deviceId: deviceId,
                status: status === undefined ? null : status
            });
        }
    };

    function bindEvents() {
        if (!events || typeof events.on !== 'function') {
            return;
        }
        for (const eventName of Object.keys(handlers)) {
            const listener = function () {
                try {
                    handlers[eventName].apply(null, arguments);
                } catch (err) {
                    // never let a listener break the runtime event pipeline
                    logError('handler for ' + eventName + ' failed', err);
                }
            };
            events.on(eventName, listener);
            boundListeners.push([eventName, listener]);
        }
    }

    function unbindEvents() {
        if (!events || typeof events.removeListener !== 'function') {
            return;
        }
        while (boundListeners.length) {
            const entry = boundListeners.pop();
            try {
                events.removeListener(entry[0], entry[1]);
            } catch (err) {
                // ignore
            }
        }
    }

    function cleanupClient() {
        const current = client;
        client = null;
        activeConfig = null;
        if (!current) {
            return Promise.resolve();
        }
        try {
            if (typeof current.quit === 'function') {
                return Promise.resolve(current.quit()).catch(function () {
                    try {
                        if (typeof current.disconnect === 'function') {
                            current.disconnect();
                        }
                    } catch (err) {
                        // ignore
                    }
                });
            }
            if (typeof current.disconnect === 'function') {
                current.disconnect();
            }
        } catch (err) {
            // ignore: shutdown must be best effort
        }
        return Promise.resolve();
    }

    function start() {
        let config;
        try {
            config = currentConfig();
        } catch (err) {
            logError('failed to resolve settings', err);
            return false;
        }
        if (!config.enabled) {
            return false;
        }
        if (started) {
            return true;
        }
        try {
            client = createClient({
                url: config.url,
                stream: config.stream,
                maxlen: config.maxlen,
                source: config.source
            }, logger) || null;
            if (!client) {
                return false;
            }
            activeConfig = config;
            bindEvents();
            if (typeof client.on === 'function') {
                client.on('error', function (err) {
                    logError('redis client error', err);
                });
                client.on('ready', function () {
                    logInfo('connected to redis');
                    // Rewrite the values map after a (re)connect: a Redis restart
                    // would otherwise lose the snapshot until every tag changes
                    // again. Nothing is written when no tag was seen yet.
                    flushValues({ force: true });
                    notifyReady();
                });
            }
            if (typeof client.connect === 'function' && (!client.status || client.status === 'wait')) {
                Promise.resolve(client.connect()).catch(function (err) {
                    logError('failed to connect', err);
                });
            }
            startValuesTimer();
            started = true;
            logInfo('publishing runtime events to stream ' + config.stream +
                (config.maxlen ? ' (maxlen ~ ' + config.maxlen + ')' : '') +
                (config.batchEnabled
                    ? ' (batching tag.changed every ' + config.batchWindowMs + 'ms, max ' + config.batchMax + ')'
                    : '') +
                (config.valuesEnabled
                    ? ' (values key ' + config.valuesKey + ' as ' + config.valuesKeyType +
                        ' every ' + config.valuesFlushMs + 'ms)'
                    : ''));
            return true;
        } catch (err) {
            logError('failed to start', err);
            unbindEvents();
            cleanupClient();
            return false;
        }
    }

    function stop() {
        unbindEvents();
        started = false;
        // best effort: hand over whatever is queued before the client closes
        try {
            flushBatch();
        } catch (err) {
            logError('failed to flush batch on stop', err);
        }
        try {
            flushValues();
        } catch (err) {
            logError('failed to flush values on stop', err);
        }
        clearBatchTimer();
        stopValuesTimer();
        clearValues();
        return cleanupClient();
    }

    function isEnabled() {
        try {
            return currentConfig().enabled;
        } catch (err) {
            return false;
        }
    }

    return {
        start: start,
        stop: stop,
        publish: publish,
        /** Publish queued tag changes now (no-op when batching is disabled). */
        flushBatch: flushBatch,
        /**
         * Write the live values key now. `force` rewrites every known value
         * instead of only the changed ones.
         */
        flushValues: flushValues,
        /** Snapshot of the in-memory latest values (tagId -> value record). */
        getLatestValues: function () {
            const out = {};
            latestValues.forEach(function (record, tagId) {
                out[tagId] = record;
            });
            return out;
        },
        /**
         * Register a callback fired every time the Redis connection is (re)ready.
         * Used by the tag catalog to republish its snapshot after a reconnect.
         */
        setReadyHandler: function (handler) {
            readyHandler = typeof handler === 'function' ? handler : null;
            // If the client is already connected when the handler is registered,
            // invoke it once so startup snapshots are not lost.
            if (readyHandler && client && client.status === 'ready') {
                try {
                    readyHandler();
                } catch (err) {
                    logError('ready handler failed', err);
                }
            }
        },
        isEnabled: isEnabled,
        isStarted: function () { return started; },
        get client() { return client; },
        get stream() { return activeConfig ? activeConfig.stream : undefined; },
        get maxlen() { return activeConfig ? activeConfig.maxlen : undefined; },
        get batchEnabled() { return !!(activeConfig && activeConfig.batchEnabled); },
        get batchWindowMs() { return activeConfig ? activeConfig.batchWindowMs : undefined; },
        get batchMax() { return activeConfig ? activeConfig.batchMax : undefined; },
        get valuesKey() { return activeConfig ? activeConfig.valuesKey : undefined; },
        get valuesKeyType() { return activeConfig ? activeConfig.valuesKeyType : undefined; }
    };
}

module.exports = {
    create: create,
    parseBoolean: parseBoolean,
    parseMaxlen: parseMaxlen,
    generateEventId: generateEventId,
    normalizeSettings: normalizeSettings,
    validateSettings: validateSettings,
    mergeSettings: mergeSettings,
    toPublicSettings: toPublicSettings,
    redactUrl: redactUrl,
    buildRedisUrl: buildRedisUrl,
    resolveConfig: resolveConfig,
    testConnection: testConnection,
    sanitizeError: sanitizeError,
    DEFAULT_URL: DEFAULT_URL,
    DEFAULT_STREAM: DEFAULT_STREAM,
    DEFAULT_TEST_TIMEOUT_MS: DEFAULT_TEST_TIMEOUT_MS
};
