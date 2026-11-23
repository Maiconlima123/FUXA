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
        source: DEFAULT_SOURCE
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

    const handlers = {
        'device-value:changed': function (event) {
            if (!event || event.id === undefined || event.id === null) {
                return;
            }
            if (!event.values || typeof event.values !== 'object') {
                return;
            }
            for (const tagId of Object.keys(event.values)) {
                const tag = event.values[tagId];
                if (!tag || typeof tag !== 'object') {
                    continue;
                }
                publish('tag.changed', {
                    deviceId: event.id,
                    tagId: tagId,
                    value: tag.value === undefined ? null : tag.value,
                    timestamp: tag.timestamp === undefined ? null : tag.timestamp,
                    quality: tag.quality === undefined ? null : tag.quality
                });
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
                    notifyReady();
                });
            }
            if (typeof client.connect === 'function' && (!client.status || client.status === 'wait')) {
                Promise.resolve(client.connect()).catch(function (err) {
                    logError('failed to connect', err);
                });
            }
            started = true;
            logInfo('publishing runtime events to stream ' + config.stream +
                (config.maxlen ? ' (maxlen ~ ' + config.maxlen + ')' : ''));
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
        /**
         * Register a callback fired every time the Redis connection is (re)ready.
         * Used by the tag catalog to republish its snapshot after a reconnect.
         */
        setReadyHandler: function (handler) {
            readyHandler = typeof handler === 'function' ? handler : null;
        },
        isEnabled: isEnabled,
        isStarted: function () { return started; },
        get client() { return client; },
        get stream() { return activeConfig ? activeConfig.stream : undefined; },
        get maxlen() { return activeConfig ? activeConfig.maxlen : undefined; }
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
