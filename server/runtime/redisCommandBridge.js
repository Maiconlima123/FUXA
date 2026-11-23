/**
 * 'runtime.redisCommandBridge': opt-in consumer of commands from Redis Streams.
 *
 * This is the reverse direction of `runtime.redisBridge`:
 *
 *   maintenance / integrations  --XADD-->  stream:fuxa:commands
 *   FUXA runtime                --XREADGROUP-->  validates + writes the tag
 *   FUXA runtime                --XADD-->  stream:fuxa:events (command.result)
 *
 * The channel is deliberately generic: it knows nothing about maintenance and
 * only executes a small, fixed set of safe commands. It never runs arbitrary
 * code, never creates tags/devices and never starts or embeds Redis.
 *
 * Command envelope (same style as the event envelope):
 *   {
 *     version: 1,
 *     commandId: '<unique id, required for deduplication>',
 *     type: 'tag.write',
 *     ts: '<ISO-8601>',
 *     source: 'maintenance',
 *     payload: { tagId, value, requestedBy, reason }
 *   }
 *
 * Stream fields: `event` (JSON above) + `type` (same as envelope type).
 *
 * Result event (published to the events stream):
 *   type: 'command.result'
 *   payload: {
 *     commandId, commandType, ok, tagId, requestedValue, value, status,
 *     error, ts, requestedBy
 *   }
 *
 * `error` is `null` on success and `{ code, message }` on failure. `status` is
 * a stable machine readable string (see RESULT_STATUS below).
 *
 * Configuration (environment, all optional):
 *   FUXA_REDIS_COMMAND_ENABLED      'true' to enable (default 'false')
 *   FUXA_REDIS_COMMAND_STREAM       command stream (default 'stream:fuxa:commands')
 *   FUXA_REDIS_COMMAND_GROUP        consumer group (default 'fuxa-command-executor')
 *   FUXA_REDIS_COMMAND_CONSUMER     consumer name (default fuxa-<host>-<pid>)
 *   FUXA_REDIS_COMMAND_START_ID     first id for a new group (default '$')
 *   FUXA_REDIS_COMMAND_TIMEOUT_MS   write+read-back timeout (default 5000)
 *   FUXA_REDIS_COMMAND_BLOCK_MS     XREADGROUP block time (default 5000)
 *   FUXA_REDIS_COMMAND_SETTLE_MS    wait before read-back (default 0)
 *   FUXA_REDIS_COMMAND_RESULT_STREAM result stream (default: events stream)
 *   FUXA_REDIS_COMMAND_TAG_ALLOWLIST comma separated tag ids (optional)
 *   FUXA_REDIS_COMMAND_DEDUPE_SIZE  remembered command ids (default 1000)
 *   FUXA_REDIS_COMMAND_DEDUPE_TTL_MS dedupe retention (default 24h)
 *
 * Redis URL/credentials are the same as the event bridge (persisted editor
 * settings > FUXA_REDIS_URL > redis://127.0.0.1:6379/0). The result stream
 * defaults to the event stream resolved by `runtime.redisBridge`, so a single
 * Redis configuration is enough.
 *
 * Safety:
 *  - disabled by default;
 *  - only `tag.write` is accepted;
 *  - the tag must exist in the FUXA project and be exposed by an active device;
 *  - the value must be a scalar accepted by the tag data type;
 *  - an optional allowlist restricts which tag ids may be written;
 *  - every failure is contained: a missing Redis/PLC never crashes FUXA.
 *
 * Alarms: the runtime only exposes the aggregate `alarms-status:changed` event
 * (no per-alarm raised/cleared event), so this bridge keeps publishing
 * `alarm.status` through `runtime.redisBridge` and does not invent
 * `alarm.raised`/`alarm.cleared`.
 */

'use strict';

const crypto = require('crypto');
const os = require('os');
const redisBridge = require('./redisBridge');

const DEFAULT_COMMAND_STREAM = 'stream:fuxa:commands';
const DEFAULT_GROUP = 'fuxa-command-executor';
const DEFAULT_TIMEOUT_MS = 5000;
const DEFAULT_BLOCK_MS = 5000;
const DEFAULT_DEDUPE_SIZE = 1000;
const DEFAULT_DEDUPE_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_EVENT_BYTES = 64 * 1024;
const ERROR_LOG_THROTTLE_MS = 30000;
const LOG_TEXT_MAX = 200;

const SUPPORTED_COMMANDS = ['tag.write'];

const RESULT_STATUS = {
    OK: 'ok',
    READBACK_MISMATCH: 'readback-mismatch',
    UNCONFIRMED: 'unconfirmed',
    INVALID_ENVELOPE: 'invalid-envelope',
    UNSUPPORTED_VERSION: 'unsupported-version',
    UNSUPPORTED_COMMAND: 'unsupported-command',
    INVALID_VALUE: 'invalid-value',
    TAG_NOT_FOUND: 'tag-not-found',
    TAG_NOT_ALLOWED: 'tag-not-allowed',
    TAG_NOT_ACTIVE: 'tag-not-active',
    WRITE_FAILED: 'write-failed',
    TIMEOUT: 'timeout',
    ERROR: 'error'
};

const BOOL_TAG_TYPES = ['bool', 'boolean', 'bit'];
const STRING_TAG_TYPES = ['string', 'bytestring', 'char', 'wstring', 'wchar'];
const NUMERIC_TAG_TYPES = [
    'byte', 'sbyte', 'int', 'uint', 'word', 'dword', 'dint',
    'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64', 'uint64',
    'sint', 'usint', 'udint', 'lint', 'ulint',
    'real', 'lreal', 'float', 'float32', 'float64', 'double', 'number', 'integer', 'uinteger'
];

function isFilled(value) {
    return value !== undefined && value !== null && value !== '';
}

function parsePositiveInt(value, fallback) {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNonNegativeInt(value, fallback) {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function normalizeTagType(type) {
    return isFilled(type) ? String(type).trim().toLowerCase() : '';
}

function isNumericTagType(type) {
    const normalized = normalizeTagType(type);
    return NUMERIC_TAG_TYPES.indexOf(normalized) !== -1 ||
        /^(u?int|u?dint|l?int|u?lint|float|double|real)\d*(le|mle|be)?$/.test(normalized);
}

function isBoolTagType(type) {
    return BOOL_TAG_TYPES.indexOf(normalizeTagType(type)) !== -1;
}

function isStringTagType(type) {
    return STRING_TAG_TYPES.indexOf(normalizeTagType(type)) !== -1;
}

/**
 * Strip control characters and cap the length of free text that is logged or
 * echoed into the result stream. Nothing here is a secret, but a command is an
 * external input and must not be able to forge multi-line log entries.
 */
function sanitizeText(value, maxLength) {
    if (!isFilled(value)) {
        return null;
    }
    const limit = maxLength || LOG_TEXT_MAX;
    let text = String(value).replace(/[\u0000-\u001f\u007f]+/g, ' ').trim();
    if (text.length > limit) {
        text = text.slice(0, limit);
    }
    return text || null;
}

/**
 * Remove connection credentials from any text that reaches the log. The Redis
 * client can echo the connection URL in transport errors.
 */
function redactSecrets(text) {
    return String(text)
        .replace(/(rediss?:\/\/)[^\s/]*@/gi, '$1***@')
        .replace(/((?:password|passwd|pwd|auth)\s*[=:]\s*)\S+/gi, '$1***');
}

/**
 * Request/result values must stay JSON scalars. Objects, arrays, functions and
 * non finite numbers are rejected before they reach a device driver.
 */
function toSafeScalar(value, maxStringLength) {
    if (value === null || value === undefined) {
        return null;
    }
    const type = typeof value;
    if (type === 'number') {
        return Number.isFinite(value) ? value : null;
    }
    if (type === 'boolean') {
        return value;
    }
    if (type === 'string') {
        const limit = maxStringLength || LOG_TEXT_MAX;
        return value.length > limit ? value.slice(0, limit) : value;
    }
    return null;
}

function generateId() {
    try {
        if (typeof crypto.randomUUID === 'function') {
            return crypto.randomUUID();
        }
    } catch (err) {
        // fall through to the manual id
    }
    return `fuxa-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function buildResultEvent(result) {
    return {
        version: 1,
        eventId: generateId(),
        type: 'command.result',
        ts: new Date().toISOString(),
        source: 'fuxa',
        payload: result
    };
}

function withTimeout(promise, timeoutMs, message) {
    let timer = null;
    const timeout = new Promise(function (resolve, reject) {
        timer = setTimeout(function () {
            const err = new Error(message || 'command timed out');
            err.code = 'COMMAND_TIMEOUT';
            reject(err);
        }, timeoutMs);
        if (timer && typeof timer.unref === 'function') {
            timer.unref();
        }
    });
    return Promise.race([Promise.resolve(promise), timeout]).finally(function () {
        if (timer) {
            clearTimeout(timer);
        }
    });
}

function sleep(ms) {
    return new Promise(function (resolve) {
        const timer = setTimeout(resolve, ms);
        if (timer && typeof timer.unref === 'function') {
            timer.unref();
        }
    });
}

function fieldsToObject(fields) {
    if (Array.isArray(fields)) {
        const out = {};
        for (let i = 0; i + 1 < fields.length; i += 2) {
            out[String(fields[i])] = fields[i + 1];
        }
        return out;
    }
    return (fields && typeof fields === 'object') ? fields : {};
}

function parseAllowlist(value) {
    if (!isFilled(value)) {
        return [];
    }
    return String(value)
        .split(',')
        .map(function (entry) { return entry.trim(); })
        .filter(function (entry) { return entry.length > 0; });
}

function buildConsumerName(env) {
    if (isFilled(env.FUXA_REDIS_COMMAND_CONSUMER)) {
        return sanitizeText(env.FUXA_REDIS_COMMAND_CONSUMER, 120) || 'fuxa';
    }
    let host = 'fuxa';
    try {
        host = os.hostname() || 'fuxa';
    } catch (err) {
        // keep the fallback
    }
    return sanitizeText(`${host}-${process.pid}`, 120) || 'fuxa';
}

/**
 * Resolve the effective configuration. Redis URL/credentials/event stream come
 * from the same sources as `runtime.redisBridge`; command-only options are
 * environment driven because the editor UI intentionally exposes only the
 * event bridge.
 */
function resolveCommandConfig(options, env) {
    options = options || {};
    env = env || process.env;

    const base = redisBridge.resolveConfig(options.settings, env);
    const config = {
        enabled: redisBridge.parseBoolean(env.FUXA_REDIS_COMMAND_ENABLED, false),
        url: base.url,
        commandStream: isFilled(env.FUXA_REDIS_COMMAND_STREAM)
            ? String(env.FUXA_REDIS_COMMAND_STREAM).trim()
            : DEFAULT_COMMAND_STREAM,
        group: isFilled(env.FUXA_REDIS_COMMAND_GROUP)
            ? String(env.FUXA_REDIS_COMMAND_GROUP).trim()
            : DEFAULT_GROUP,
        consumer: buildConsumerName(env),
        startId: isFilled(env.FUXA_REDIS_COMMAND_START_ID)
            ? String(env.FUXA_REDIS_COMMAND_START_ID).trim()
            : '$',
        timeoutMs: parsePositiveInt(env.FUXA_REDIS_COMMAND_TIMEOUT_MS, DEFAULT_TIMEOUT_MS),
        blockMs: parsePositiveInt(env.FUXA_REDIS_COMMAND_BLOCK_MS, DEFAULT_BLOCK_MS),
        settleMs: parseNonNegativeInt(env.FUXA_REDIS_COMMAND_SETTLE_MS, 0),
        resultStream: isFilled(env.FUXA_REDIS_COMMAND_RESULT_STREAM)
            ? String(env.FUXA_REDIS_COMMAND_RESULT_STREAM).trim()
            : base.stream,
        resultMaxlen: base.maxlen,
        allowlist: parseAllowlist(env.FUXA_REDIS_COMMAND_TAG_ALLOWLIST),
        dedupeSize: parsePositiveInt(env.FUXA_REDIS_COMMAND_DEDUPE_SIZE, DEFAULT_DEDUPE_SIZE),
        dedupeTtlMs: parsePositiveInt(env.FUXA_REDIS_COMMAND_DEDUPE_TTL_MS, DEFAULT_DEDUPE_TTL_MS)
    };

    // Explicit options win (used by tests and by the runtime for the stream
    // that the event bridge already resolved).
    if (options.enabled !== undefined) {
        config.enabled = redisBridge.parseBoolean(options.enabled, config.enabled);
    }
    if (isFilled(options.url)) {
        config.url = String(options.url).trim();
    }
    if (isFilled(options.commandStream)) {
        config.commandStream = String(options.commandStream).trim();
    }
    if (isFilled(options.group)) {
        config.group = String(options.group).trim();
    }
    if (isFilled(options.consumer)) {
        config.consumer = String(options.consumer).trim();
    }
    if (isFilled(options.startId)) {
        config.startId = String(options.startId).trim();
    }
    if (options.timeoutMs !== undefined) {
        config.timeoutMs = parsePositiveInt(options.timeoutMs, config.timeoutMs);
    }
    if (options.blockMs !== undefined) {
        config.blockMs = parsePositiveInt(options.blockMs, config.blockMs);
    }
    if (options.settleMs !== undefined) {
        config.settleMs = parseNonNegativeInt(options.settleMs, config.settleMs);
    }
    if (isFilled(options.resultStream)) {
        config.resultStream = String(options.resultStream).trim();
    }
    if (options.resultMaxlen !== undefined) {
        config.resultMaxlen = parsePositiveInt(options.resultMaxlen, config.resultMaxlen);
    }
    if (options.allowlist !== undefined) {
        config.allowlist = Array.isArray(options.allowlist)
            ? options.allowlist.map(function (entry) { return String(entry).trim(); }).filter(Boolean)
            : parseAllowlist(options.allowlist);
    }
    if (options.dedupeSize !== undefined) {
        config.dedupeSize = parsePositiveInt(options.dedupeSize, config.dedupeSize);
    }
    if (options.dedupeTtlMs !== undefined) {
        config.dedupeTtlMs = parsePositiveInt(options.dedupeTtlMs, config.dedupeTtlMs);
    }
    return config;
}

/**
 * Find a tag by id in the project device map. Tags are normally keyed by id,
 * but the lookup also accepts a map keyed differently when `tag.id` matches.
 *
 * @returns {{deviceId: string, tag: object}|null}
 */
function findProjectTag(devicesMap, tagId) {
    if (!devicesMap || typeof devicesMap !== 'object' || !isFilled(tagId)) {
        return null;
    }
    const wanted = String(tagId);
    for (const deviceKey of Object.keys(devicesMap)) {
        const device = devicesMap[deviceKey];
        if (!device || typeof device !== 'object' || !device.tags || typeof device.tags !== 'object') {
            continue;
        }
        const direct = device.tags[wanted] || device.tags[tagId];
        if (direct && typeof direct === 'object') {
            return {
                deviceId: isFilled(device.id) ? String(device.id) : String(deviceKey),
                tag: direct
            };
        }
        for (const tagKey of Object.keys(device.tags)) {
            const tag = device.tags[tagKey];
            if (tag && typeof tag === 'object' && String(tag.id) === wanted) {
                return {
                    deviceId: isFilled(device.id) ? String(device.id) : String(deviceKey),
                    tag: tag
                };
            }
        }
    }
    return null;
}

/**
 * Validate/coerce a requested value for a tag. Only JSON scalars are accepted;
 * booleans and numbers are converted when the tag type is known.
 *
 * @returns {{ok: true, value: *}|{ok: false, message: string}}
 */
function normalizeTagValue(tag, rawValue) {
    if (rawValue === undefined || rawValue === null) {
        return { ok: false, message: 'value is required' };
    }
    const valueType = typeof rawValue;
    if (valueType !== 'string' && valueType !== 'number' && valueType !== 'boolean') {
        return { ok: false, message: 'value must be a string, number or boolean' };
    }

    const tagType = tag && tag.type;
    if (valueType === 'number') {
        if (!Number.isFinite(rawValue)) {
            return { ok: false, message: 'value must be a finite number' };
        }
        if (isBoolTagType(tagType) && rawValue !== 0 && rawValue !== 1) {
            return { ok: false, message: 'a boolean tag accepts only 0 or 1' };
        }
        return { ok: true, value: rawValue };
    }
    if (valueType === 'boolean') {
        return { ok: true, value: rawValue };
    }

    const text = rawValue.trim();
    if (text === '') {
        return { ok: false, message: 'value must not be empty' };
    }
    if (isStringTagType(tagType)) {
        return { ok: true, value: rawValue };
    }
    if (isBoolTagType(tagType)) {
        const lowered = text.toLowerCase();
        if (['1', 'true', 'on', 'yes'].indexOf(lowered) !== -1) {
            return { ok: true, value: true };
        }
        if (['0', 'false', 'off', 'no'].indexOf(lowered) !== -1) {
            return { ok: true, value: false };
        }
        return { ok: false, message: 'value is not a valid boolean' };
    }
    if (isNumericTagType(tagType)) {
        const number = Number(text.replace(',', '.'));
        if (!Number.isFinite(number)) {
            return { ok: false, message: 'value is not a valid number' };
        }
        return { ok: true, value: number };
    }
    // Unknown/driver specific type: keep the scalar as provided.
    return { ok: true, value: rawValue };
}

function canonicalValue(value) {
    if (typeof value === 'boolean') {
        return value ? '1' : '0';
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
        return String(value);
    }
    if (typeof value === 'string') {
        return value.trim();
    }
    return null;
}

function valuesEqual(expected, actual) {
    const canonicalExpected = canonicalValue(expected);
    const canonicalActual = canonicalValue(actual);
    if (canonicalExpected !== null && canonicalActual !== null) {
        return canonicalExpected === canonicalActual;
    }
    return expected === actual;
}

/**
 * Create a command executor bridge.
 *
 * @param {object} options
 * @param {object} [options.logger] runtime logger (info/warn/error)
 * @param {object|function} [options.settings] persisted Redis settings (or provider)
 * @param {object} [options.env] environment source (defaults to process.env)
 * @param {object|function} [options.devices] devices manager (or provider)
 * @param {function} [options.createClient] client factory, for tests
 * @param {object} [options] config overrides: enabled, url, commandStream, group,
 *   consumer, startId, timeoutMs, blockMs, settleMs, resultStream, resultMaxlen,
 *   allowlist, dedupeSize, dedupeTtlMs
 * @returns bridge with start(), stop(), handleStreamEntry() and helpers
 */
function create(options) {
    options = options || {};

    const logger = options.logger || {};
    const env = options.env || process.env;
    const settingsSource = options.settings;
    const settingsProvider = typeof settingsSource === 'function'
        ? settingsSource
        : function () { return settingsSource; };
    const devicesSource = options.devices;
    const getDevicesManager = typeof devicesSource === 'function'
        ? devicesSource
        : function () { return devicesSource; };
    const createClient = options.createClient || function (config, log) {
        const Redis = loadRedisConstructor();
        return new Redis(config.url, {
            lazyConnect: true,
            enableOfflineQueue: false,
            maxRetriesPerRequest: 1,
            connectionName: 'fuxa-command-executor',
            retryStrategy: function (times) {
                return Math.min(times * 1000, 30000);
            }
        });
    };

    let client = null;
    let activeConfig = null;
    let started = false;
    let stopping = false;
    let ready = false;
    let groupReady = false;
    let groupPromise = null;
    let loopRunning = false;
    let lastErrorAt = 0;
    const processed = new Map();

    function currentConfig() {
        return resolveCommandConfig({
            settings: settingsProvider(),
            enabled: options.enabled,
            url: options.url,
            commandStream: options.commandStream,
            group: options.group,
            consumer: options.consumer,
            startId: options.startId,
            timeoutMs: options.timeoutMs,
            blockMs: options.blockMs,
            settleMs: options.settleMs,
            resultStream: options.resultStream,
            resultMaxlen: options.resultMaxlen,
            allowlist: options.allowlist,
            dedupeSize: options.dedupeSize,
            dedupeTtlMs: options.dedupeTtlMs
        }, env);
    }

    function logInfo(message) {
        if (logger && typeof logger.info === 'function') {
            logger.info('redisCommandBridge: ' + message);
        }
    }

    function logWarn(message) {
        if (logger && typeof logger.warn === 'function') {
            logger.warn('redisCommandBridge: ' + message);
        } else {
            logInfo(message);
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
            detail = ': ' + redactSecrets(err.stack || err.message || String(err));
        }
        logger.error('redisCommandBridge: ' + message + detail);
    }

    function logCommand(result, command) {
        const payload = (command && command.payload) || {};
        const requestedBy = sanitizeText(payload.requestedBy, LOG_TEXT_MAX) || '-';
        const reason = sanitizeText(payload.reason, LOG_TEXT_MAX) || '-';
        logInfo(
            (result.commandType || 'unknown') +
            ' tag=' + (result.tagId || '-') +
            ' ok=' + String(result.ok) +
            ' status=' + result.status +
            ' requestedBy=' + requestedBy +
            ' reason=' + reason
        );
    }

    // ---------------------------------------------------------------------
    // Deduplication
    // ---------------------------------------------------------------------

    function evictProcessed(now) {
        const ttl = activeConfig ? activeConfig.dedupeTtlMs : DEFAULT_DEDUPE_TTL_MS;
        const maxSize = activeConfig ? activeConfig.dedupeSize : DEFAULT_DEDUPE_SIZE;
        for (const entry of processed) {
            if (now - entry[1].at > ttl) {
                processed.delete(entry[0]);
            }
        }
        while (processed.size >= maxSize) {
            const oldest = processed.keys().next();
            if (oldest.done) {
                break;
            }
            processed.delete(oldest.value);
        }
    }

    function getProcessed(commandId) {
        if (!commandId) {
            return null;
        }
        evictProcessed(Date.now());
        return processed.get(commandId) || null;
    }

    function rememberResult(commandId, result) {
        if (!commandId) {
            return;
        }
        evictProcessed(Date.now());
        processed.set(commandId, { at: Date.now(), result: result });
    }

    // ---------------------------------------------------------------------
    // Results
    // ---------------------------------------------------------------------

    function publishResult(result) {
        try {
            const current = client;
            if (!current || !activeConfig) {
                return false;
            }
            if (current.status && current.status !== 'ready') {
                return false;
            }
            const message = JSON.stringify(buildResultEvent(result));
            let pending;
            if (activeConfig.resultMaxlen) {
                pending = current.xadd(activeConfig.resultStream, 'MAXLEN', '~', activeConfig.resultMaxlen,
                    '*', 'event', message, 'type', 'command.result');
            } else {
                pending = current.xadd(activeConfig.resultStream, '*', 'event', message, 'type', 'command.result');
            }
            if (pending && typeof pending.catch === 'function') {
                pending.catch(function (err) {
                    logError('failed to publish command result', err);
                });
            }
            return true;
        } catch (err) {
            logError('failed to publish command result', err);
            return false;
        }
    }

    // ---------------------------------------------------------------------
    // Command execution
    // ---------------------------------------------------------------------

    function baseResult(commandId, commandType, payload) {
        return {
            commandId: commandId || null,
            commandType: commandType || null,
            ok: false,
            tagId: null,
            requestedValue: null,
            value: null,
            status: RESULT_STATUS.ERROR,
            error: null,
            ts: new Date().toISOString(),
            requestedBy: null
        };
    }

    function failResult(result, status, code, message) {
        result.ok = false;
        result.status = status;
        result.error = { code: code, message: message };
        return result;
    }

    function findDevicesManager() {
        try {
            return getDevicesManager() || null;
        } catch (err) {
            logError('failed to resolve devices manager', err);
            return null;
        }
    }

    /**
     * Execute a validated `tag.write` command. Never throws: every failure is
     * returned as a structured result.
     */
    async function executeTagWrite(result, payload) {
        const tagId = isFilled(payload.tagId) ? String(payload.tagId) : null;
        result.tagId = sanitizeText(tagId, LOG_TEXT_MAX);
        const requestedValue = toSafeScalar(payload.value, MAX_EVENT_BYTES);
        result.requestedValue = requestedValue;

        if (!tagId) {
            return failResult(result, RESULT_STATUS.INVALID_ENVELOPE, 'missing-tag-id',
                'payload.tagId is required');
        }

        const manager = findDevicesManager();
        if (!manager || typeof manager.getDevices !== 'function') {
            return failResult(result, RESULT_STATUS.ERROR, 'devices-unavailable',
                'device manager is not available');
        }

        let projectDevices;
        try {
            projectDevices = manager.getDevices() || {};
        } catch (err) {
            logError('failed to read project devices', err);
            return failResult(result, RESULT_STATUS.ERROR, 'devices-unavailable',
                'project devices could not be read');
        }

        const found = findProjectTag(projectDevices, tagId);
        if (!found) {
            return failResult(result, RESULT_STATUS.TAG_NOT_FOUND, 'tag-not-found',
                'tag does not exist in the FUXA project');
        }

        if (activeConfig.allowlist.length > 0 && activeConfig.allowlist.indexOf(tagId) === -1) {
            return failResult(result, RESULT_STATUS.TAG_NOT_ALLOWED, 'tag-not-allowed',
                'tag is not present in FUXA_REDIS_COMMAND_TAG_ALLOWLIST');
        }

        const normalized = normalizeTagValue(found.tag, payload.value);
        if (!normalized.ok) {
            return failResult(result, RESULT_STATUS.INVALID_VALUE, 'invalid-value', normalized.message);
        }
        const writeValue = normalized.value;
        result.requestedValue = toSafeScalar(writeValue, MAX_EVENT_BYTES);

        // The tag exists in the project, but the driver only accepts writes for
        // tags it currently exposes (device enabled/connected/loaded).
        if (typeof manager.getDeviceIdFromTag === 'function') {
            let activeDeviceId = null;
            try {
                activeDeviceId = manager.getDeviceIdFromTag(tagId);
            } catch (err) {
                logError('failed to resolve active tag ' + tagId, err);
            }
            if (!activeDeviceId) {
                return failResult(result, RESULT_STATUS.TAG_NOT_ACTIVE, 'tag-not-active',
                    'tag is not exposed by an active device (disabled, offline or unsupported driver)');
            }
        }

        if (typeof manager.setTagValue !== 'function') {
            return failResult(result, RESULT_STATUS.ERROR, 'devices-unavailable',
                'device manager cannot write tags');
        }

        let writeResult;
        try {
            writeResult = await withTimeout(
                Promise.resolve(manager.setTagValue(tagId, writeValue)),
                activeConfig.timeoutMs,
                'tag write timed out'
            );
        } catch (err) {
            if (err && err.code === 'COMMAND_TIMEOUT') {
                return failResult(result, RESULT_STATUS.TIMEOUT, 'timeout',
                    'tag write did not complete within ' + activeConfig.timeoutMs + ' ms');
            }
            logError('tag write failed for ' + tagId, err);
            return failResult(result, RESULT_STATUS.WRITE_FAILED, 'write-failed',
                'device driver rejected the write');
        }

        if (writeResult === false || writeResult === null || writeResult === undefined) {
            return failResult(result, RESULT_STATUS.WRITE_FAILED, 'write-failed',
                'device driver rejected the write');
        }

        if (activeConfig.settleMs > 0) {
            await sleep(activeConfig.settleMs);
        }

        let readBack;
        try {
            if (typeof manager.getTagValue === 'function') {
                readBack = manager.getTagValue(tagId, true);
            }
        } catch (err) {
            logError('failed to read back tag ' + tagId, err);
        }

        let readValue = null;
        if (readBack && typeof readBack === 'object') {
            readValue = readBack.value === undefined ? null : readBack.value;
        } else if (readBack !== undefined) {
            readValue = readBack;
        }
        readValue = toSafeScalar(readValue, 4096);
        result.value = readValue;

        if (readValue === null) {
            // The driver accepted the write but has no cached value yet (common
            // on polling drivers right after a write).
            result.ok = true;
            result.status = RESULT_STATUS.UNCONFIRMED;
            return result;
        }

        if (valuesEqual(writeValue, readValue)) {
            result.ok = true;
            result.status = RESULT_STATUS.OK;
            return result;
        }

        // Write was accepted but the cached value has not converged yet.
        result.ok = true;
        result.status = RESULT_STATUS.READBACK_MISMATCH;
        return result;
    }

    async function executeCommand(command) {
        const commandType = (command && isFilled(command.type)) ? String(command.type) : null;
        const result = baseResult(
            (command && isFilled(command.commandId)) ? String(command.commandId) : null,
            commandType,
            command && command.payload
        );

        if (!command || typeof command !== 'object') {
            return failResult(result, RESULT_STATUS.INVALID_ENVELOPE, 'invalid-envelope',
                'command envelope must be a JSON object');
        }
        if (command.version !== 1) {
            return failResult(result, RESULT_STATUS.UNSUPPORTED_VERSION, 'unsupported-version',
                'only command envelope version 1 is supported');
        }
        if (!isFilled(command.commandId)) {
            return failResult(result, RESULT_STATUS.INVALID_ENVELOPE, 'missing-command-id',
                'commandId is required');
        }

        const payload = (command.payload && typeof command.payload === 'object') ? command.payload : {};
        result.requestedBy = sanitizeText(payload.requestedBy, LOG_TEXT_MAX);
        result.requestedValue = toSafeScalar(payload.value, MAX_EVENT_BYTES);

        if (SUPPORTED_COMMANDS.indexOf(commandType) === -1) {
            return failResult(result, RESULT_STATUS.UNSUPPORTED_COMMAND, 'unsupported-command',
                'only tag.write is supported');
        }

        return executeTagWrite(result, payload);
    }

    // ---------------------------------------------------------------------
    // Redis stream consumption
    // ---------------------------------------------------------------------

    function ensureGroup() {
        if (groupReady) {
            return Promise.resolve();
        }
        if (groupPromise) {
            return groupPromise;
        }
        groupPromise = Promise.resolve()
            .then(function () { return client.xgroup('CREATE', activeConfig.commandStream,
                activeConfig.group, activeConfig.startId, 'MKSTREAM'); })
            .then(function () {
                groupReady = true;
                logInfo('consumer group ' + activeConfig.group + ' ready on ' + activeConfig.commandStream);
            })
            .catch(function (err) {
                groupPromise = null;
                if (err && /BUSYGROUP/i.test(String(err.message || err))) {
                    groupReady = true;
                    return;
                }
                throw err;
            });
        return groupPromise;
    }

    async function ackEntry(entryId) {
        try {
            if (client && typeof client.xack === 'function') {
                await client.xack(activeConfig.commandStream, activeConfig.group, entryId);
            }
        } catch (err) {
            logError('failed to XACK command ' + entryId, err);
        }
    }

    /**
     * Parse, validate and execute one stream entry, then publish the result and
     * XACK it. Exposed for tests and for operators that want to replay entries.
     *
     * @param {string} entryId Redis stream entry id
     * @param {Array|object} fields entry fields (`event`, `type`)
     * @returns {Promise<object|null>} command.result payload (null when stopped)
     */
    async function handleStreamEntry(entryId, fields) {
        if (!started || !activeConfig) {
            return null;
        }
        const raw = fieldsToObject(fields);
        let command = null;
        let result = null;
        let duplicate = false;

        try {
            if (!isFilled(raw.event)) {
                result = baseResult(null, null, null);
                failResult(result, RESULT_STATUS.INVALID_ENVELOPE, 'missing-event-field',
                    'stream entry has no event field');
            } else if (String(raw.event).length > MAX_EVENT_BYTES) {
                result = baseResult(null, null, null);
                failResult(result, RESULT_STATUS.INVALID_ENVELOPE, 'event-too-large',
                    'command envelope is larger than ' + MAX_EVENT_BYTES + ' bytes');
            } else {
                command = JSON.parse(raw.event);
                if (command && typeof command === 'object' && !isFilled(command.type) && isFilled(raw.type)) {
                    command.type = raw.type;
                }
            }
        } catch (err) {
            result = baseResult(null, null, null);
            failResult(result, RESULT_STATUS.INVALID_ENVELOPE, 'invalid-json',
                'event field is not valid JSON');
        }

        if (!result) {
            const commandId = (command && isFilled(command.commandId)) ? String(command.commandId) : null;
            const cached = getProcessed(commandId);
            if (cached) {
                duplicate = true;
                result = cached.result;
            } else {
                try {
                    result = await executeCommand(command);
                } catch (err) {
                    // A bug in a driver must still produce a result and must
                    // never take the runtime down.
                    logError('command execution failed', err);
                    result = baseResult(commandId,
                        (command && isFilled(command.type)) ? String(command.type) : null, null);
                    failResult(result, RESULT_STATUS.ERROR, 'execution-error',
                        'command execution failed unexpectedly');
                }
                rememberResult(commandId, result);
            }
        }

        if (duplicate) {
            logInfo('duplicate command ' + (sanitizeText(result.commandId, LOG_TEXT_MAX) || '-') +
                ' ignored (republishing cached result)');
        } else {
            logCommand(result, command);
        }
        publishResult(result);
        await ackEntry(entryId);
        return result;
    }

    function normalizeXReadReply(reply) {
        const entries = [];
        if (!Array.isArray(reply)) {
            return entries;
        }
        for (const streamReply of reply) {
            if (!Array.isArray(streamReply) || !Array.isArray(streamReply[1])) {
                continue;
            }
            for (const item of streamReply[1]) {
                if (Array.isArray(item) && item.length >= 2) {
                    entries.push([item[0], item[1]]);
                }
            }
        }
        return entries;
    }

    /**
     * Read and process at most one batch (COUNT 1). Exposed for tests.
     */
    async function readOnce() {
        if (!started || !activeConfig || !client || !ready) {
            return 0;
        }
        const reply = await client.xreadgroup('GROUP', activeConfig.group, activeConfig.consumer,
            'COUNT', 1, 'BLOCK', activeConfig.blockMs, 'STREAMS', activeConfig.commandStream, '>');
        const entries = normalizeXReadReply(reply);
        for (const entry of entries) {
            await handleStreamEntry(entry[0], entry[1]);
        }
        return entries.length;
    }

    async function loop() {
        if (loopRunning) {
            return;
        }
        loopRunning = true;
        try {
            while (started && ready && !stopping) {
                try {
                    await readOnce();
                } catch (err) {
                    if (!started || stopping) {
                        break;
                    }
                    logError('failed to read commands', err);
                    // avoid a busy loop on a broken connection; the ready
                    // handler restarts consumption after a reconnect
                    await sleep(Math.min(1000, activeConfig ? activeConfig.blockMs : 1000));
                }
            }
        } finally {
            loopRunning = false;
        }
    }

    function onReady() {
        if (!started || stopping) {
            return;
        }
        ensureGroup().then(function () {
            if (!started || stopping) {
                return;
            }
            Promise.resolve().then(loop).catch(function (err) {
                logError('command loop failed', err);
            });
        }).catch(function (err) {
            logError('failed to create consumer group', err);
        });
    }

    // ---------------------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------------------

    function start() {
        let config;
        try {
            config = currentConfig();
        } catch (err) {
            logError('failed to resolve command settings', err);
            return false;
        }
        if (!config.enabled) {
            return false;
        }
        if (started) {
            return true;
        }
        try {
            client = createClient({ url: config.url, stream: config.commandStream, group: config.group }, logger) || null;
            if (!client) {
                return false;
            }
            activeConfig = config;
            stopping = false;
            ready = false;
            groupReady = false;
            groupPromise = null;
            started = true;

            if (typeof client.on === 'function') {
                client.on('error', function (err) {
                    logError('redis client error', err);
                });
                client.on('ready', function () {
                    ready = true;
                    logInfo('connected to redis, consuming ' + config.commandStream);
                    onReady();
                });
                client.on('close', function () {
                    ready = false;
                });
                client.on('end', function () {
                    ready = false;
                });
            }

            if (typeof client.connect === 'function' && (!client.status || client.status === 'wait')) {
                Promise.resolve(client.connect()).then(function () {
                    if (client && client.status === 'ready') {
                        ready = true;
                        onReady();
                    }
                }).catch(function (err) {
                    logError('failed to connect', err);
                });
            } else if (client.status === 'ready') {
                ready = true;
                Promise.resolve().then(onReady);
            }

            logInfo('consuming commands from ' + config.commandStream + ' (group ' +
                config.group + ', consumer ' + config.consumer + ')');
            return true;
        } catch (err) {
            logError('failed to start', err);
            started = false;
            activeConfig = null;
            try {
                if (client && typeof client.disconnect === 'function') {
                    client.disconnect();
                }
            } catch (disconnectErr) {
                // ignore: startup must be best effort
            }
            client = null;
            return false;
        }
    }

    function stop() {
        stopping = true;
        started = false;
        ready = false;
        groupReady = false;
        groupPromise = null;
        processed.clear();

        const current = client;
        client = null;
        activeConfig = null;
        if (!current) {
            return Promise.resolve();
        }
        try {
            // A blocking XREADGROUP would delay QUIT; disconnect is immediate
            // and the loop exits on its next iteration.
            if (typeof current.disconnect === 'function') {
                current.disconnect();
                return Promise.resolve();
            }
            if (typeof current.quit === 'function') {
                return Promise.resolve(current.quit()).catch(function () {
                    try {
                        if (typeof current.disconnect === 'function') {
                            current.disconnect();
                        }
                    } catch (err) {
                        // ignore: shutdown must be best effort
                    }
                });
            }
        } catch (err) {
            // ignore: shutdown must be best effort
        }
        return Promise.resolve();
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
        handleStreamEntry: handleStreamEntry,
        readOnce: readOnce,
        executeCommand: executeCommand,
        isEnabled: isEnabled,
        isStarted: function () { return started; },
        isGroupReady: function () { return groupReady; },
        get client() { return client; },
        get stream() { return activeConfig ? activeConfig.commandStream : undefined; },
        get group() { return activeConfig ? activeConfig.group : undefined; },
        get consumer() { return activeConfig ? activeConfig.consumer : undefined; }
    };
}

function loadRedisConstructor() {
    const mod = require('ioredis');
    // ioredis v6 exposes both a default and the named Redis export
    return mod.Redis || mod.default || mod;
}

module.exports = {
    create: create,
    resolveCommandConfig: resolveCommandConfig,
    normalizeTagValue: normalizeTagValue,
    findProjectTag: findProjectTag,
    sanitizeText: sanitizeText,
    buildResultEvent: buildResultEvent,
    RESULT_STATUS: RESULT_STATUS,
    SUPPORTED_COMMANDS: SUPPORTED_COMMANDS,
    DEFAULT_COMMAND_STREAM: DEFAULT_COMMAND_STREAM,
    DEFAULT_GROUP: DEFAULT_GROUP,
    DEFAULT_TIMEOUT_MS: DEFAULT_TIMEOUT_MS,
    DEFAULT_BLOCK_MS: DEFAULT_BLOCK_MS,
    DEFAULT_DEDUPE_SIZE: DEFAULT_DEDUPE_SIZE,
    DEFAULT_DEDUPE_TTL_MS: DEFAULT_DEDUPE_TTL_MS,
    MAX_EVENT_BYTES: MAX_EVENT_BYTES
};
