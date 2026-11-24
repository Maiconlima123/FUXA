/**
 * 'runtime.fuxaTagCatalog': publish the project tag catalog to the runtime
 * event pipeline so external consumers (dashboards, maintenance systems, log
 * pipelines, ...) can resolve tag id/name/description/type/address/device
 * without scraping the project REST API.
 *
 * Events are handed to the existing redis bridge (`runtime.redisBridge`), which
 * owns the transport, configuration and event envelope:
 *
 *   { version, eventId, type, ts, source: 'fuxa', payload }
 *
 * Published types:
 *   tag.snapshot  payload = { count, tags: [tag, ...] }   (one tag per catalog entry)
 *   tag.created   payload = tag
 *   tag.updated   payload = tag
 *   tag.deleted   payload = tag (last known metadata)
 *
 * Tag payload:
 *   { tagId, deviceId, name, description, protocol, deviceType, type, tagType,
 *     address, dataType, registerType, unit: null, source: 'fuxa' }
 *
 * Protocol metadata is derived from the device (never from the tag name/address):
 *   - protocol      normalized connection protocol, e.g. s7, modbus-tcp,
 *                   modbus-rtu, mqtt, opcua, iothttp, httprequest, ethernetip;
 *   - deviceType    raw FUXA device type (SiemensS7, ModbusTCP, MQTTclient, ...);
 *   - registerType  canonical Modbus table (coil, discrete-input, input-register,
 *                   holding-register) or any driver-provided register type;
 *   - tagType       raw FUXA tag type as stored in the project.
 * Only tag/device *type* metadata is exposed, never device connection settings,
 * credentials or driver options.
 *
 * Behaviour:
 *  - the full snapshot is published when the catalog starts (project/devices
 *    loaded) and on every runtime restart;
 *  - project changes are debounced and diffed against the last known catalog,
 *    so only real creates/updates/deletes are published (no flooding, no
 *    duplicates);
 *  - everything is best effort: a disabled or unreachable bridge never throws
 *    into the FUXA runtime.
 *
 * This module is deliberately generic: it knows nothing about any consumer.
 */

'use strict';

const DEFAULT_DEBOUNCE_MS = 250;
const ERROR_LOG_THROTTLE_MS = 30000;

const CATALOG_EVENTS = ['project-tags:changed', 'project-device:change'];

/**
 * Normalized protocol per FUXA device type. Keep this list additive: unknown
 * device types fall back to a normalized slug of their raw name, so a new
 * driver never breaks the catalog.
 */
const PROTOCOL_BY_DEVICE_TYPE = Object.freeze({
    SiemensS7: 's7',
    OPCUA: 'opcua',
    ModbusRTU: 'modbus-rtu',
    ModbusTCP: 'modbus-tcp',
    IoTHTTP: 'iothttp',
    BACnet: 'bacnet',
    WebAPI: 'httprequest',
    MQTTclient: 'mqtt',
    EthernetIP: 'ethernetip',
    GenericEthernetIP: 'ethernetip',
    OmronEthernetIP: 'omron-ethernetip',
    FuxaServer: 'fuxa-server',
    ODBC: 'odbc',
    ADSclient: 'ads',
    GPIO: 'gpio',
    internal: 'internal',
    WebCam: 'webcam',
    MELSEC: 'melsec',
    REDIS: 'redis',
    SNMP: 'snmp'
});

/**
 * Modbus register tables. FUXA stores the table selector in `tag.memaddress`
 * (the low address remains in `tag.address`), so this is explicit driver
 * metadata and not an inference from the numeric address.
 */
const MODBUS_REGISTER_TYPES = Object.freeze({
    '000000': 'coil',
    '100000': 'discrete-input',
    '300000': 'input-register',
    '400000': 'holding-register'
});

const REGISTER_TYPE_ALIASES = Object.freeze({
    coil: 'coil',
    coils: 'coil',
    'coil-status': 'coil',
    'discrete-input': 'discrete-input',
    'discrete-inputs': 'discrete-input',
    'digital-input': 'discrete-input',
    'digital-inputs': 'discrete-input',
    'input-register': 'input-register',
    'input-registers': 'input-register',
    'holding-register': 'holding-register',
    'holding-registers': 'holding-register'
});

function isFilled(value) {
    return value !== undefined && value !== null && value !== '';
}

function toText(value, fallback) {
    if (!isFilled(value)) {
        return fallback === undefined ? null : fallback;
    }
    return String(value);
}

function normalizeSlug(value) {
    const text = toText(value);
    if (!text) {
        return null;
    }
    const slug = text
        .trim()
        .replace(/([a-z0-9])([A-Z])/g, '$1-$2')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
    return slug || null;
}

/**
 * Resolve the normalized protocol from the project device type.
 */
function resolveProtocol(deviceType) {
    const raw = toText(deviceType);
    if (!raw) {
        return null;
    }
    if (Object.prototype.hasOwnProperty.call(PROTOCOL_BY_DEVICE_TYPE, raw)) {
        return PROTOCOL_BY_DEVICE_TYPE[raw];
    }
    // tolerate legacy/case variations of the stored device type
    const known = Object.keys(PROTOCOL_BY_DEVICE_TYPE).find(function (key) {
        return key.toLowerCase() === raw.toLowerCase();
    });
    return known ? PROTOCOL_BY_DEVICE_TYPE[known] : normalizeSlug(raw);
}

function normalizeRegisterType(value) {
    const slug = normalizeSlug(value);
    if (!slug) {
        return null;
    }
    return Object.prototype.hasOwnProperty.call(REGISTER_TYPE_ALIASES, slug)
        ? REGISTER_TYPE_ALIASES[slug]
        : slug;
}

/**
 * Register type comes from an explicit driver/tag option when present;
 * otherwise the Modbus table selector (`memaddress`) is used. The numeric
 * address is never interpreted to guess a table or a data type.
 */
function resolveRegisterType(protocol, source) {
    const options = (source.options && typeof source.options === 'object') ? source.options : {};
    if (isFilled(options.registerType)) {
        return normalizeRegisterType(options.registerType);
    }
    if (isFilled(source.registerType)) {
        return normalizeRegisterType(source.registerType);
    }
    if (protocol === 'modbus-tcp' || protocol === 'modbus-rtu') {
        const memaddress = toText(source.memaddress);
        if (memaddress) {
            const table = Object.keys(MODBUS_REGISTER_TYPES).find(function (prefix) {
                return memaddress.trim().startsWith(prefix);
            });
            if (table) {
                return MODBUS_REGISTER_TYPES[table];
            }
        }
    }
    return null;
}

function deviceTypeOf(device) {
    if (!isFilled(device)) {
        return null;
    }
    if (typeof device === 'string') {
        return device;
    }
    return toText(device.type);
}

/**
 * Normalize one project tag into the public catalog shape.
 * FUXA stores the engineering data type in `type`; `dataType` mirrors it unless
 * the device driver exposes a dedicated attribute. Protocol metadata is read
 * from the owning device, never guessed from the tag name or address.
 */
function normalizeTag(deviceId, tagKey, tag, device) {
    const source = (tag && typeof tag === 'object') ? tag : {};
    const tagId = isFilled(source.id) ? String(source.id) : String(tagKey);
    const deviceType = deviceTypeOf(device);
    const protocol = resolveProtocol(deviceType);
    return {
        tagId: tagId,
        deviceId: toText(deviceId),
        name: toText(source.name, tagId),
        description: toText(source.description),
        protocol: protocol,
        deviceType: deviceType,
        type: toText(source.type),
        tagType: toText(source.type),
        address: toText(source.address),
        dataType: isFilled(source.dataType) ? source.dataType : toText(source.type),
        registerType: resolveRegisterType(protocol, source),
        unit: null,
        source: 'fuxa'
    };
}

function tagKeyOf(deviceId, tagId) {
    return String(deviceId) + '\u0000' + String(tagId);
}

function signatureOf(tag) {
    return JSON.stringify(tag);
}

/**
 * Create a catalog publisher.
 *
 * @param {object} options
 * @param {EventEmitter} options.events runtime events emitter
 * @param {object} [options.logger] runtime logger (info/warn/error)
 * @param {object|function} [options.bridge] redis bridge (or provider) exposing publish(type, payload)
 * @param {function} [options.getDevices] provider of the project devices map
 * @param {number} [options.debounceMs] project change debounce (0 = synchronous, for tests)
 * @returns catalog with start(), stop(), publishSnapshot(), flush() and handleProjectChange()
 */
function create(options) {
    options = options || {};

    const events = options.events;
    const logger = options.logger || {};
    const bridgeSource = options.bridge;
    const getBridge = typeof bridgeSource === 'function'
        ? bridgeSource
        : function () { return bridgeSource; };
    const getDevices = typeof options.getDevices === 'function'
        ? options.getDevices
        : function () { return {}; };
    const debounceMs = Number.isFinite(Number(options.debounceMs)) && Number(options.debounceMs) >= 0
        ? Number(options.debounceMs)
        : DEFAULT_DEBOUNCE_MS;

    let started = false;
    let knownTags = new Map();
    let timer = null;
    let lastErrorAt = 0;
    const boundListeners = [];

    function logError(message, err) {
        if (!logger || typeof logger.error !== 'function') {
            return;
        }
        const now = Date.now();
        if (now - lastErrorAt < ERROR_LOG_THROTTLE_MS) {
            return;
        }
        lastErrorAt = now;
        const detail = err ? ': ' + (err.stack || err.message || String(err)) : '';
        logger.error('fuxaTagCatalog: ' + message + detail);
    }

    function collectTags() {
        const devices = getDevices() || {};
        const tags = [];
        for (const deviceKey of Object.keys(devices)) {
            const device = devices[deviceKey];
            if (!device || typeof device !== 'object' || !device.tags || typeof device.tags !== 'object') {
                continue;
            }
            const deviceId = isFilled(device.id) ? device.id : deviceKey;
            for (const tagKey of Object.keys(device.tags)) {
                const tag = device.tags[tagKey];
                if (!tag || typeof tag !== 'object') {
                    continue;
                }
                tags.push(normalizeTag(deviceId, tagKey, tag, device));
            }
        }
        return tags;
    }

    function indexTags(tags) {
        const index = new Map();
        for (const tag of tags) {
            index.set(tagKeyOf(tag.deviceId, tag.tagId), {
                tag: tag,
                signature: signatureOf(tag)
            });
        }
        return index;
    }

    /**
     * Hand one event to the bridge. Never throws: a disabled bridge simply
     * returns false, and any bridge failure stays inside this module.
     */
    function publishEvent(type, payload) {
        try {
            const bridge = getBridge();
            if (!bridge || typeof bridge.publish !== 'function') {
                return false;
            }
            const result = bridge.publish(type, payload);
            // a disabled/skipped bridge reports false; anything else is accepted
            return result === false ? false : true;
        } catch (err) {
            logError('failed to publish ' + type, err);
            return false;
        }
    }

    /**
     * Publish every known tag as one event and rebase the diff baseline.
     */
    function writeCatalogKey(tags) {
        try {
            const bridge = getBridge();
            const client = bridge && bridge.client;
            if (!client || client.status !== 'ready') {
                return false;
            }
            const body = JSON.stringify({
                version: 1,
                updatedAt: new Date().toISOString(),
                count: tags.length,
                tags: tags
            });
            const result = client.set(CATALOG_KEY, body);
            if (result && typeof result.catch === 'function') {
                result.catch(function (err) {
                    logError('failed to persist catalog key', err);
                });
            }
            return true;
        } catch (err) {
            logError('failed to persist catalog key', err);
            return false;
        }
    }

    function publishSnapshot() {
        if (!started) {
            return false;
        }
        try {
            const tags = collectTags();
            knownTags = indexTags(tags);
            // Durable catalog for consumers that missed the stream snapshot.
            writeCatalogKey(tags);
            return publishEvent('tag.snapshot', {
                count: tags.length,
                tags: tags
            });
        } catch (err) {
            logError('failed to publish snapshot', err);
            return false;
        }
    }

    /**
     * Diff the current project against the last published catalog and emit one
     * event per real change.
     */
    function flush() {
        if (!started) {
            return;
        }
        try {
            const next = indexTags(collectTags());
            const created = [];
            const updated = [];
            const deleted = [];

            next.forEach(function (entry, key) {
                const previous = knownTags.get(key);
                if (!previous) {
                    created.push(entry.tag);
                } else if (previous.signature !== entry.signature) {
                    updated.push(entry.tag);
                }
            });
            knownTags.forEach(function (entry, key) {
                if (!next.has(key)) {
                    deleted.push(entry.tag);
                }
            });

            knownTags = next;
            for (const tag of created) {
                publishEvent('tag.created', tag);
            }
            for (const tag of updated) {
                publishEvent('tag.updated', tag);
            }
            for (const tag of deleted) {
                publishEvent('tag.deleted', tag);
            }
        } catch (err) {
            logError('failed to publish catalog changes', err);
        }
    }

    function scheduleFlush() {
        if (debounceMs <= 0) {
            flush();
            return;
        }
        if (timer) {
            return;
        }
        timer = setTimeout(function () {
            timer = null;
            flush();
        }, debounceMs);
        if (timer && typeof timer.unref === 'function') {
            // never keep the process alive just to publish a catalog diff
            timer.unref();
        }
    }

    /**
     * Runtime project device/tag change: recompute after the debounce window so
     * a burst of editor writes collapses into one diff.
     */
    function handleProjectChange() {
        if (!started) {
            return;
        }
        scheduleFlush();
    }

    function bind() {
        if (!events || typeof events.on !== 'function') {
            return;
        }
        for (const eventName of CATALOG_EVENTS) {
            if (boundListeners.some(function (entry) { return entry[0] === eventName; })) {
                continue;
            }
            const listener = function () {
                try {
                    handleProjectChange.apply(null, arguments);
                } catch (err) {
                    logError('handler for ' + eventName + ' failed', err);
                }
            };
            events.on(eventName, listener);
            boundListeners.push([eventName, listener]);
        }
    }

    function unbind() {
        while (boundListeners.length) {
            const entry = boundListeners.pop();
            try {
                if (events && typeof events.removeListener === 'function') {
                    events.removeListener(entry[0], entry[1]);
                }
            } catch (err) {
                // ignore: shutdown must be best effort
            }
        }
    }

    function currentBridge() {
        try {
            return getBridge();
        } catch (err) {
            return null;
        }
    }

    /**
     * Republish the snapshot every time the Redis connection becomes ready, so
     * a startup snapshot is not lost when Redis is slow and consumers resync
     * after a reconnect.
     */
    function bindBridge() {
        const bridge = currentBridge();
        if (!bridge || typeof bridge.setReadyHandler !== 'function') {
            return;
        }
        try {
            bridge.setReadyHandler(publishSnapshot);
        } catch (err) {
            logError('failed to register ready handler', err);
        }
    }

    function unbindBridge() {
        const bridge = currentBridge();
        if (!bridge || typeof bridge.setReadyHandler !== 'function') {
            return;
        }
        try {
            bridge.setReadyHandler(null);
        } catch (err) {
            // ignore: shutdown must be best effort
        }
    }

    const CATALOG_KEY = process.env.FUXA_REDIS_CATALOG_KEY || 'fuxa:catalog:tags';
    const SNAPSHOT_INTERVAL_MS = Math.max(15000, Number(process.env.FUXA_REDIS_SNAPSHOT_INTERVAL_MS) || 60000);
    let snapshotRetry = null;
    let snapshotTimer = null;

    function retrySnapshot(attempt) {
        snapshotRetry = null;
        if (!started) return;
        if (publishSnapshot()) return;
        if (attempt >= 30) return;
        snapshotRetry = setTimeout(function () {
            retrySnapshot(attempt + 1);
        }, 1000);
    }

    function start() {
        bind();
        started = true;
        bindBridge();
        // The Redis bridge connects asynchronously. The first snapshot may be
        // dropped until the client is ready; retry briefly instead of losing it.
        const bridge = getBridge();
        const bridgeEnabled = bridge && (typeof bridge.isEnabled !== 'function' || bridge.isEnabled());
        const published = publishSnapshot();
        if (!published && bridgeEnabled) retrySnapshot(1);
        // Refresh the durable catalog and stream snapshot periodically so a
        // consumer that was offline does not depend on the high-frequency
        // tag.changed stream retaining the startup snapshot.
        if (snapshotTimer) clearInterval(snapshotTimer);
        snapshotTimer = setInterval(function () {
            if (started) publishSnapshot();
        }, SNAPSHOT_INTERVAL_MS);
        if (snapshotTimer.unref) snapshotTimer.unref();
        return published;
    }

    function stop() {
        started = false;
        if (timer) {
            clearTimeout(timer);
            timer = null;
        }
        if (snapshotRetry) {
            clearTimeout(snapshotRetry);
            snapshotRetry = null;
        }
        if (snapshotTimer) {
            clearInterval(snapshotTimer);
            snapshotTimer = null;
        }
        unbindBridge();
        unbind();
        knownTags = new Map();
    }

    function getSnapshot() {
        const tags = [];
        knownTags.forEach(function (entry) {
            tags.push(entry.tag);
        });
        return tags;
    }

    return {
        start: start,
        stop: stop,
        isStarted: function () { return started; },
        publishSnapshot: publishSnapshot,
        flush: flush,
        handleProjectChange: handleProjectChange,
        getSnapshot: getSnapshot,
        normalizeTag: normalizeTag
    };
}

module.exports = {
    create: create,
    normalizeTag: normalizeTag,
    DEFAULT_DEBOUNCE_MS: DEFAULT_DEBOUNCE_MS,
    CATALOG_EVENTS: CATALOG_EVENTS
};
