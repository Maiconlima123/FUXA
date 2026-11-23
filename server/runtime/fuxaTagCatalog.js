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
 *   { tagId, deviceId, name, description, type, address, dataType, unit: null, source: 'fuxa' }
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

function isFilled(value) {
    return value !== undefined && value !== null && value !== '';
}

function toText(value, fallback) {
    if (!isFilled(value)) {
        return fallback === undefined ? null : fallback;
    }
    return String(value);
}

/**
 * Normalize one project tag into the public catalog shape.
 * FUXA stores the engineering data type in `type`; `dataType` mirrors it unless
 * the device driver exposes a dedicated attribute.
 */
function normalizeTag(deviceId, tagKey, tag) {
    const source = (tag && typeof tag === 'object') ? tag : {};
    const tagId = isFilled(source.id) ? String(source.id) : String(tagKey);
    return {
        tagId: tagId,
        deviceId: toText(deviceId),
        name: toText(source.name, tagId),
        description: toText(source.description),
        type: toText(source.type),
        address: toText(source.address),
        dataType: isFilled(source.dataType) ? source.dataType : toText(source.type),
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
                tags.push(normalizeTag(deviceId, tagKey, tag));
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
    function publishSnapshot() {
        if (!started) {
            return false;
        }
        try {
            const tags = collectTags();
            knownTags = indexTags(tags);
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

    function start() {
        bind();
        started = true;
        bindBridge();
        return publishSnapshot();
    }

    function stop() {
        started = false;
        if (timer) {
            clearTimeout(timer);
            timer = null;
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
