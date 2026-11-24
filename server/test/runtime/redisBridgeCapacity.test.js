'use strict';

const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const redisBridge = require('../../runtime/redisBridge');

function createLogger() {
    const entries = { info: [], warn: [], error: [] };
    return {
        entries,
        info(message) { entries.info.push(message); },
        warn(message) { entries.warn.push(message); },
        error(message) { entries.error.push(message); }
    };
}

function sleep(ms) {
    return new Promise(function (resolve) { setTimeout(resolve, ms); });
}

function createFakeClient(overrides) {
    const client = {
        status: 'wait',
        connectCalls: 0,
        quitCalls: 0,
        xaddCalls: [],
        hsetCalls: [],
        setCalls: [],
        listeners: {},
        connect() { this.connectCalls++; this.status = 'ready'; return Promise.resolve(); },
        xadd() { this.xaddCalls.push(Array.from(arguments)); return Promise.resolve('1-0'); },
        hset() { this.hsetCalls.push(Array.from(arguments)); return Promise.resolve(1); },
        set() { this.setCalls.push(Array.from(arguments)); return Promise.resolve('OK'); },
        on(name, listener) { this.listeners[name] = listener; },
        quit() { this.quitCalls++; return Promise.resolve('OK'); },
        disconnect() {}
    };
    return Object.assign(client, overrides || {});
}

function setup(config) {
    config = config || {};
    const events = new EventEmitter();
    const logger = createLogger();
    const client = config.client || createFakeClient();
    const bridge = redisBridge.create({
        events: events,
        logger: logger,
        env: Object.assign({ FUXA_REDIS_ENABLED: 'true' }, config.env || {}),
        createClient: function () { return client; },
        batchEnabled: config.batchEnabled,
        batchWindowMs: config.batchWindowMs,
        batchMax: config.batchMax,
        valuesEnabled: config.valuesEnabled,
        valuesKeyType: config.valuesKeyType,
        valuesFlushMs: config.valuesFlushMs
    });
    return { events: events, logger: logger, client: client, bridge: bridge };
}

function publishedEvents(client) {
    return client.xaddCalls.map(function (args) {
        const eventIndex = args.indexOf('event');
        assert.notEqual(eventIndex, -1, 'xadd must carry an event field');
        const event = JSON.parse(args[eventIndex + 1]);
        const typeIndex = args.indexOf('type');
        assert.notEqual(typeIndex, -1, 'xadd must carry a type field');
        assert.equal(args[typeIndex + 1], event.type, 'the type field mirrors the envelope');
        return event;
    });
}

/** Flatten the HSET calls recorded by the fake client into tagId -> record. */
function writtenHash(client) {
    const out = {};
    let key = null;
    for (const args of client.hsetCalls) {
        key = args[0];
        for (let index = 1; index < args.length; index += 2) {
            out[args[index]] = JSON.parse(args[index + 1]);
        }
    }
    return { key: key, values: out };
}

function changesOf(events) {
    const changes = [];
    for (const event of events) {
        if (event.type !== 'tag.changed.batch') {
            continue;
        }
        changes.push.apply(changes, event.payload.changes);
    }
    return changes;
}

describe('runtime redis bridge capacity', function () {
    describe('tag.changed batching', function () {
        it('keeps one tag.changed per tag when batching is disabled (default)', function () {
            const ctx = setup();
            assert.equal(ctx.bridge.start(), true);
            assert.equal(ctx.bridge.batchEnabled, false);
            assert.equal(ctx.bridge.batchWindowMs, 200);
            assert.equal(ctx.bridge.batchMax, 1000);

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: {
                    tag1: { value: 1, timestamp: 1000, quality: 'good' },
                    tag2: { value: 2, timestamp: 1001, quality: 'good' }
                }
            });
            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 3, timestamp: 1002, quality: 'good' } }
            });

            const events = publishedEvents(ctx.client);
            assert.deepEqual(events.map(function (entry) { return entry.type; }),
                ['tag.changed', 'tag.changed', 'tag.changed']);
            assert.deepEqual(events[2].payload, {
                deviceId: 'dev1',
                tagId: 'tag1',
                value: 3,
                timestamp: 1002,
                quality: 'good'
            });
        });

        it('coalesces a window into one tag.changed.batch with last value per tag', function () {
            const ctx = setup({ batchEnabled: true, batchWindowMs: 200, batchMax: 1000, valuesEnabled: false });
            ctx.bridge.start();
            assert.equal(ctx.bridge.batchEnabled, true);

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: {
                    tag1: { value: 1, timestamp: 1000, quality: 'good' },
                    tag2: { value: 'a', timestamp: 1001, quality: 'good' }
                }
            });
            // same tag twice inside the window: the last write must win
            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 7, timestamp: 1002, quality: 'bad' } }
            });

            assert.equal(ctx.client.xaddCalls.length, 0, 'nothing is published inside the window');
            assert.equal(ctx.bridge.flushBatch(), true);

            const events = publishedEvents(ctx.client);
            assert.equal(events.length, 1);
            assert.equal(events[0].type, 'tag.changed.batch');
            assert.equal(events[0].version, 1);
            assert.equal(events[0].source, 'fuxa');
            assert.equal(typeof events[0].eventId, 'string');
            assert.equal(new Date(events[0].ts).toISOString(), events[0].ts);
            assert.equal(events[0].payload.count, 2);
            assert.deepEqual(events[0].payload.changes, [
                { deviceId: 'dev1', tagId: 'tag1', value: 7, timestamp: 1002, quality: 'bad' },
                { deviceId: 'dev1', tagId: 'tag2', value: 'a', timestamp: 1001, quality: 'good' }
            ]);

            const args = ctx.client.xaddCalls[0];
            assert.equal(args[0], 'stream:fuxa:events');
            assert.equal(args[args.length - 1], 'tag.changed.batch');
        });

        it('flushes automatically at the end of the window', async function () {
            const ctx = setup({ batchEnabled: true, batchWindowMs: 100, valuesEnabled: false });
            ctx.bridge.start();

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 1, timestamp: 1000, quality: 'good' } }
            });
            assert.equal(ctx.client.xaddCalls.length, 0);
            await sleep(220);

            const events = publishedEvents(ctx.client);
            assert.equal(events.length, 1);
            assert.equal(events[0].type, 'tag.changed.batch');
            assert.equal(events[0].payload.count, 1);
        });

        it('flushes the pending batch on stop', async function () {
            const ctx = setup({ batchEnabled: true, batchWindowMs: 500, valuesEnabled: false });
            ctx.bridge.start();

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 1, timestamp: 1000, quality: 'good' } }
            });
            assert.equal(ctx.client.xaddCalls.length, 0);

            await ctx.bridge.stop();
            assert.equal(ctx.client.xaddCalls.length, 1);
            assert.equal(publishedEvents(ctx.client)[0].type, 'tag.changed.batch');
        });

        it('handles 1,000+ changes without one XADD per change', function () {
            const ctx = setup({ batchEnabled: true, batchMax: 1000, valuesEnabled: true });
            ctx.bridge.start();

            const tagCount = 1500;
            const values = {};
            for (let index = 0; index < tagCount; index++) {
                values['tag' + index] = {
                    value: index,
                    timestamp: 1700000000000 + index,
                    quality: 'good'
                };
            }
            ctx.events.emit('device-value:changed', { id: 'dev1', values: values });
            // whatever is left after the last full batch goes out on the window
            ctx.bridge.flushBatch();

            const events = publishedEvents(ctx.client);
            assert.ok(events.length <= 2, 'one batch per 1000 changes, got ' + events.length);
            assert.equal(ctx.client.xaddCalls.length, events.length);
            assert.ok(ctx.client.xaddCalls.length < tagCount / 100,
                'XADD count must stay far below the change count');

            const changes = changesOf(events);
            assert.equal(changes.length, tagCount, 'every tag is delivered exactly once');
            const byTag = {};
            for (const change of changes) {
                byTag[change.tagId] = change;
            }
            assert.equal(Object.keys(byTag).length, tagCount);
            assert.equal(byTag.tag0.value, 0);
            assert.equal(byTag.tag1499.value, 1499);

            // values key: 1500 dirty tags written in 500-field chunks
            ctx.bridge.flushValues();
            assert.equal(ctx.client.hsetCalls.length, Math.ceil(tagCount / 500));
            const written = writtenHash(ctx.client);
            assert.equal(written.key, 'fuxa:catalog:values');
            assert.equal(Object.keys(written.values).length, tagCount);
            assert.deepEqual(written.values.tag42, {
                value: 42,
                timestamp: 1700000000042,
                quality: 'good',
                deviceId: 'dev1'
            });
        });
    });

    describe('live values key', function () {
        it('writes one HSET per flush with the latest value per tag', function () {
            const ctx = setup();
            ctx.bridge.start();
            assert.equal(ctx.bridge.valuesKey, 'fuxa:catalog:values');
            assert.equal(ctx.bridge.valuesKeyType, 'hash');

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: {
                    tag1: { value: 10, timestamp: 1700000000000, quality: 'good' },
                    tag2: { value: false, timestamp: 1700000001000, quality: 'bad' }
                }
            });
            ctx.events.emit('device-value:changed', {
                id: 'dev2',
                values: { tag1: { value: 11, timestamp: 1700000002000, quality: 'good' } }
            });
            assert.equal(ctx.client.hsetCalls.length, 0, 'values are flushed on the interval, not per change');

            assert.equal(ctx.bridge.flushValues(), true);
            assert.equal(ctx.client.hsetCalls.length, 1);

            const written = writtenHash(ctx.client);
            assert.equal(written.key, 'fuxa:catalog:values');
            assert.deepEqual(written.values, {
                tag1: { value: 11, timestamp: 1700000002000, quality: 'good', deviceId: 'dev2' },
                tag2: { value: false, timestamp: 1700000001000, quality: 'bad', deviceId: 'dev1' }
            });
            // nothing dirty: a second flush is a no-op
            assert.equal(ctx.bridge.flushValues(), true);
            assert.equal(ctx.client.hsetCalls.length, 1);
        });

        it('flushes values on the interval', async function () {
            const ctx = setup({ valuesFlushMs: 100 });
            ctx.bridge.start();

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 1, timestamp: 1000, quality: 'good' } }
            });
            await sleep(260);

            assert.ok(ctx.client.hsetCalls.length >= 1);
            assert.equal(writtenHash(ctx.client).values.tag1.value, 1);
        });

        it('rewrites the whole map after a reconnect', function () {
            const ctx = setup();
            ctx.bridge.start();

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: {
                    tag1: { value: 1, timestamp: 1000, quality: 'good' },
                    tag2: { value: 2, timestamp: 1001, quality: 'good' }
                }
            });
            ctx.bridge.flushValues();
            assert.equal(ctx.client.hsetCalls.length, 1);

            ctx.client.listeners['ready']();
            assert.equal(ctx.client.hsetCalls.length, 2, 'a reconnect republishes every known value');
            assert.deepEqual(Object.keys(writtenHash(ctx.client).values).sort(), ['tag1', 'tag2']);
        });

        it('keeps the legacy JSON document when valuesKeyType is string', function () {
            const ctx = setup({ valuesKeyType: 'string' });
            ctx.bridge.start();
            assert.equal(ctx.bridge.valuesKeyType, 'string');

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 5, timestamp: 1000, quality: 'good' } }
            });
            ctx.bridge.flushValues();

            assert.equal(ctx.client.hsetCalls.length, 0);
            assert.equal(ctx.client.setCalls.length, 1);
            const args = ctx.client.setCalls[0];
            assert.equal(args[0], 'fuxa:catalog:values');
            const body = JSON.parse(args[1]);
            assert.equal(body.version, 1);
            assert.equal(body.count, 1);
            assert.deepEqual(body.values.tag1, {
                value: 5,
                timestamp: 1000,
                quality: 'good',
                deviceId: 'dev1'
            });
        });

        it('can be disabled', function () {
            const ctx = setup({ valuesEnabled: false });
            ctx.bridge.start();

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 1, timestamp: 1000, quality: 'good' } }
            });
            assert.equal(ctx.bridge.flushValues(), false);
            assert.equal(ctx.client.hsetCalls.length, 0);
            assert.deepEqual(ctx.bridge.getLatestValues(), {});
        });

        it('parses env configuration and clamps unsafe values', function () {
            const config = redisBridge.resolveConfig({}, {
                FUXA_REDIS_BATCH_ENABLED: 'true',
                FUXA_REDIS_BATCH_WINDOW_MS: '5',
                FUXA_REDIS_BATCH_MAX: '0',
                FUXA_REDIS_VALUES_KEY_TYPE: 'json',
                FUXA_REDIS_VALUES_FLUSH_MS: '999999',
                FUXA_REDIS_VALUES_MAX_TAGS: 'abc'
            });
            assert.equal(config.batchEnabled, true);
            assert.equal(config.batchWindowMs, 100, 'window is clamped to the minimum');
            assert.equal(config.batchMax, 1, 'batch max is clamped to at least one change');
            assert.equal(config.valuesKeyType, 'string');
            assert.equal(config.valuesFlushMs, 60000);
            assert.equal(config.valuesMaxTags, 20000);
            assert.equal(config.valuesEnabled, true, 'values key is on by default');

            const defaults = redisBridge.resolveConfig({}, {});
            assert.equal(defaults.batchEnabled, false, 'batching stays opt-in');
            assert.equal(defaults.batchWindowMs, 200);
            assert.equal(defaults.batchMax, 1000);
            assert.equal(defaults.valuesKey, 'fuxa:catalog:values');
            assert.equal(defaults.valuesKeyType, 'hash');
            assert.equal(defaults.valuesFlushMs, 1000);
        });
    });

    describe('failure isolation', function () {
        it('never throws when xadd and hset fail', function () {
            const client = createFakeClient({
                xadd: function () { throw new Error('stream down'); },
                hset: function () { throw new Error('hash down'); }
            });
            const ctx = setup({ client: client, batchEnabled: true, valuesEnabled: true });
            assert.equal(ctx.bridge.start(), true);

            assert.doesNotThrow(function () {
                ctx.events.emit('device-value:changed', {
                    id: 'dev1',
                    values: { tag1: { value: 1, timestamp: 1000, quality: 'good' } }
                });
                ctx.bridge.flushBatch();
                ctx.bridge.flushValues();
            });
            assert.ok(ctx.logger.entries.error.length >= 1);
            assert.equal(ctx.bridge.isStarted(), true);
        });

        it('asynchronous rejections do not propagate', async function () {
            const client = createFakeClient({
                xadd: function () { return Promise.reject(new Error('stream down')); },
                hset: function () { return Promise.reject(new Error('hash down')); }
            });
            const ctx = setup({ client: client, batchEnabled: true });
            ctx.bridge.start();

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 1, timestamp: 1000, quality: 'good' } }
            });
            ctx.bridge.flushBatch();
            ctx.bridge.flushValues();
            await new Promise(function (resolve) { setImmediate(resolve); });

            assert.ok(ctx.logger.entries.error.length >= 1);
            assert.equal(ctx.bridge.isStarted(), true);
        });

        it('keeps the readiness guard when Redis is not ready', function () {
            const client = createFakeClient({
                connect: function () { this.connectCalls++; return Promise.resolve(); } // stays 'wait'
            });
            const ctx = setup({ client: client, batchEnabled: true });
            ctx.bridge.start();

            assert.doesNotThrow(function () {
                ctx.events.emit('device-value:changed', {
                    id: 'dev1',
                    values: { tag1: { value: 1, timestamp: 1000, quality: 'good' } }
                });
                assert.equal(ctx.bridge.flushBatch(), false);
                assert.equal(ctx.bridge.flushValues(), false);
            });
            assert.equal(client.xaddCalls.length, 0);
            assert.equal(client.hsetCalls.length, 0);
        });

        it('does not publish the batch when the client drops mid-window', function () {
            const ctx = setup({ batchEnabled: true, valuesEnabled: false });
            ctx.bridge.start();

            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 1, timestamp: 1000, quality: 'good' } }
            });
            ctx.client.status = 'reconnecting';
            assert.equal(ctx.bridge.flushBatch(), false);
            assert.equal(ctx.client.xaddCalls.length, 0);
            assert.equal(ctx.bridge.isStarted(), true);
        });
    });
});
