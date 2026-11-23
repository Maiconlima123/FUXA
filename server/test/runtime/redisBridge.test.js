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

function createFakeClient(overrides) {
    const client = {
        status: 'wait',
        connectCalls: 0,
        quitCalls: 0,
        xaddCalls: [],
        listeners: {},
        connect() { this.connectCalls++; this.status = 'ready'; return Promise.resolve(); },
        xadd() { this.xaddCalls.push(Array.from(arguments)); return Promise.resolve('1-0'); },
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
    const clients = [];
    const bridge = redisBridge.create({
        events: events,
        logger: logger,
        env: config.env || { FUXA_REDIS_ENABLED: 'true' },
        createClient: function () {
            const client = config.client || createFakeClient();
            clients.push(client);
            return client;
        }
    });
    return { events: events, logger: logger, clients: clients, bridge: bridge };
}

function publishedEvents(client) {
    return client.xaddCalls.map(function (args) {
        const eventIndex = args.indexOf('event');
        assert.notEqual(eventIndex, -1, 'xadd must carry an event field');
        return JSON.parse(args[eventIndex + 1]);
    });
}

describe('runtime redis bridge', function () {
    it('does nothing when disabled', function () {
        const ctx = setup({ env: { FUXA_REDIS_ENABLED: 'false' } });
        assert.equal(ctx.bridge.start(), false);
        assert.equal(ctx.clients.length, 0, 'no client must be created while disabled');

        assert.doesNotThrow(function () {
            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 1, timestamp: 1000, quality: 'good' } }
            });
            ctx.events.emit('device-status:changed', { id: 'dev1', status: 'connect-ok' });
        });
        assert.equal(ctx.bridge.isStarted(), false);
    });

    it('publishes one tag.changed event per tag', function () {
        const ctx = setup();
        assert.equal(ctx.bridge.start(), true);
        const client = ctx.clients[0];
        assert.equal(client.connectCalls, 1);

        ctx.events.emit('device-value:changed', {
            id: 'dev1',
            values: {
                tag1: { id: 'tag1', value: 10, timestamp: 1700000000000, quality: 'good' },
                tag2: { id: 'tag2', value: false, timestamp: 1700000001000, quality: 'bad' }
            }
        });

        const events = publishedEvents(client);
        assert.equal(events.length, 2);
        assert.equal(client.xaddCalls.length, 2);

        const first = events[0];
        assert.equal(first.version, 1);
        assert.equal(first.type, 'tag.changed');
        assert.equal(first.source, 'fuxa');
        assert.equal(typeof first.eventId, 'string');
        assert.equal(new Date(first.ts).toISOString(), first.ts);
        assert.deepEqual(first.payload, {
            deviceId: 'dev1',
            tagId: 'tag1',
            value: 10,
            timestamp: 1700000000000,
            quality: 'good'
        });
        assert.deepEqual(events[1].payload, {
            deviceId: 'dev1',
            tagId: 'tag2',
            value: false,
            timestamp: 1700000001000,
            quality: 'bad'
        });
        assert.notEqual(first.eventId, events[1].eventId);
    });

    it('publishes device.updated on device status changes', function () {
        const ctx = setup();
        ctx.bridge.start();
        const client = ctx.clients[0];

        ctx.events.emit('device-status:changed', { id: 'dev1', status: 'connect-ok' });

        const events = publishedEvents(client);
        assert.equal(events.length, 1);
        assert.equal(events[0].type, 'device.updated');
        assert.deepEqual(events[0].payload, { deviceId: 'dev1', status: 'connect-ok' });
    });

    it('publishes alarm.status with the aggregate payload as-is', function () {
        const ctx = setup();
        ctx.bridge.start();
        const client = ctx.clients[0];
        const aggregate = { alarms: [{ id: 'a1', status: 'active' }], count: 1 };

        ctx.events.emit('alarms-status:changed', aggregate);

        const events = publishedEvents(client);
        assert.equal(events.length, 1);
        assert.equal(events[0].type, 'alarm.status');
        assert.deepEqual(events[0].payload, aggregate);
    });

    it('publishes project-device changes only when a device id is present', function () {
        const ctx = setup();
        ctx.bridge.start();
        const client = ctx.clients[0];

        ctx.events.emit('project-device:change', { name: 'no-id' });
        assert.equal(client.xaddCalls.length, 0);

        ctx.events.emit('project-device:change', { id: 'dev1', status: 'connect-ok' });
        const events = publishedEvents(client);
        assert.equal(events.length, 1);
        assert.equal(events[0].type, 'device.updated');
        assert.deepEqual(events[0].payload, { deviceId: 'dev1', status: 'connect-ok' });
    });

    it('uses approximate MAXLEN when configured', function () {
        const ctx = setup({ env: { FUXA_REDIS_ENABLED: 'true', FUXA_REDIS_MAXLEN: '1000' } });
        ctx.bridge.start();
        const client = ctx.clients[0];

        ctx.events.emit('device-status:changed', { id: 'dev1', status: 'connect-ok' });

        const args = client.xaddCalls[0];
        assert.equal(args[0], 'stream:fuxa:events');
        assert.deepEqual(args.slice(1, 5), ['MAXLEN', '~', 1000, '*']);
    });

    it('does not propagate synchronous xadd failures', function () {
        const failing = createFakeClient({
            xadd: function () { throw new Error('stream down'); }
        });
        const ctx = setup({ client: failing });
        assert.equal(ctx.bridge.start(), true);

        assert.doesNotThrow(function () {
            ctx.events.emit('device-value:changed', {
                id: 'dev1',
                values: { tag1: { value: 1, timestamp: 1000, quality: 'good' } }
            });
        });
        assert.equal(ctx.logger.entries.error.length, 1);
        assert.equal(ctx.bridge.isStarted(), true);
    });

    it('does not propagate asynchronous xadd rejections', async function () {
        const failing = createFakeClient({
            xadd: function () { return Promise.reject(new Error('stream down')); }
        });
        const ctx = setup({ client: failing });
        ctx.bridge.start();

        ctx.events.emit('device-status:changed', { id: 'dev1', status: 'connect-ok' });
        await new Promise(function (resolve) { setImmediate(resolve); });

        assert.equal(ctx.logger.entries.error.length, 1);
        assert.equal(ctx.bridge.isStarted(), true);
    });

    it('stops cleanly, removing listeners and closing the client', async function () {
        const ctx = setup();
        ctx.bridge.start();
        const client = ctx.clients[0];
        assert.equal(ctx.events.listenerCount('device-value:changed'), 1);

        await ctx.bridge.stop();
        assert.equal(ctx.events.listenerCount('device-value:changed'), 0);
        assert.equal(client.quitCalls, 1);
        assert.equal(ctx.bridge.isStarted(), false);

        ctx.events.emit('device-status:changed', { id: 'dev1', status: 'connect-ok' });
        assert.equal(client.xaddCalls.length, 0);
    });
});
