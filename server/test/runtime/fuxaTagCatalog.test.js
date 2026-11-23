'use strict';

const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const fuxaTagCatalog = require('../../runtime/fuxaTagCatalog');

function createLogger() {
    const entries = { info: [], warn: [], error: [] };
    return {
        entries,
        info(message) { entries.info.push(message); },
        warn(message) { entries.warn.push(message); },
        error(message) { entries.error.push(message); }
    };
}

function tag(id, overrides) {
    return Object.assign({
        id: id,
        name: id,
        type: 'Bool',
        address: '1',
        description: id + ' description'
    }, overrides || {});
}

function setup(config) {
    config = config || {};
    const events = new EventEmitter();
    const logger = createLogger();
    const published = [];
    const state = {
        devices: config.devices || {}
    };
    const bridge = config.bridge === undefined
        ? {
            publish: function (type, payload) {
                published.push({ type: type, payload: payload });
                return true;
            }
        }
        : config.bridge;
    const catalog = fuxaTagCatalog.create({
        events: events,
        logger: logger,
        bridge: function () { return bridge; },
        getDevices: function () { return state.devices; },
        debounceMs: config.debounceMs !== undefined ? config.debounceMs : 0
    });
    return { events, logger, published, state, catalog };
}

function typesOf(published) {
    return published.map(function (entry) { return entry.type; });
}

describe('runtime fuxa tag catalog', function () {
    it('publishes one snapshot with every project tag on start', function () {
        const ctx = setup({
            devices: {
                dev1: { id: 'dev1', name: 'PLC 1', tags: { t1: tag('t1'), t2: tag('t2', { type: 'Word' }) } },
                dev2: { id: 'dev2', name: 'PLC 2', tags: { t3: tag('t3', { description: undefined, address: 42 }) } }
            }
        });

        assert.equal(ctx.catalog.start(), true);
        assert.equal(ctx.published.length, 1);

        const snapshot = ctx.published[0];
        assert.equal(snapshot.type, 'tag.snapshot');
        assert.equal(snapshot.payload.count, 3);
        assert.equal(snapshot.payload.tags.length, 3);

        const byId = {};
        snapshot.payload.tags.forEach(function (entry) { byId[entry.tagId] = entry; });
        assert.deepEqual(byId.t1, {
            tagId: 't1',
            deviceId: 'dev1',
            name: 't1',
            description: 't1 description',
            type: 'Bool',
            address: '1',
            dataType: 'Bool',
            unit: null,
            source: 'fuxa'
        });
        assert.equal(byId.t2.type, 'Word');
        assert.equal(byId.t3.description, null);
        assert.equal(byId.t3.address, '42');
        assert.equal(byId.t3.deviceId, 'dev2');
    });

    it('publishes tag.created, tag.updated and tag.deleted on project changes', function () {
        const ctx = setup({
            devices: {
                dev1: { id: 'dev1', tags: { t1: tag('t1'), t2: tag('t2') } }
            }
        });
        ctx.catalog.start();
        ctx.published.length = 0;

        const device = ctx.state.devices.dev1;
        device.tags.t1.description = 'changed';
        delete device.tags.t2;
        device.tags.t3 = tag('t3');

        ctx.events.emit('project-tags:changed', { deviceId: 'dev1', device: device, change: 'set-device' });

        assert.deepEqual(typesOf(ctx.published), ['tag.created', 'tag.updated', 'tag.deleted']);
        assert.equal(ctx.published[0].payload.tagId, 't3');
        assert.equal(ctx.published[1].payload.tagId, 't1');
        assert.equal(ctx.published[1].payload.description, 'changed');
        assert.equal(ctx.published[2].payload.tagId, 't2');
        assert.equal(ctx.published[2].payload.deviceId, 'dev1');
    });

    it('publishes nothing when the catalog did not change', function () {
        const ctx = setup({
            devices: { dev1: { id: 'dev1', tags: { t1: tag('t1') } } }
        });
        ctx.catalog.start();
        ctx.published.length = 0;

        ctx.events.emit('project-tags:changed', { deviceId: 'dev1' });
        ctx.events.emit('project-tags:changed', { deviceId: 'dev1' });
        ctx.catalog.flush();

        assert.equal(ctx.published.length, 0);
    });

    it('debounces bursts of project changes into a single diff', async function () {
        const ctx = setup({
            devices: { dev1: { id: 'dev1', tags: {} } },
            debounceMs: 10
        });
        ctx.catalog.start();
        ctx.published.length = 0;

        ctx.state.devices.dev1.tags.t1 = tag('t1');
        ctx.events.emit('project-tags:changed', { deviceId: 'dev1' });
        ctx.events.emit('project-tags:changed', { deviceId: 'dev1' });
        ctx.state.devices.dev1.tags.t2 = tag('t2');
        ctx.events.emit('project-tags:changed', { deviceId: 'dev1' });

        assert.equal(ctx.published.length, 0, 'changes must wait for the debounce window');
        await new Promise(function (resolve) { setTimeout(resolve, 40); });

        assert.deepEqual(typesOf(ctx.published), ['tag.created', 'tag.created']);
    });

    it('does nothing and never throws when the bridge is disabled', function () {
        const disabled = { publish: function () { return false; } };
        const ctx = setup({
            devices: { dev1: { id: 'dev1', tags: { t1: tag('t1') } } },
            bridge: disabled
        });

        assert.equal(ctx.catalog.start(), false);
        assert.doesNotThrow(function () {
            ctx.state.devices.dev1.tags.t2 = tag('t2');
            ctx.events.emit('project-tags:changed', { deviceId: 'dev1' });
        });
        assert.equal(ctx.catalog.isStarted(), true);
    });

    it('does nothing when no bridge is available', function () {
        const ctx = setup({
            devices: { dev1: { id: 'dev1', tags: { t1: tag('t1') } } },
            bridge: null
        });

        assert.equal(ctx.catalog.start(), false);
        assert.doesNotThrow(function () {
            ctx.events.emit('project-tags:changed', { deviceId: 'dev1' });
        });
    });

    it('never propagates redis failures', function () {
        const failing = {
            publish: function () { throw new Error('stream down'); }
        };
        const ctx = setup({
            devices: { dev1: { id: 'dev1', tags: { t1: tag('t1') } } },
            bridge: failing
        });

        assert.doesNotThrow(function () { ctx.catalog.start(); });
        assert.doesNotThrow(function () {
            ctx.state.devices.dev1.tags.t2 = tag('t2');
            ctx.events.emit('project-tags:changed', { deviceId: 'dev1' });
        });
        assert.equal(ctx.logger.entries.error.length, 1);
        assert.equal(ctx.catalog.isStarted(), true);
    });

    it('republishes the snapshot when the bridge becomes ready', function () {
        let readyHandler = null;
        const readyBridge = {
            publish: function (type, payload) {
                ctx.published.push({ type: type, payload: payload });
                return true;
            },
            setReadyHandler: function (handler) { readyHandler = handler; }
        };
        const ctx = setup({
            devices: { dev1: { id: 'dev1', tags: { t1: tag('t1') } } },
            bridge: readyBridge
        });

        ctx.catalog.start();
        assert.equal(ctx.published.length, 1);
        assert.equal(typeof readyHandler, 'function', 'the catalog registers a ready handler');

        ctx.state.devices.dev1.tags.t2 = tag('t2');
        readyHandler();
        assert.deepEqual(typesOf(ctx.published), ['tag.snapshot', 'tag.snapshot']);
        assert.equal(ctx.published[1].payload.count, 2);

        ctx.catalog.stop();
        assert.equal(readyHandler, null, 'stop unregisters the ready handler');
    });

    it('stops publishing and unbinds listeners after stop', function () {
        const ctx = setup({
            devices: { dev1: { id: 'dev1', tags: { t1: tag('t1') } } }
        });
        ctx.catalog.start();
        assert.equal(ctx.events.listenerCount('project-tags:changed'), 1);

        ctx.catalog.stop();
        assert.equal(ctx.events.listenerCount('project-tags:changed'), 0);
        ctx.published.length = 0;

        ctx.state.devices.dev1.tags.t2 = tag('t2');
        ctx.events.emit('project-tags:changed', { deviceId: 'dev1' });
        assert.equal(ctx.published.length, 0);
        assert.equal(ctx.catalog.start(), true, 'a restart republishes the snapshot');
        assert.equal(ctx.published[0].type, 'tag.snapshot');
    });

    it('republishes the snapshot with the new baseline on restart', function () {
        const ctx = setup({
            devices: { dev1: { id: 'dev1', tags: { t1: tag('t1') } } }
        });
        ctx.catalog.start();
        ctx.catalog.stop();
        ctx.published.length = 0;

        ctx.catalog.start();
        assert.deepEqual(typesOf(ctx.published), ['tag.snapshot']);
        assert.equal(ctx.published[0].payload.count, 1);
    });

    it('ignores malformed devices and tags', function () {
        const ctx = setup({
            devices: {
                broken: null,
                noTags: { id: 'noTags' },
                dev1: { id: 'dev1', tags: { good: tag('good'), bad: null } }
            }
        });

        assert.doesNotThrow(function () { ctx.catalog.start(); });
        assert.equal(ctx.published.length, 1);
        assert.deepEqual(ctx.published[0].payload.tags.map(function (entry) { return entry.tagId; }), ['good']);
    });
});
