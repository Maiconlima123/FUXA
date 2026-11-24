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
            protocol: null,
            deviceType: null,
            type: 'Bool',
            tagType: 'Bool',
            address: '1',
            dataType: 'Bool',
            registerType: null,
            unit: null,
            source: 'fuxa'
        });
        assert.equal(byId.t2.type, 'Word');
        assert.equal(byId.t3.description, null);
        assert.equal(byId.t3.address, '42');
        assert.equal(byId.t3.deviceId, 'dev2');
    });

    it('derives protocol metadata from the device instead of the tag', function () {
        const ctx = setup({
            devices: {
                s7: {
                    id: 's7',
                    type: 'SiemensS7',
                    property: { password: 's7-secret', address: '10.0.0.10' },
                    tags: { t1: tag('t1', { address: 'DB1,D0.0' }) }
                },
                mb: {
                    id: 'mb',
                    type: 'ModbusTCP',
                    tags: {
                        coil: tag('coil', { address: '1', memaddress: '000000' }),
                        discrete: tag('discrete', { address: '2', memaddress: '100000' }),
                        input: tag('input', { address: '3', memaddress: '300000', type: 'UInt16' }),
                        holding: tag('holding', { address: '4', memaddress: 400000, type: 'Float32' }),
                        explicit: tag('explicit', { address: '5', type: 'UInt16', options: { registerType: 'Holding Registers' } }),
                        plain: tag('plain', { address: '40002', type: 'UInt16', memaddress: undefined })
                    }
                },
                mqtt: {
                    id: 'mqtt',
                    type: 'MQTTclient',
                    tags: { topic: tag('topic', { type: 'number', options: { subs: ['plant/temp'] } }) }
                },
                opc: {
                    id: 'opc',
                    type: 'OPCUA',
                    tags: { node: tag('node', { address: 'ns=2;s=Plant.Temp', type: 'Double' }) }
                },
                iot: {
                    id: 'iot',
                    type: 'IoTHTTP',
                    tags: { sensor: tag('sensor', { address: 'device-1', memaddress: 'data.sensor.value', type: 'number' }) }
                },
                web: { id: 'web', type: 'WebAPI', tags: { web: tag('web') } },
                eip: { id: 'eip', type: 'EthernetIP', tags: { eip: tag('eip') } },
                custom: { id: 'custom', type: 'CustomDriver', tags: { custom: tag('custom') } }
            }
        });

        ctx.catalog.start();
        const byId = {};
        ctx.published[0].payload.tags.forEach(function (entry) { byId[entry.tagId] = entry; });

        assert.equal(byId.t1.protocol, 's7');
        assert.equal(byId.t1.deviceType, 'SiemensS7');
        assert.equal(byId.t1.registerType, null);
        assert.equal(byId.t1.tagType, 'Bool');

        assert.equal(byId.coil.protocol, 'modbus-tcp');
        assert.equal(byId.coil.deviceType, 'ModbusTCP');
        assert.equal(byId.coil.registerType, 'coil');
        assert.equal(byId.discrete.registerType, 'discrete-input');
        assert.equal(byId.input.registerType, 'input-register');
        assert.equal(byId.holding.registerType, 'holding-register');
        assert.equal(byId.explicit.registerType, 'holding-register');
        assert.equal(byId.plain.registerType, null, 'the register type is not guessed from the address');
        assert.equal(byId.plain.address, '40002');
        assert.equal(byId.plain.dataType, 'UInt16');

        assert.equal(byId.topic.protocol, 'mqtt');
        assert.equal(byId.topic.tagType, 'number');
        assert.equal(byId.topic.registerType, null);
        assert.equal(byId.node.protocol, 'opcua');
        assert.equal(byId.sensor.protocol, 'iothttp');
        assert.equal(byId.web.protocol, 'httprequest');
        assert.equal(byId.eip.protocol, 'ethernetip');
        assert.equal(byId.custom.protocol, 'custom-driver');
        assert.equal(byId.custom.deviceType, 'CustomDriver');
        assert.equal(byId.custom.registerType, null);

        const serialized = JSON.stringify(ctx.published[0].payload);
        assert.equal(serialized.includes('s7-secret'), false, 'device credentials never leak');
    });

    it('normalizes Modbus RTU and keeps fields null when the driver type is unknown', function () {
        const ctx = setup({
            devices: {
                rtu: {
                    id: 'rtu',
                    type: 'ModbusRTU',
                    tags: { hr: tag('hr', { address: '10', memaddress: '400000', type: 'Int16' }) }
                },
                untyped: { id: 'untyped', tags: { t1: tag('t1') } }
            }
        });

        ctx.catalog.start();
        const byId = {};
        ctx.published[0].payload.tags.forEach(function (entry) { byId[entry.tagId] = entry; });

        assert.equal(byId.hr.protocol, 'modbus-rtu');
        assert.equal(byId.hr.deviceType, 'ModbusRTU');
        assert.equal(byId.hr.registerType, 'holding-register');
        assert.equal(byId.t1.protocol, null);
        assert.equal(byId.t1.deviceType, null);
        assert.equal(byId.t1.address, '1');
        assert.equal(byId.t1.unit, null);
        assert.equal(byId.t1.source, 'fuxa');
    });

    it('keeps the durable catalog key backward compatible with the new fields', function () {
        let stored = null;
        const bridge = {
            publish: function () { return true; },
            client: {
                status: 'ready',
                set: function (key, body) {
                    stored = { key: key, body: body };
                    return Promise.resolve('OK');
                }
            }
        };
        const ctx = setup({
            devices: {
                mb: { id: 'mb', type: 'ModbusTCP', tags: { hr: tag('hr', { address: '7', memaddress: '400000', type: 'UInt16' }) } }
            },
            bridge: bridge
        });

        ctx.catalog.start();
        assert.equal(stored.key, 'fuxa:catalog:tags');
        const body = JSON.parse(stored.body);
        assert.equal(body.version, 1);
        assert.equal(body.count, 1);
        assert.equal(body.tags[0].tagId, 'hr');
        assert.equal(body.tags[0].protocol, 'modbus-tcp');
        assert.equal(body.tags[0].deviceType, 'ModbusTCP');
        assert.equal(body.tags[0].registerType, 'holding-register');
        assert.equal(body.tags[0].unit, null);
        assert.equal(body.tags[0].source, 'fuxa');
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
