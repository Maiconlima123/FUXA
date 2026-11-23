'use strict';

const assert = require('node:assert/strict');
const redisCommandBridge = require('../../runtime/redisCommandBridge');

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
        disconnectCalls: 0,
        quitCalls: 0,
        xgroupCalls: [],
        xreadgroupCalls: [],
        xackCalls: [],
        xaddCalls: [],
        listeners: {},
        inbox: [],
        connect() {
            this.connectCalls++;
            this.status = 'ready';
            return Promise.resolve();
        },
        xgroup() {
            this.xgroupCalls.push(Array.from(arguments));
            return Promise.resolve('OK');
        },
        xreadgroup() {
            this.xreadgroupCalls.push(Array.from(arguments));
            if (this.inbox.length) {
                return Promise.resolve(this.inbox.shift());
            }
            // emulate BLOCK returning after a short timeout instead of a busy loop
            return new Promise(function (resolve) {
                setTimeout(function () { resolve(null); }, 5);
            });
        },
        xack() {
            this.xackCalls.push(Array.from(arguments));
            return Promise.resolve(1);
        },
        xadd() {
            this.xaddCalls.push(Array.from(arguments));
            return Promise.resolve('1-0');
        },
        on(name, listener) {
            this.listeners[name] = listener;
        },
        disconnect() {
            this.disconnectCalls++;
            this.status = 'end';
        },
        quit() {
            this.quitCalls++;
            this.status = 'end';
            return Promise.resolve('OK');
        }
    };
    return Object.assign(client, overrides || {});
}

function createFakeDevices(overrides) {
    const state = Object.assign({
        active: true,
        writeResult: true,
        writeNeverResolves: false,
        keepValue: false,
        values: { tag1: { value: 1 } },
        writes: [],
        getTagValueThrows: false,
        tags: {
            tag1: { id: 'tag1', name: 'tag1', description: 'Counter', type: 'Int' },
            bool1: { id: 'bool1', name: 'bool1', description: 'Breaker', type: 'Bool' }
        }
    }, overrides || {});

    state.values = Object.assign({ tag1: { value: 1 }, bool1: { value: false } }, state.values || {});

    const devicesMap = {
        dev1: {
            id: 'dev1',
            name: 'Device 1',
            tags: state.tags
        }
    };

    return {
        state: state,
        getDevices() { return devicesMap; },
        getDeviceIdFromTag(tagId) {
            if (!state.active) {
                return null;
            }
            return Object.values(devicesMap.dev1.tags).some(function (tag) {
                return String(tag.id) === String(tagId);
            }) ? 'dev1' : null;
        },
        setTagValue(tagId, value) {
            state.writes.push({ tagId: tagId, value: value });
            if (state.writeNeverResolves) {
                return new Promise(function () {});
            }
            if (state.writeResult === false || state.writeResult === null) {
                return Promise.resolve(state.writeResult);
            }
            if (!state.keepValue) {
                state.values[tagId] = { value: value };
            }
            return Promise.resolve(state.writeResult);
        },
        getTagValue(tagId, fully) {
            if (state.getTagValueThrows) {
                throw new Error('read boom');
            }
            const entry = state.values[tagId];
            if (!entry) {
                return null;
            }
            return fully ? { id: tagId, value: entry.value, ts: Date.now() } : entry.value;
        }
    };
}

function buildCommand(overrides) {
    return Object.assign({
        version: 1,
        commandId: 'cmd-1',
        type: 'tag.write',
        ts: '2026-11-23T10:00:00.000Z',
        source: 'maintenance',
        payload: {
            tagId: 'tag1',
            value: 42,
            requestedBy: 'maintenance-agent',
            reason: 'breaker calibration test'
        }
    }, overrides || {});
}

function toFields(command) {
    return ['event', JSON.stringify(command), 'type', command.type];
}

function setup(config) {
    config = config || {};
    const logger = createLogger();
    const clients = [];
    const client = config.client || createFakeClient();
    const devices = config.devices || createFakeDevices();
    const bridge = redisCommandBridge.create(Object.assign({
        logger: logger,
        env: config.env || { FUXA_REDIS_COMMAND_ENABLED: 'true' },
        devices: devices,
        blockMs: 5,
        timeoutMs: 100,
        createClient: function () {
            clients.push(client);
            return client;
        }
    }, config.options || {}));
    return { bridge: bridge, client: client, clients: clients, devices: devices, logger: logger };
}

function resultEvents(client) {
    return client.xaddCalls.map(function (args) {
        const eventIndex = args.indexOf('event');
        assert.notEqual(eventIndex, -1, 'xadd must carry an event field');
        return JSON.parse(args[eventIndex + 1]);
    });
}

function waitFor(predicate, timeoutMs) {
    const deadline = Date.now() + (timeoutMs || 1000);
    return new Promise(function (resolve, reject) {
        function step() {
            let value;
            try {
                value = predicate();
            } catch (err) {
                reject(err);
                return;
            }
            if (value) {
                resolve(value);
                return;
            }
            if (Date.now() > deadline) {
                reject(new Error('timed out waiting for condition'));
                return;
            }
            setTimeout(step, 5);
        }
        step();
    });
}

describe('runtime redis command bridge', function () {
    it('is disabled by default and does nothing', async function () {
        const ctx = setup({ env: { FUXA_REDIS_COMMAND_ENABLED: 'false' } });

        assert.equal(ctx.bridge.start(), false);
        assert.equal(ctx.clients.length, 0, 'no client must be created while disabled');
        assert.equal(ctx.bridge.isStarted(), false);

        const result = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand()));
        assert.equal(result, null);
        assert.equal(ctx.client.xaddCalls.length, 0);
        assert.equal(ctx.client.xackCalls.length, 0);
        assert.equal(ctx.devices.state.writes.length, 0);
    });

    it('executes a valid tag.write and publishes a result with the read-back value', async function () {
        const ctx = setup();
        assert.equal(ctx.bridge.start(), true);

        const result = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand()));

        assert.equal(result.commandId, 'cmd-1');
        assert.equal(result.commandType, 'tag.write');
        assert.equal(result.ok, true);
        assert.equal(result.status, 'ok');
        assert.equal(result.tagId, 'tag1');
        assert.equal(result.requestedValue, 42);
        assert.equal(result.value, 42);
        assert.equal(result.requestedBy, 'maintenance-agent');
        assert.equal(result.error, null);
        assert.equal(typeof result.ts, 'string');

        assert.equal(ctx.devices.state.writes.length, 1);
        assert.deepEqual(ctx.devices.state.writes[0], { tagId: 'tag1', value: 42 });

        const events = resultEvents(ctx.client);
        assert.equal(events.length, 1);
        assert.equal(events[0].version, 1);
        assert.equal(events[0].type, 'command.result');
        assert.equal(events[0].source, 'fuxa');
        assert.equal(typeof events[0].eventId, 'string');
        assert.deepEqual(events[0].payload, result);

        const xadd = ctx.client.xaddCalls[0];
        assert.equal(xadd[0], 'stream:fuxa:events');
        assert.equal(xadd[xadd.indexOf('type') + 1], 'command.result');

        assert.equal(ctx.client.xackCalls.length, 1);
        assert.deepEqual(ctx.client.xackCalls[0], ['stream:fuxa:commands', 'fuxa-command-executor', '1-0']);

        assert.ok(ctx.logger.entries.info.some(function (line) {
            return line.indexOf('requestedBy=maintenance-agent') !== -1 &&
                line.indexOf('reason=breaker calibration test') !== -1;
        }));

        await ctx.bridge.stop();
        assert.equal(ctx.client.disconnectCalls, 1);
        assert.equal(ctx.bridge.isStarted(), false);
    });

    it('normalizes boolean values for Bool tags', async function () {
        const ctx = setup();
        ctx.bridge.start();

        const result = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand({
            commandId: 'cmd-bool',
            payload: { tagId: 'bool1', value: 'true', requestedBy: 'maint' }
        })));

        assert.equal(result.ok, true);
        assert.equal(result.requestedValue, true);
        assert.equal(result.value, true);
        assert.deepEqual(ctx.devices.state.writes[0], { tagId: 'bool1', value: true });

        await ctx.bridge.stop();
    });

    it('rejects unsupported commands without executing anything', async function () {
        const ctx = setup();
        ctx.bridge.start();

        const result = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand({
            commandId: 'cmd-script',
            type: 'script.run'
        })));

        assert.equal(result.ok, false);
        assert.equal(result.commandType, 'script.run');
        assert.equal(result.status, 'unsupported-command');
        assert.equal(result.error.code, 'unsupported-command');
        assert.equal(ctx.devices.state.writes.length, 0);
        assert.equal(resultEvents(ctx.client).length, 1);
        assert.equal(ctx.client.xackCalls.length, 1);

        await ctx.bridge.stop();
    });

    it('rejects missing tags', async function () {
        const ctx = setup();
        ctx.bridge.start();

        const result = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand({
            commandId: 'cmd-missing',
            payload: { tagId: 'does-not-exist', value: 1, requestedBy: 'maint' }
        })));

        assert.equal(result.ok, false);
        assert.equal(result.tagId, 'does-not-exist');
        assert.equal(result.status, 'tag-not-found');
        assert.equal(result.error.code, 'tag-not-found');
        assert.equal(ctx.devices.state.writes.length, 0);

        await ctx.bridge.stop();
    });

    it('rejects tags that are not exposed by an active device', async function () {
        const ctx = setup({ devices: createFakeDevices({ active: false }) });
        ctx.bridge.start();

        const result = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand()));

        assert.equal(result.ok, false);
        assert.equal(result.status, 'tag-not-active');
        assert.equal(ctx.devices.state.writes.length, 0);

        await ctx.bridge.stop();
    });

    it('rejects values that are not valid for the tag type', async function () {
        const ctx = setup();
        ctx.bridge.start();

        const objectResult = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand({
            commandId: 'cmd-object',
            payload: { tagId: 'tag1', value: { nested: true }, requestedBy: 'maint' }
        })));
        assert.equal(objectResult.ok, false);
        assert.equal(objectResult.status, 'invalid-value');
        assert.equal(objectResult.requestedValue, null);

        const numberResult = await ctx.bridge.handleStreamEntry('2-0', toFields(buildCommand({
            commandId: 'cmd-number',
            payload: { tagId: 'tag1', value: 'not-a-number', requestedBy: 'maint' }
        })));
        assert.equal(numberResult.ok, false);
        assert.equal(numberResult.status, 'invalid-value');
        assert.equal(numberResult.error.code, 'invalid-value');

        assert.equal(ctx.devices.state.writes.length, 0);

        await ctx.bridge.stop();
    });

    it('enforces the optional tag allowlist', async function () {
        const ctx = setup({ options: { allowlist: ['bool1'] } });
        ctx.bridge.start();

        const result = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand()));

        assert.equal(result.ok, false);
        assert.equal(result.status, 'tag-not-allowed');
        assert.equal(result.error.code, 'tag-not-allowed');
        assert.equal(ctx.devices.state.writes.length, 0);

        await ctx.bridge.stop();
    });

    it('deduplicates commands by commandId (one write, cached result republished)', async function () {
        const ctx = setup();
        ctx.bridge.start();
        const command = buildCommand();

        const first = await ctx.bridge.handleStreamEntry('1-0', toFields(command));
        const second = await ctx.bridge.handleStreamEntry('2-0', toFields(command));

        assert.equal(first.ok, true);
        assert.deepEqual(second, first);
        assert.equal(ctx.devices.state.writes.length, 1, 'duplicate must not write twice');
        assert.equal(resultEvents(ctx.client).length, 2, 'cached result must be republished');
        assert.equal(ctx.client.xackCalls.length, 2, 'duplicate entry must be acked');
        assert.ok(ctx.logger.entries.info.some(function (line) {
            return line.indexOf('duplicate command cmd-1') !== -1;
        }));

        await ctx.bridge.stop();
    });

    it('reports a write rejection from the driver', async function () {
        const ctx = setup({ devices: createFakeDevices({ writeResult: false }) });
        ctx.bridge.start();

        const result = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand()));

        assert.equal(result.ok, false);
        assert.equal(result.status, 'write-failed');
        assert.equal(result.error.code, 'write-failed');
        assert.equal(result.value, null);
        assert.equal(ctx.client.xackCalls.length, 1, 'failed commands must still be acked');

        await ctx.bridge.stop();
    });

    it('reports a read-back mismatch without failing the accepted write', async function () {
        const ctx = setup({
            devices: createFakeDevices({ keepValue: true, values: { tag1: { value: 1 } } })
        });
        ctx.bridge.start();

        const result = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand()));

        assert.equal(result.ok, true);
        assert.equal(result.status, 'readback-mismatch');
        assert.equal(result.requestedValue, 42);
        assert.equal(result.value, 1);
        assert.equal(result.error, null);

        await ctx.bridge.stop();
    });

    it('never logs connection secrets when Redis is unreachable', async function () {
        const failing = createFakeClient({
            connect() {
                this.connectCalls++;
                return Promise.reject(new Error('connect failed redis://user:secret@redis.example:6379/0'));
            }
        });
        const ctx = setup({ client: failing });

        assert.equal(ctx.bridge.start(), true);
        await new Promise(function (resolve) { setImmediate(resolve); });

        const errors = ctx.logger.entries.error.join('\n');
        assert.ok(errors.indexOf('redis://***@redis.example:6379/0') !== -1, errors);
        assert.equal(errors.indexOf('secret'), -1, 'password must be redacted');

        await ctx.bridge.stop();
    });

    it('times out a write instead of blocking forever', async function () {
        const ctx = setup({
            devices: createFakeDevices({ writeNeverResolves: true }),
            options: { timeoutMs: 20 }
        });
        ctx.bridge.start();

        const startedAt = Date.now();
        const result = await ctx.bridge.handleStreamEntry('1-0', toFields(buildCommand()));
        const elapsed = Date.now() - startedAt;

        assert.equal(result.ok, false);
        assert.equal(result.status, 'timeout');
        assert.equal(result.error.code, 'timeout');
        assert.ok(elapsed >= 15, 'timeout must be observed, elapsed=' + elapsed);

        await ctx.bridge.stop();
    });

    it('answers an invalid JSON envelope without crashing', async function () {
        const ctx = setup();
        ctx.bridge.start();

        const result = await ctx.bridge.handleStreamEntry('1-0', ['event', '{not-json', 'type', 'tag.write']);

        assert.equal(result.commandId, null);
        assert.equal(result.ok, false);
        assert.equal(result.status, 'invalid-envelope');
        assert.equal(result.error.code, 'invalid-json');
        assert.equal(resultEvents(ctx.client).length, 1);
        assert.equal(ctx.client.xackCalls.length, 1);

        await ctx.bridge.stop();
    });

    it('creates the consumer group and consumes a command from the stream', async function () {
        const ctx = setup();
        assert.equal(ctx.bridge.start(), true);

        const command = buildCommand({
            commandId: 'cmd-loop',
            payload: { tagId: 'tag1', value: 7, requestedBy: 'maint-loop' }
        });
        ctx.client.inbox.push([
            ['stream:fuxa:commands', [['9-0', toFields(command)]]]
        ]);

        const event = await waitFor(function () {
            const events = resultEvents(ctx.client);
            return events.length ? events[0] : null;
        });

        assert.equal(event.type, 'command.result');
        assert.equal(event.payload.commandId, 'cmd-loop');
        assert.equal(event.payload.ok, true);
        assert.equal(event.payload.value, 7);
        assert.equal(ctx.client.xackCalls.length, 1);
        assert.deepEqual(ctx.client.xackCalls[0], ['stream:fuxa:commands', 'fuxa-command-executor', '9-0']);

        const groupCall = ctx.client.xgroupCalls[0];
        assert.equal(groupCall[0], 'CREATE');
        assert.equal(groupCall[1], 'stream:fuxa:commands');
        assert.equal(groupCall[2], 'fuxa-command-executor');
        assert.equal(groupCall[3], '$');
        assert.equal(groupCall[4], 'MKSTREAM');

        const readCall = ctx.client.xreadgroupCalls[0];
        assert.equal(readCall[0], 'GROUP');
        assert.equal(readCall[1], 'fuxa-command-executor');
        assert.equal(readCall[3], 'COUNT');
        assert.equal(readCall[4], 1);
        assert.equal(readCall[5], 'BLOCK');
        assert.equal(readCall[7], 'STREAMS');
        assert.equal(readCall[8], 'stream:fuxa:commands');
        assert.equal(readCall[9], '>');

        await ctx.bridge.stop();
    });
});

describe('runtime redis command bridge configuration', function () {
    it('is disabled with safe defaults', function () {
        const config = redisCommandBridge.resolveCommandConfig({}, {});
        assert.equal(config.enabled, false);
        assert.equal(config.commandStream, 'stream:fuxa:commands');
        assert.equal(config.group, 'fuxa-command-executor');
        assert.equal(config.url, 'redis://127.0.0.1:6379/0');
        assert.equal(config.resultStream, 'stream:fuxa:events');
        assert.equal(config.timeoutMs, 5000);
        assert.equal(config.blockMs, 5000);
        assert.deepEqual(config.allowlist, []);
    });

    it('reads command settings from the environment and reuses the Redis URL', function () {
        const config = redisCommandBridge.resolveCommandConfig({
            settings: { url: 'redis://user:secret@redis.example:6380/2', stream: 'custom:events' }
        }, {
            FUXA_REDIS_COMMAND_ENABLED: 'true',
            FUXA_REDIS_COMMAND_STREAM: 'custom:commands',
            FUXA_REDIS_COMMAND_GROUP: 'executor-x',
            FUXA_REDIS_COMMAND_CONSUMER: 'consumer-1',
            FUXA_REDIS_COMMAND_TIMEOUT_MS: '1500',
            FUXA_REDIS_COMMAND_TAG_ALLOWLIST: 'tag1, tag2'
        });

        assert.equal(config.enabled, true);
        assert.equal(config.commandStream, 'custom:commands');
        assert.equal(config.group, 'executor-x');
        assert.equal(config.consumer, 'consumer-1');
        assert.equal(config.timeoutMs, 1500);
        assert.deepEqual(config.allowlist, ['tag1', 'tag2']);
        assert.equal(config.url, 'redis://user:secret@redis.example:6380/2');
        assert.equal(config.resultStream, 'custom:events');
    });

    it('finds tags by map key or by tag id', function () {
        const devices = {
            dev1: { id: 'dev1', tags: { key1: { id: 'tag-1', type: 'Int' } } }
        };
        assert.equal(redisCommandBridge.findProjectTag(devices, 'key1').deviceId, 'dev1');
        assert.equal(redisCommandBridge.findProjectTag(devices, 'tag-1').tag.type, 'Int');
        assert.equal(redisCommandBridge.findProjectTag(devices, 'missing'), null);
    });
});
