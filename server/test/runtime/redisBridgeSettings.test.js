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
        pingCalls: 0,
        quitCalls: 0,
        disconnectCalls: 0,
        xaddCalls: [],
        listeners: {},
        connect() { this.connectCalls++; this.status = 'ready'; return Promise.resolve(); },
        ping() { this.pingCalls++; return Promise.resolve('PONG'); },
        xadd() { this.xaddCalls.push(Array.from(arguments)); return Promise.resolve('1-0'); },
        on(name, listener) { this.listeners[name] = listener; },
        quit() { this.quitCalls++; return Promise.resolve('OK'); },
        disconnect() { this.disconnectCalls++; }
    };
    return Object.assign(client, overrides || {});
}

describe('runtime redis bridge settings', function () {
    describe('resolveConfig', function () {
        it('prefers persisted settings over environment and defaults', function () {
            const config = redisBridge.resolveConfig({
                enabled: true,
                host: 'redis.example.com',
                port: 6380,
                username: 'fuxa',
                password: 'p@ss word',
                tls: true,
                db: 2,
                stream: 'stream:custom',
                maxlen: 5000
            }, {
                FUXA_REDIS_ENABLED: 'false',
                FUXA_REDIS_URL: 'redis://env-host:6379/9',
                FUXA_REDIS_STREAM: 'stream:env',
                FUXA_REDIS_MAXLEN: '10'
            });

            assert.equal(config.enabled, true);
            assert.equal(config.url, 'rediss://fuxa:p%40ss%20word@redis.example.com:6380/2');
            assert.equal(config.stream, 'stream:custom');
            assert.equal(config.maxlen, 5000);
        });

        it('prefers a persisted explicit disable flag over the environment', function () {
            const config = redisBridge.resolveConfig({ enabled: false }, { FUXA_REDIS_ENABLED: 'true' });
            assert.equal(config.enabled, false);
        });

        it('falls back to environment values for empty persisted fields', function () {
            const config = redisBridge.resolveConfig({ host: '', port: '', stream: '', maxlen: '' }, {
                FUXA_REDIS_ENABLED: 'true',
                FUXA_REDIS_URL: 'redis://env-host:6380/3',
                FUXA_REDIS_STREAM: 'stream:env',
                FUXA_REDIS_MAXLEN: '1234'
            });

            assert.equal(config.enabled, true);
            assert.equal(config.url, 'redis://env-host:6380/3');
            assert.equal(config.stream, 'stream:env');
            assert.equal(config.maxlen, 1234);
        });

        it('falls back to safe defaults without settings or environment', function () {
            const config = redisBridge.resolveConfig(undefined, {});
            assert.equal(config.enabled, false);
            assert.equal(config.url, redisBridge.DEFAULT_URL);
            assert.equal(config.stream, redisBridge.DEFAULT_STREAM);
            assert.equal(config.maxlen, undefined);
        });

        it('accepts a full persisted url when no host is given', function () {
            const config = redisBridge.resolveConfig({ url: 'rediss://user:secret@redis.internal:6380/1' },
                { FUXA_REDIS_URL: 'redis://env-host:6379/0' });
            assert.equal(config.url, 'rediss://user:secret@redis.internal:6380/1');
        });
    });

    describe('persisted settings in the bridge', function () {
        it('uses persisted connection details and stream', function () {
            const events = new EventEmitter();
            const captured = [];
            const client = createFakeClient();
            const bridge = redisBridge.create({
                events: events,
                logger: createLogger(),
                settings: {
                    enabled: true,
                    host: 'redis.internal',
                    port: 6380,
                    stream: 'stream:test',
                    maxlen: 100
                },
                env: { FUXA_REDIS_URL: 'redis://env-host:6379/0' },
                createClient: function (config) { captured.push(config); return client; }
            });

            assert.equal(bridge.start(), true);
            assert.equal(captured.length, 1);
            assert.equal(captured[0].url, 'redis://redis.internal:6380/0');
            assert.equal(captured[0].stream, 'stream:test');

            events.emit('device-status:changed', { id: 'dev1', status: 'connect-ok' });
            assert.equal(client.xaddCalls[0][0], 'stream:test');
            assert.deepEqual(client.xaddCalls[0].slice(1, 5), ['MAXLEN', '~', 100, '*']);
        });

        it('uses the environment url when persisted settings have no host', function () {
            const captured = [];
            const bridge = redisBridge.create({
                events: new EventEmitter(),
                logger: createLogger(),
                settings: { enabled: true, stream: 'stream:test' },
                env: { FUXA_REDIS_URL: 'redis://env-host:6380/4' },
                createClient: function (config) { captured.push(config); return createFakeClient(); }
            });

            assert.equal(bridge.start(), true);
            assert.equal(captured[0].url, 'redis://env-host:6380/4');
        });

        it('resolves settings at start time so a reload picks up changes', async function () {
            const captured = [];
            let current = { enabled: true, host: 'first.internal' };
            const bridge = redisBridge.create({
                events: new EventEmitter(),
                logger: createLogger(),
                settings: function () { return current; },
                env: {},
                createClient: function (config) { captured.push(config); return createFakeClient(); }
            });

            bridge.start();
            await bridge.stop();
            current = { enabled: true, host: 'second.internal' };
            bridge.start();

            assert.equal(captured.length, 2);
            assert.equal(captured[0].url, 'redis://first.internal:6379/0');
            assert.equal(captured[1].url, 'redis://second.internal:6379/0');
        });
    });

    describe('settings sanitisation and validation', function () {
        it('never exposes the password and reports hasPassword', function () {
            const publicSettings = redisBridge.toPublicSettings({
                enabled: true,
                host: 'redis.internal',
                username: 'fuxa',
                password: 'super-secret',
                stream: 'stream:fuxa:events',
                hasPassword: true
            });

            assert.equal(publicSettings.password, undefined);
            assert.equal(publicSettings.hasPassword, true);
            assert.equal(JSON.stringify(publicSettings).includes('super-secret'), false);
        });

        it('never round-trips a url that embeds credentials', function () {
            const publicSettings = redisBridge.toPublicSettings({
                url: 'rediss://user:url-secret@redis.internal:6380/0'
            });
            assert.equal(publicSettings.hasPassword, true);
            assert.equal(publicSettings.hasUrl, true);
            assert.equal(publicSettings.url, undefined);
            assert.equal(JSON.stringify(publicSettings).includes('url-secret'), false);

            const merged = redisBridge.mergeSettings(
                { url: 'rediss://user:url-secret@redis.internal:6380/0' },
                { url: '', host: '' });
            assert.equal(merged.url, 'rediss://user:url-secret@redis.internal:6380/0');
        });

        it('preserves the stored password when the incoming password is empty', function () {
            const merged = redisBridge.mergeSettings(
                { host: 'redis.internal', password: 'stored-secret' },
                { host: 'redis.internal', password: '' });
            assert.equal(merged.password, 'stored-secret');
        });

        it('replaces the stored password when a new one is provided', function () {
            const merged = redisBridge.mergeSettings(
                { password: 'stored-secret' },
                { password: 'new-secret' });
            assert.equal(merged.password, 'new-secret');
        });

        it('accepts valid settings', function () {
            assert.deepEqual(redisBridge.validateSettings({
                enabled: true,
                host: 'redis.internal',
                port: 6379,
                db: 0,
                stream: 'stream:fuxa:events',
                maxlen: 100000
            }), []);
        });

        it('rejects invalid ports, database, max length, host and url', function () {
            const errors = redisBridge.validateSettings({
                host: 'redis://user:pass@redis.internal',
                port: 70000,
                db: -1,
                maxlen: 'many',
                url: 'http://redis.internal'
            });
            assert.ok(errors.some(error => error.includes('port')));
            assert.ok(errors.some(error => error.includes('database')));
            assert.ok(errors.some(error => error.includes('max length')));
            assert.ok(errors.some(error => error.includes('host')));
            assert.ok(errors.some(error => error.includes('url')));
        });
    });

    describe('testConnection', function () {
        it('pings the server and reports latency', async function () {
            const client = createFakeClient();
            const result = await redisBridge.testConnection({ url: 'redis://redis.internal:6379/0' }, {
                timeoutMs: 1000,
                createClient: function () { return client; }
            });

            assert.equal(result.ok, true);
            assert.equal(typeof result.latencyMs, 'number');
            assert.equal(client.connectCalls, 1);
            assert.equal(client.pingCalls, 1);
            assert.equal(client.disconnectCalls, 1);
        });

        it('reports a connection failure without throwing or leaking the password', async function () {
            const client = createFakeClient({
                connect: function () { return Promise.reject(new Error('connection refused')); }
            });
            const result = await redisBridge.testConnection({
                url: 'redis://fuxa:top-secret@redis.internal:6379/0',
                password: 'top-secret'
            }, {
                timeoutMs: 1000,
                createClient: function () { return client; }
            });

            assert.equal(result.ok, false);
            assert.ok(result.error.includes('connection refused'));
            assert.equal(result.error.includes('top-secret'), false);
            assert.equal(client.disconnectCalls, 1);
        });

        it('reports a ping failure', async function () {
            const client = createFakeClient({
                ping: function () { return Promise.reject(new Error('NOAUTH Authentication required')); }
            });
            const result = await redisBridge.testConnection({ url: 'redis://redis.internal:6379/0' }, {
                timeoutMs: 1000,
                createClient: function () { return client; }
            });

            assert.equal(result.ok, false);
            assert.equal(result.error, 'NOAUTH Authentication required');
            assert.equal(client.disconnectCalls, 1);
        });

        it('reports a client construction failure without throwing', async function () {
            const result = await redisBridge.testConnection({ url: 'redis://redis.internal:6379/0' }, {
                timeoutMs: 1000,
                createClient: function () { throw new Error('invalid url'); }
            });

            assert.equal(result.ok, false);
            assert.equal(result.error, 'invalid url');
        });

        it('removes a secret echoed by the server error', function () {
            const error = redisBridge.sanitizeError(
                new Error('WRONGPASS invalid username-password pair: top-secret'), 'top-secret');
            assert.equal(error.includes('top-secret'), false);
        });
    });
});
