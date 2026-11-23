'use strict';

/**
 * POST /api/iothttp/test
 *
 * Endpoint usado pelo botão "Testar leitura" do editor de tags IoTHTTP.
 * Aqui validamos o contrato HTTP (autorização, sucesso, falha do dispositivo
 * e erro inesperado) sem tocar em nenhum dispositivo real: o cliente HTTP
 * de saída (axios) é stubado.
 */

const http = require('http');
const express = require('express');
const sinon = require('sinon');
const axios = require('axios');

const projectsApi = require('../../api/projects');
const iothttp = require('../../runtime/devices/iothttp');

const ADMIN_PERMISSION = -1;
const NON_ADMIN_PERMISSION = 5;

let expect;

function postJson(server, path, payload) {
    return new Promise((resolve, reject) => {
        const body = payload === undefined ? '' : JSON.stringify(payload);
        const req = http.request({
            host: '127.0.0.1',
            port: server.address().port,
            method: 'POST',
            path,
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(body)
            }
        }, (res) => {
            let responseBody = '';
            res.setEncoding('utf8');
            res.on('data', chunk => { responseBody += chunk; });
            res.on('end', () => resolve({
                statusCode: res.statusCode,
                body: responseBody ? JSON.parse(responseBody) : null
            }));
        });
        req.on('error', reject);
        if (body) {
            req.write(body);
        }
        req.end();
    });
}

describe('IoT HTTP test API - POST /api/iothttp/test', () => {
    let server;
    let axiosRequest;

    before(async () => {
        const chai = await import('chai');
        expect = chai.expect;
    });

    async function startServer(permission) {
        const runtime = {
            settings: {},
            project: {},
            logger: { error() {} }
        };
        projectsApi.init(runtime, (req, res, next) => next(), () => permission);

        const app = express();
        app.use(express.json({ limit: '1mb' }));
        app.use(projectsApi.app());

        server = await new Promise(resolve => {
            const listening = app.listen(0, '127.0.0.1', () => resolve(listening));
        });
    }

    beforeEach(async () => {
        axiosRequest = sinon.stub(axios, 'request');
        await startServer(ADMIN_PERMISSION);
    });

    afterEach(done => {
        sinon.restore();
        server.close(done);
    });

    it('rejects callers without admin permission before touching the device', async () => {
        await new Promise(resolve => server.close(resolve));
        await startServer(NON_ADMIN_PERMISSION);

        const response = await postJson(server, '/api/iothttp/test', {
            property: { address: 'http://device.local' },
            tag: { address: '/status' }
        });

        expect(response.statusCode).to.equal(401);
        expect(response.body).to.deep.equal({ error: 'unauthorized_error' });
        expect(axiosRequest.called).to.equal(false);
    });

    it('performs one read and returns the extracted value', async () => {
        axiosRequest.resolves({
            status: 200,
            headers: {},
            data: { status: { temperature: 21.5, online: true } }
        });

        const response = await postJson(server, '/api/iothttp/test', {
            property: { address: 'http://device.local' },
            tag: { address: '/status', memaddress: '$.status.temperature' }
        });

        expect(response.statusCode).to.equal(200);
        expect(response.body).to.deep.equal({
            ok: true,
            status: 200,
            value: 21.5,
            data: { status: { temperature: 21.5, online: true } }
        });
        expect(axiosRequest.calledOnce).to.equal(true);
        const config = axiosRequest.firstCall.args[0];
        expect(config.method).to.equal('GET');
        expect(config.url).to.equal('http://device.local/status');
    });

    it('reports an unreachable device as ok:false instead of an HTTP error', async () => {
        const deviceError = new Error('connect ECONNREFUSED 127.0.0.1:9');
        deviceError.response = { status: 503, data: { message: 'offline' } };
        axiosRequest.rejects(deviceError);

        const response = await postJson(server, '/api/iothttp/test', {
            property: { address: 'http://device.local' },
            tag: { address: '/status' }
        });

        expect(response.statusCode).to.equal(200);
        expect(response.body).to.deep.equal({
            ok: false,
            status: 503,
            error: 'connect ECONNREFUSED 127.0.0.1:9',
            data: { message: 'offline' }
        });
    });

    it('defaults to an empty request when the body is missing', async () => {
        const deviceError = new Error('Invalid URL');
        axiosRequest.rejects(deviceError);

        const response = await postJson(server, '/api/iothttp/test');

        expect(response.statusCode).to.equal(200);
        expect(response.body.ok).to.equal(false);
        expect(response.body.error).to.equal('Invalid URL');
        expect(axiosRequest.calledOnce).to.equal(true);
    });

    it('returns 500 when the driver throws unexpectedly', async () => {
        sinon.stub(iothttp, 'testRequest').rejects(new Error('boom'));

        const response = await postJson(server, '/api/iothttp/test', {
            property: { address: 'http://device.local' },
            tag: { address: '/status' }
        });

        expect(response.statusCode).to.equal(500);
        expect(response.body).to.deep.equal({ ok: false, error: 'boom' });
        expect(axiosRequest.called).to.equal(false);
    });
});
