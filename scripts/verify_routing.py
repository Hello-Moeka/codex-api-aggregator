"""Isolated, deterministic faults. No production credentials or provider calls."""
import asyncio
import contextlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
from unittest.mock import patch

import aiohttp
from aiohttp import web


async def serve(app):
    runner = web.AppRunner(app, handler_cancellation=True)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    return runner, 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])


async def main():
    results, hits, modes, cancelled = [], [], {}, asyncio.Event()
    with tempfile.TemporaryDirectory() as folder:
        data = Path(folder)
        def save(name, obj): (data / name).write_text(json.dumps(obj), encoding='utf-8')
        save('secrets.json', {'client': 'synthetic-client', 'management': 'synthetic-admin'})
        sources = [{'id': x, 'name': x, 'kind': 'api', 'enabled': True, 'models': [{'alias': 'test', 'upstream': 'test'}, {'alias': 'other', 'upstream': 'other'}]} for x in ('a', 'b', 'c')]
        save('control-settings.json', {'sources': sources, 'session_affinity': True})
        os.environ['ROUTER_DATA'] = folder
        spec = importlib.util.spec_from_file_location('routing_under_test', Path(__file__).with_name('router.py'))
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        async def upstream(request):
            body = await request.json(); ident = body['model'].split('/')[0]
            hits.append(ident); mode = modes.get(ident, 'ok')
            if mode == '503': return web.json_response({'error': {'code': 'server_error'}}, status=503)
            if mode == 'model': return web.json_response({'error': {'code': 'get_channel_failed'}}, status=500)
            if mode == 'bad': return web.json_response({'error': {'code': 'invalid_request_error'}}, status=400)
            if mode == 'rate': return web.json_response({'error': {'code': 'rate_limit'}}, status=429, headers={'Retry-After': '7'})
            if mode == 'quota': return web.json_response({'error': {'code': 'usage_limit_reached', 'resets_at': time.time() + 900}}, status=429)
            if mode == 'html': return web.Response(text='<html>oops</html>', content_type='text/html')
            if mode == 'hang':
                try: await asyncio.sleep(20)
                except asyncio.CancelledError: cancelled.set(); raise
            if not body.get('stream'):
                return web.json_response({'id': 'resp_test', 'status': 'completed', 'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'OK'}]}], 'usage': {'input_tokens': 2, 'output_tokens': 1, 'total_tokens': 3}})
            response = web.StreamResponse(headers={'Content-Type': 'text/event-stream'})
            await response.prepare(request)
            async def emit(obj): await response.write(('data: ' + json.dumps(obj) + '\n\n').encode())
            if mode == 'heartbeat':
                try:
                    while True:
                        await response.write(b': heartbeat\n\n'); await asyncio.sleep(.02)
                except (ConnectionError, asyncio.CancelledError): cancelled.set(); return response
            await emit({'type': 'response.created', 'response': {'id': 'resp_test', 'status': 'in_progress'}})
            if mode == 'created_only':
                try:
                    while True:
                        await response.write(b': heartbeat\n\n'); await asyncio.sleep(.02)
                except (ConnectionError, asyncio.CancelledError): return response
            if mode == 'tool_break':
                await emit({'type': 'response.output_item.added', 'item': {'type': 'function_call', 'name': 'test', 'call_id': 'call_test'}})
                return response
            await emit({'type': 'response.output_text.delta', 'delta': 'OK'})
            if mode == 'idle':
                try: await asyncio.sleep(20)
                except asyncio.CancelledError: cancelled.set(); raise
            if mode == 'long_progress':
                for _ in range(12):
                    await asyncio.sleep(.05)
                    await emit({'type': 'response.reasoning_text.delta', 'delta': 'thinking'})
            await emit({'type': 'response.completed', 'response': {'id': 'resp_test', 'status': 'completed', 'usage': {'input_tokens': 2, 'output_tokens': 1, 'total_tokens': 3}}})
            return response
        fake = web.Application(); fake.router.add_post('/v1/responses', upstream)
        native, native_url = await serve(fake); module.GATEWAY = native_url
        router = module.Router(); router.clock_scale = .003
        # No background probes in isolated fault scenarios.
        async def monitor(): await asyncio.Event().wait()
        router.monitor = monitor
        gateway, url = await serve(router.app())
        async with aiohttp.ClientSession() as client:
            async def call(model='test', **extra):
                async with client.post(url + '/v1/responses', json={'model': model, 'input': 'test', 'stream': True, **extra}, headers={'Authorization': 'Bearer synthetic-client'}) as response:
                    return response.status, await response.read()
            def reset(**values):
                modes.clear(); modes.update(values); hits.clear(); router.state.items.clear(); router.state.history.clear(); router.state.active.clear(); cancelled.clear(); router.sessions.clear(); router.responses.clear()
            def passed(name): results.append(name); print('PASS', name, flush=True)
            try:
                reset(a='503', b='model'); status, raw = await call()
                assert hits == ['a', 'b', 'c'] and status == 200 and b'response.completed' in raw
                passed('ordered fallback and per-request one attempt')
                assert router.state.get('b/test')['state'] == 'open' and router.state.acquire('b', 'other')[0] is not None
                passed('model failures do not disable sibling models')
                reset(a='bad'); assert (await call())[0] == 400 and hits == ['a']; passed('invalid request stops without provider sweep')
                reset(a='503'); assert (await call('a/test'))[0] == 503 and hits == ['a']; passed('explicit source never crosses lanes')
                reset(a='rate'); await call(); assert 5 < router.state.get('a')['until'] - time.time() <= 7; passed('Retry-After respected')
                reset(a='html'); assert (await call())[0] == 200 and hits == ['a', 'b']; passed('HTTP 200 HTML rejected before output')
                reset(a='heartbeat'); t = time.monotonic(); assert (await call())[0] == 200 and hits == ['a', 'b'] and time.monotonic() - t < 1; passed('infinite heartbeats cannot reset first event deadline')
                reset(a='created_only'); t = time.monotonic(); status, _ = await call(); assert status == 504 and hits == ['a'] and time.monotonic() - t < 1; passed('valid handshake without progress and chain budget bounded')
                reset(a='long_progress'); assert b'response.completed' in (await call())[1] and hits == ['a']; passed('ongoing reasoning survives preoutput chain budget')
                reset(a='tool_break'); status, raw = await call(); assert hits == ['a'] and b'stream_interrupted' in raw; passed('no replay after tool call exposed')
                reset(a='idle'); status, raw = await call(); assert hits == ['a'] and b'stream_idle_timeout' in raw; passed('midstream idle fails without replay')
                reset(a='hang'); task = asyncio.create_task(call()); await asyncio.sleep(.03); task.cancel()
                with contextlib.suppress(asyncio.CancelledError): await task
                await asyncio.wait_for(cancelled.wait(), 1); assert hits == ['a']; passed('client cancellation cancels upstream and fallback')
                reset(a='503');
                for _ in range(3): await call()
                assert router.state.get('a')['state'] == 'open'
                hits.clear(); await call(); assert hits == ['b']; passed('three failures trip and open breaker skipped')
                router.state.items['a']['until'] = 0
                permit, _ = router.state.acquire('a', 'test'); assert permit is not None
                assert router.state.acquire('a', 'test')[0] is None
                router.state.success('a', 'test', permit); router.state.release(permit)
                assert router.state.get('a')['state'] == 'half_open'
                router.state.items['a']['until'] = 0; permit, _ = router.state.acquire('a', 'test')
                router.state.success('a', 'test', permit); router.state.release(permit)
                assert router.state.get('a')['state'] == 'closed'; passed('single half-open permit and two successes restore')
                router.state.failure('a', 'test', module.Failure('auth_required', 401)); router.state.persist()
                state2 = module.State(data); assert state2.get('a')['state'] == 'auth_required'; passed('restart preserves cooldown and auth state')
                reset(); await call(); modes['a'] = '503'; hits.clear()
                status, _ = await call(previous_response_id='resp_test'); assert status == 503 and hits == ['a']; passed('response id continuation pinned to original account')
                hits.clear(); assert (await call(previous_response_id='unknown'))[0] == 409 and not hits; passed('unknown continuation requires full history')
                reset(); status, raw = await call(stream=False); assert status == 200 and json.loads(raw)['status'] == 'completed'; passed('nonstream completed JSON validated')
                official = {'id': 'plus-a', 'slot': 'A', 'kind': 'oauth', 'enabled': True}
                save('auths-placeholder.json', {})
                (data / 'auths').mkdir(exist_ok=True); save('auths/plus-A.json', {'access_token': 'synthetic', 'account_id': 'synthetic'})
                class QuotaReply:
                    status = 200
                    def __init__(self, obj): self.obj = obj
                    async def __aenter__(self): return self
                    async def __aexit__(self, *_): pass
                    async def json(self): return self.obj
                class QuotaClient:
                    def __init__(self, obj): self.obj = obj
                    def get(self, *args, **kwargs): return QuotaReply(self.obj)
                real_client = router.client
                try:
                    router.state.items['plus-a'] = {'state': 'quota_wait'}
                    router.client = QuotaClient({'rate_limit': {'allowed': True, 'limit_reached': False, 'primary_window': {'used_percent': 5, 'reset_at': time.time() + 100}}})
                    result = await router.quota(official); assert result['state'] == 'closed'; passed('early quota reset immediately recognized')
                    router.client = QuotaClient({'rate_limit': {'allowed': False, 'primary_window': {'used_percent': 0}, 'secondary_window': {'used_percent': 100, 'reset_at': time.time() + 86400}}})
                    result = await router.quota(official); assert result['state'] == 'quota_wait' and result['reset_at'] > time.time() + 80000; passed('weekly block survives short window reset')
                    router.client = QuotaClient({'unexpected': True}); result = await router.quota(official); assert 'state' not in result and result['query_error']; passed('unknown quota query never unblocks')
                finally: router.client = real_client
                db = router.db()
                records = db.execute('SELECT outcome,trace FROM requests').fetchall(); db.close()
                assert any(r[0] == 'stream_interrupted' for r in records) and any(r[0] == 'client_cancelled' for r in records)
                binary = (data / 'routing-requests.sqlite').read_bytes()
                assert b'synthetic-client' not in binary and b'"input":' not in binary; passed('logs include outcomes and safe trace without credentials or prompts')
            finally:
                await gateway.cleanup(); await native.cleanup()
        report = {'passed': True, 'count': len(results), 'checks': results, 'real_provider_calls': 0}
        output = Path(os.environ.get('ROUTING_REPORT', '/tmp/routing-verification.json'))
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__': asyncio.run(main())
