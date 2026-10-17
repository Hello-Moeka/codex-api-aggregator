"""Synthetic capability failures must never poison plain-text routing."""
import asyncio
import contextlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time

import aiohttp
from aiohttp import web
from verify_routing import serve


async def main():
    checks = []

    def check(name, value):
        assert value, name
        checks.append(name)

    with tempfile.TemporaryDirectory() as folder:
        data = Path(folder)
        (data / 'secrets.json').write_text(json.dumps({'client': 'synthetic', 'management': 'synthetic'}))
        sources = [{'id': ident, 'name': ident, 'kind': 'api', 'enabled': True,
                    'models': [{'alias': 'test'}]} for ident in ('a', 'b')]
        (data / 'control-settings.json').write_text(json.dumps({'sources': sources, 'session_affinity': True}))
        os.environ['ROUTER_DATA'] = folder
        spec = importlib.util.spec_from_file_location('capability_router', Path(__file__).with_name('router.py'))
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        r = m.Router()
        hits = []
        mode = {'code': 'get_channel_failed', 'wire': 'http'}
        tool = {'type': 'function', 'name': 'private_tool_name', 'description': 'PRIVATE_DESCRIPTION',
                'parameters': {'type': 'object', 'properties': {'secret_arg': {'type': 'string'}}}}
        image = {'type': 'image_generation', 'output_format': 'png'}
        cap = m.request_capabilities({'tools': [tool]})
        check('ordinary text has no capability scope', m.request_capabilities({'input': 'hello'}) is None)
        check('empty tools do not create scope', m.request_capabilities({'tools': []}) is None)
        check('different tools isolated separately', cap != m.request_capabilities({'tools': [image]}))
        check('tool choice has independent scope', cap != m.request_capabilities({'tools': [tool], 'tool_choice': 'required'}))
        check('private tool metadata never appears in scope', 'PRIVATE' not in json.dumps(cap) and 'private_tool_name' not in json.dumps(cap))
        check('image input only records type', m.request_capabilities({'input': [{'type': 'input_image', 'image_url': 'PRIVATE_URL'}]})['features'] == ['input:input_image'])
        check('Responses Lite tools recognized', m.request_capabilities({'input': [{'type': 'additional_tools', 'tools': [tool]}]}) is not None)

        async def upstream(request):
            body = await request.json()
            ident = body['model'].split('/')[0]
            hits.append((ident, body))
            rejected = ident == 'a' and (body.get('tools') and mode.get('reject_tools', True) or mode.get('plain_reject'))
            if rejected:
                payload = {'error': {'code': mode['code'], 'type': 'new_api_error', 'message': 'PRIVATE_ERROR'}}
                if mode['wire'] == 'http':
                    return web.json_response(payload, status=500)
                if mode['wire'] == 'json':
                    return web.json_response({'status': 'failed', **payload})
                return web.Response(text='data: ' + json.dumps({'type': 'response.failed', 'response': payload}) + '\n\n',
                                    content_type='text/event-stream')
            result = {'status': 'completed', 'id': 'synthetic-response', 'output': []}
            if body.get('stream'):
                return web.Response(text='data: ' + json.dumps({'type': 'response.completed', 'response': result}) + '\n\n',
                                    content_type='text/event-stream')
            return web.json_response(result)

        app = web.Application()
        app.router.add_post('/v1/responses', upstream)
        native, m.GATEWAY = await serve(app)

        async def monitor():
            await asyncio.Event().wait()

        r.monitor = monitor
        runner, url = await serve(r.app())
        try:
            async with aiohttp.ClientSession(headers={'Authorization': 'Bearer synthetic'}) as client:
                async def call(**extra):
                    body = {'model': 'test', 'input': 'PRIVATE_PROMPT', **extra}
                    async with client.post(url + '/v1/responses', json=body) as response:
                        return response.status, await response.read()

                for wire in ('http', 'json', 'sse'):
                    for code in ('get_channel_failed', 'permission_denied', 'unsupported_tool_type', 'insufficient_quota'):
                        mode.update(wire=wire, code=code)
                        r.state.items.clear()
                        r.capabilities.items.clear()
                        r.capabilities.active.clear()
                        hits.clear()
                        status, _ = await call(tools=[tool], stream=wire == 'sse')
                        check(wire + '/' + code + ' falls back', status == 200 and [i for i, _ in hits] == ['a', 'b'])
                        check(wire + '/' + code + ' leaves model/source healthy', not r.state.get('a/test').get('reason') and not r.state.get('a').get('reason'))
                        check(wire + '/' + code + ' retains original tools on fallback', all(body['tools'] == [tool] for _, body in hits))
                        check(wire + '/' + code + ' records independent capability', len(r.capabilities.items) == 1)
                        hits.clear()
                        await call()
                        check(wire + '/' + code + ' plain text still uses first source', [i for i, _ in hits] == ['a'])
                        check(wire + '/' + code + ' text success cannot clear capability', len(r.capabilities.items) == 1)

                mode['wire'] = 'http'
                hits.clear()
                await call(tools=[tool])
                check('matching request observes cooldown', [i for i, _ in hits] == ['b'])
                hits.clear()
                async with client.post(url + '/v1/responses', json={'model': 'a/test', 'input': 'test', 'tools': [tool]}) as response:
                    await response.read()
                    check('directed cooldown returns capability error without an upstream attempt', response.status == 503 and not hits)
                    check('Retry-After follows capability cooldown', 1 <= int(response.headers['Retry-After']) <= 30)
                hits.clear()
                await call(tools=[image])
                check('different capability still attempts first source', [i for i, _ in hits] == ['a', 'b'])
                # A text-only recovery probe cannot change capability evidence.
                before = json.dumps(r.capabilities.items, sort_keys=True)
                status, _ = await call(model='a/test')
                check('directed text also remains available', status == 200)
                check('text verification preserves capability cooldowns', before == json.dumps(r.capabilities.items, sort_keys=True))
                # Retry uses the original capability, never a text-only substitute.
                for item in r.capabilities.items.values():
                    item['until'] = 0
                mode['plain_reject'] = False
                mode['reject_tools'] = False
                hits.clear()
                status, _ = await call(model='a/test', tools=[tool])
                check('expired cooldown retries original tool request', status == 200 and [i for i, _ in hits] == ['a'] and hits[0][1]['tools'] == [tool])
                check('matching success clears only matching capability', len(r.capabilities.items) == 1)
                permit, _ = r.capabilities.acquire('a', 'test', m.request_capabilities({'tools': [image]}))
                newer = m.Failure('model_unavailable')
                r.capabilities.failure('a', 'test', m.request_capabilities({'tools': [image]}), newer)
                r.capabilities.success(permit)
                r.capabilities.release(permit)
                check('old success cannot erase newer capability failure', len(r.capabilities.items) == 1)
                reloaded = m.CapabilityHealth(data)
                check('capability cooldown survives restart', len(reloaded.items) == 1)
                persisted = (data / 'capability-state.json').read_text()
                check('capability state excludes private request data', all(s not in persisted for s in ('PRIVATE', 'private_tool_name', 'secret_arg')))
                for item in reloaded.items.values():
                    item['last_client_request_at'] = time.time() - 3601
                check('idle capability faults expire', len(reloaded.expire()) == 1 and not reloaded.items)
                reloaded.limit = 2
                for name in ('a', 'b', 'c'):
                    reloaded.failure(name, 'test', cap, m.Failure('model_unavailable'))
                check('capability state has a bounded size', len(reloaded.items) == 2)

                r.capabilities.items.clear()
                r.state.items.clear()
                hits.clear()
                mode.update(code='get_channel_failed', wire='http', plain_reject=True, reject_tools=True)
                await call()
                check('plain text model failures still isolate model', r.state.get('a/test').get('reason') == 'model_unavailable')
                r.state.items.clear()
                mode.update(code='server_error', plain_reject=False)
                await call(tools=[tool])
                check('real upstream outages still affect source', r.state.get('a').get('reason') == 'upstream_error' and not r.capabilities.items)
                r.state.failure('a', 'test', m.Failure('capability_unavailable'))
                check('unidentified capability failure cannot trip source', r.state.get('a').get('consecutive') == 1)
                r.state.items.clear()
                hits.clear()
                mode['code'] = 'prompt_blocked'
                status, _ = await call(tools=[tool])
                check('tool-bearing policy failure still stops', status == 400 and [i for i, _ in hits] == ['a'] and not r.capabilities.items)
                with contextlib.closing(r.db()) as db:
                    traces = [json.loads(row[0]) for row in db.execute('SELECT trace FROM requests')]
                check('request logs explain capability scope', any(t.get('reason') == 'capability_unavailable' and t.get('original_reason') == 'model_unavailable' for trace in traces for t in trace))
        finally:
            await runner.cleanup()
            await native.cleanup()
    report = {'passed': True, 'count': len(checks), 'checks': checks, 'real_provider_calls': 0}
    Path('reports/capability-routing-verification.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report))


if __name__ == '__main__':
    asyncio.run(main())
