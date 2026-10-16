"""Synthetic probe lifecycle regression; never uses production credentials."""
import asyncio
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
    with tempfile.TemporaryDirectory() as folder:
        data = Path(folder)
        def save(name, value): (data / name).write_text(json.dumps(value), encoding='utf-8')
        save('secrets.json', {'client': 'synthetic-client', 'management': 'synthetic-management'})
        save('control-settings.json', {'sources': [{'id': 'a', 'name': 'Synthetic', 'kind': 'api', 'enabled': True}]})
        save('routing-state.json', {'a': {'state': 'closed', 'consecutive': 1, 'until': time.time()-100,
                                       'probe_model': 'test', 'level': 1, 'probe_in_progress': True}})
        os.environ['ROUTER_DATA'] = folder
        spec = importlib.util.spec_from_file_location('timer_router', Path(__file__).with_name('router.py'))
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        router = module.Router()
        assert router.state.get('a')['state'] == 'open' and not router.state.get('a')['probe_in_progress']
        checks.append('restart repairs stale closed cooldown and clears interrupted probe flag')
        mode, started, release, hits = ['fail'], asyncio.Event(), asyncio.Event(), []
        async def native_response(request):
            hits.append(await request.json()); started.set(); await release.wait()
            if mode[0] == 'fail': return web.json_response({'error': {'message': 'synthetic outage'}}, status=503)
            return web.Response(text='data: {"type":"response.completed","response":{"status":"completed"}}\n\n', content_type='text/event-stream')
        native_app = web.Application(); native_app.router.add_post('/v1/responses', native_response)
        native_runner, module.GATEWAY = await serve(native_app)
        probe_app = web.Application(); probe_app.router.add_post('/internal/probe', router.internal)
        probe_runner, probe_url = await serve(probe_app)
        async with aiohttp.ClientSession() as session:
            class ProbeClient:
                def post(self, url, **kwargs):
                    return session.post(url.replace('http://127.0.0.1:18319', probe_url), **kwargs)
            router.client = ProbeClient()
            try:
                task = asyncio.create_task(router.check('a'))
                await asyncio.wait_for(started.wait(), 2)
                assert router.state.get('a')['probe_in_progress']
                checks.append('due recovery publishes in-progress status while network request is running')
                release.set(); await task
                item = router.state.get('a')
                assert item['state'] == 'open' and item['until'] > time.time() and not item['probe_in_progress']
                assert item['probe_finished_at'] >= item['probe_started_at']
                assert item['last_probe']['passed'] is False and item['last_probe']['model'] == 'test'
                assert item['last_probe']['reason'] == 'upstream_error'
                checks.append('failed recovery stays open and schedules a new future cooldown')
                count = len(hits); await router.check('a'); assert len(hits) == count
                checks.append('future cooldown does not send an early probe')
                mode[0] = 'success'; item['until'] = 0; await router.check('a')
                assert item['state'] == 'half_open'
                assert item['last_probe']['passed'] is True and item['last_probe']['reason'] == 'success'
                item['until'] = 0; await router.check('a'); assert item['state'] == 'closed' and item['until'] == 0
                checks.append('two successful recovery probes close the breaker and clear cooldown')
                count=len(hits)
                await router.check('a'); assert len(hits)==count
                item['reclaim_after']=time.time()-1
                await router.check('a')
                assert len(hits)==count+1 and item['reclaim_verified']
                checks.append('stable observation waits then verifies with an extra real probe')
                item.update(state='closed', consecutive=1, reason='upstream_error', probe_model='test')
                count = len(hits); await router.check('a', manual=True)
                assert len(hits) == count + 1
                checks.append('manual recovery also probes a non-tripped recent failure')
                router.state.items['b'] = {'state': 'closed', 'until': 123, 'consecutive': 0}
                router.state.success('b', 'test'); assert router.state.get('b')['until'] == 0
                checks.append('ordinary successful requests clear leftover cooldown metadata')
            finally:
                await probe_runner.cleanup(); await native_runner.cleanup()
    report = {'passed': True, 'count': len(checks), 'checks': checks, 'real_provider_calls': 0}
    Path('reports/recovery-timer-verification.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report))


if __name__ == '__main__': asyncio.run(main())
