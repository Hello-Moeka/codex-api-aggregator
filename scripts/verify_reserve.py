"""Reserve threshold and routing acceptance against synthetic quota snapshots."""
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
    checks, hits, queries = [], [], []
    usage = {}
    with tempfile.TemporaryDirectory() as folder:
        data = Path(folder)
        def save(name, obj): (data / name).write_text(json.dumps(obj), encoding='utf-8')
        save('secrets.json', {'client': 'synthetic', 'management': 'synthetic-admin'})
        sources = [{'id': 'a', 'name': 'Plus A', 'kind': 'oauth', 'slot': 'A', 'enabled': True},
                   {'id': 'b', 'name': 'Relay B', 'kind': 'api', 'enabled': True, 'models': [{'alias': 'test', 'upstream': 'test'}]}]
        settings = {'sources': sources, 'session_affinity': True, 'official_reserve': {'enabled': True, 'percent': 10}}
        save('control-settings.json', settings); save('official-models.json', {'a': ['test']})
        (data / 'auths').mkdir(); save('auths/plus-A.json', {'access_token': 'synthetic-only', 'account_id': 'synthetic'})
        os.environ['ROUTER_DATA'] = folder
        spec = importlib.util.spec_from_file_location('reserve_router', Path(__file__).with_name('router.py'))
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        async def fake(request):
            body = await request.json(); hits.append(body['model'].split('/')[0])
            return web.json_response({'id': 'synthetic-response', 'status': 'completed', 'output': [], 'usage': {'input_tokens': 1, 'output_tokens': 1, 'total_tokens': 2}})
        app = web.Application(); app.router.add_post('/v1/responses', fake)
        upstream, module.GATEWAY = await serve(app)
        router = module.Router()
        async def no_monitor(): await asyncio.Event().wait()
        router.monitor = no_monitor
        runner, url = await serve(router.app())
        real = router.client
        class QuotaReply:
            def __init__(self): self.status = usage.get('http', 200)
            async def __aenter__(self): queries.append(time.monotonic()); await asyncio.sleep(.01); return self
            async def __aexit__(self, *_): pass
            async def json(self): return usage.get('raw', {})
        class Facade:
            def get(self, url, **kwargs):
                return QuotaReply() if url.startswith('https://chatgpt.com/') else real.get(url, **kwargs)
            def post(self, *args, **kwargs): return real.post(*args, **kwargs)
            async def close(self): await real.close()
        router.client = Facade()
        def snapshot(primary, secondary=20, *, reset=None):
            usage.clear(); usage['raw'] = {'rate_limit': {'allowed': max(primary, secondary) < 100, 'limit_reached': max(primary, secondary) >= 100,
                'primary_window': {'used_percent': primary, 'reset_at': reset or time.time()+18000, 'limit_window_seconds': 18000},
                'secondary_window': {'used_percent': secondary, 'reset_at': reset or time.time()+604800, 'limit_window_seconds': 604800}}}
        def reset(primary=20, secondary=20):
            hits.clear(); queries.clear(); router.state.items.clear(); router.state.history.clear(); router.sessions.clear()
            settings['official_reserve'] = {'enabled': True, 'percent': 10}; sources[0]['enabled'] = True
            save('control-settings.json', settings); snapshot(primary, secondary)
        def passed(name): checks.append(name); print('PASS', name, flush=True)
        async with aiohttp.ClientSession(headers={'Authorization': 'Bearer synthetic'}) as client:
            async def call(model='test', session=None):
                async with client.post(url+'/v1/responses', json={'model': model, 'input': 'test', 'stream': False}, headers={'session_id': session} if session else {}) as response:
                    return response.status, await response.json()
            try:
                reset(89); assert (await call())[0] == 200 and hits == ['a']; passed('11 percent remaining uses official source')
                reset(90); assert (await call())[0] == 200 and hits == ['b']; assert router.state.get('a')['reserve_blocked']; passed('exactly 10 percent remaining skips official')
                reset(91); await call(); assert hits == ['b']; passed('below reserve skips official')
                reset(20,90); await call(); assert hits == ['b'] and router.state.get('a')['reserve_windows'] == ['secondary_window']; passed('weekly reserve independently blocks source')
                reset(90,90); await call(); snapshot(5,90); router.state.get('a')['quota_attempt_at'] = 0; hits.clear(); await call(); assert hits == ['b']; passed('short reset cannot clear weekly reserve')
                snapshot(5,5); router.state.get('a')['quota_attempt_at'] = 0; hits.clear(); await call(); assert hits == ['a'] and not router.state.get('a')['reserve_blocked']; passed('early reset above both limits restores official')
                reset(89); await call(session='affinity'); snapshot(90); router.state.get('a')['quota_attempt_at'] = 0; hits.clear(); await call(session='affinity'); assert hits == ['b']; passed('reserve overrides existing official session affinity')
                reset(90); status,_ = await call('a/test'); assert status == 503 and not hits; passed('explicit source cannot bypass reserve')
                reset(20); usage.clear(); usage['http']=503; await call(); assert hits == ['b']; passed('failed quota query fails closed')
                reset(90); await call(); usage.clear(); usage['raw']={'unexpected':True}; router.state.get('a')['quota_attempt_at']=0; hits.clear(); await call(); assert hits == ['b'] and router.state.get('a')['reserve_blocked']; passed('unknown quota preserves existing reserve hold')
                reset(20); usage['raw']['rate_limit']['primary_window']['used_percent'] = '90'; await call(); assert hits == ['b']; passed('malformed percentage cannot authorize spending')
                reset(90); await call(); settings['official_reserve']['enabled']=False; save('control-settings.json',settings); hits.clear(); await call(); assert hits == ['a']; passed('disabling reserve immediately permits nonexhausted official')
                reset(80); settings['official_reserve']['percent']=20; save('control-settings.json',settings); await call(); assert hits == ['b']; passed('custom percentage applied at exact threshold')
                reset(100); await call(); settings['official_reserve']['enabled']=False; save('control-settings.json',settings); hits.clear(); await call(); assert hits == ['b'] and router.state.get('a')['state']=='quota_wait'; passed('disabling reserve never clears actual quota exhaustion')
                reset(20); sources[0]['enabled']=False; save('control-settings.json',settings); await call(); assert not queries and hits == ['b']; passed('manual pause remains untouched and avoids official checks')
                reset(20); await asyncio.gather(*(call() for _ in range(6))); assert len(queries)==1 and hits==['a']*6; passed('concurrent requests share one quota read')
                snapshot(90); hits.clear(); await call(); assert hits==['a'] and len(queries)==1; router.state.get('a')['quota_attempt_at']=time.time()-16; hits.clear(); await call(); assert hits==['b'] and len(queries)==2; passed('request quota cache expires after 15 seconds')
                router.state.persist(); restarted=module.State(data); assert restarted.get('a')['reserve_blocked'] and restarted.acquire('a','test')[1]=='reserve_wait'; passed('reserve survives restart')
                reset(20); router.state.items['a']={'state':'open','reason':'connection_failed','until':time.time()+100,'consecutive':3}; await router.refresh_official(sources[0]); assert router.state.get('a')['state']=='open'; passed('positive quota cannot clear circuit breaker')
                with __import__('contextlib').closing(router.db()) as db:
                    trace=[json.loads(row[0]) for row in db.execute('SELECT trace FROM requests')]
                assert any(any(t['reason']=='reserve_wait' and t['action']=='skip' for t in ts) for ts in trace); passed('request log explains reserve skip')
                snapshot(90,20,reset=time.time()-1); router.state.items.clear(); await router.refresh_official(sources[0]); assert router.state.get('a')['reserve_blocked']; passed('past reset timestamp alone never releases reserve')
            finally:
                await runner.cleanup(); await upstream.cleanup()
        report={'passed':True,'checks':checks,'count':len(checks),'real_provider_calls':0}
        Path(os.environ.get('RESERVE_REPORT','/tmp/reserve-verification.json')).write_text(json.dumps(report,indent=2),encoding='utf-8')
        print(json.dumps(report))


if __name__=='__main__': asyncio.run(main())
