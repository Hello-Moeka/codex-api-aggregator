"""Structured error regression against local fake upstreams only."""
import asyncio
import contextlib
import importlib.util
import json
import os
import tempfile
import time
from pathlib import Path
from verify_routing import serve
import aiohttp
from aiohttp import web


async def main():
    checks=[]
    def check(name, value):
        assert value, name
        checks.append(name)
    with tempfile.TemporaryDirectory() as folder:
        data=Path(folder)
        for name,value in [('secrets.json',{'client':'synthetic','management':'synthetic'}),
                           ('control-settings.json',{'sources':[{'id':x,'name':x,'enabled':True,'kind':'api','models':[{'alias':'test'}]} for x in ('a','b')],'session_affinity':True})]:
            (data/name).write_text(json.dumps(value))
        os.environ['ROUTER_DATA']=folder
        spec=importlib.util.spec_from_file_location('structured_router',Path(__file__).with_name('router.py'))
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        cases={
            'context_length_exceeded':'invalid_request','context_too_large':'invalid_request',
            'invalid_prompt':'invalid_request','bad_request_body':'invalid_request',
            'rate_limit_exceeded':'rate_limited','slow_down':'rate_limited','server_is_overloaded':'rate_limited',
            'invalid_api_key':'auth_required','permission_denied':'permission_denied',
            'unsupported_country_region_territory':'access_denied',
            'insufficient_user_quota':'model_unavailable','pre_consume_token_quota_failed':'model_unavailable',
            'credit_balance_exhausted':'model_unavailable','project_spend_limit_exceeded':'model_unavailable',
            'channel:no_available_key':'model_unavailable','model_price_error':'model_unavailable',
            'channel:model_mapped_error':'model_unavailable','get_channel_failed':'model_unavailable',
            'do_request_failed':'upstream_error','channel:response_time_exceeded':'upstream_error',
            'channel:invalid_key':'upstream_error','bad_response_body':'upstream_error',
            'previous_response_not_found':'continuation_requires_full_history',
            'cyber_policy':'policy_rejected','bio_policy':'policy_rejected',
            'misalignment_policy_violation':'policy_rejected','prompt_blocked':'policy_rejected',
            'sensitive_words_detected':'policy_rejected'}
        for code,kind in cases.items():
            for status in (400,502):
                actual=m.classify(status,{'error':{'code':code,'type':'invalid_request_error'}})
                check(f'{code} at {status}',actual.kind==kind)
        for code in ('insufficient_quota','credit_balance_exhausted','organization_spend_limit_exceeded','project_spend_limit_exceeded','usage_limit_reached'):
            f=m.classify(502,{'error':{'code':code,'resets_in_seconds':300}},official=True)
            check('official '+code,f.kind=='quota_wait' and time.time()+298<f.reset<time.time()+301)
        for value in ('nan','inf',True,{},-1):
            f=m.classify(429,{'error':{'code':'usage_limit_reached','resets_at':value}},official=True)
            check('invalid reset '+str(value),f.reset==0)
        check('nested Retry-After',m.classify(502,{'error':{'code':'slow_down','headers':{'retry-after':'12'}}}).delay==12)
        check('HTTP Retry-After',m.classify(400,{'error':{'code':'server_is_overloaded'}},{'Retry-After':'9'}).delay==9)
        check('unknown HTTP 403 not automatically model scoped',m.classify(403,{}).kind=='access_denied')
        check('message keywords do not override parameter error',m.classify(400,{'error':{'type':'invalid_request_error','message':'model_not_found insufficient_quota server_error'}}).kind=='invalid_request')
        check('malformed error fields do not throw',m.classify(502,{'error':{'code':['bad'],'type':{},'message':[]}}).kind=='upstream_error')
        policy={'error':{'code':'prompt_blocked','message':json.dumps({'error':{'code':'upstream_unavailable'}})}}
        check('policy cannot be unwrapped away',m.classify(400,policy).kind=='policy_rejected')
        nested={'error':{'type':'invalid_request_error','message':'data: '+json.dumps({'error':{'code':'server_is_overloaded','type':'service_unavailable_error'}})}}
        check('nested overload classification',m.classify(400,nested).kind=='rate_limited')
        r=m.Router();hits=[];mode={}
        async def upstream(request):
            body=await request.json();ident=body['model'].split('/')[0];hits.append(ident)
            if ident=='b':
                if body.get('stream'):return web.Response(text='data: '+json.dumps({'type':'response.completed','response':{'status':'completed','id':'fake','output':[]}})+'\n\n',content_type='text/event-stream')
                return web.json_response({'status':'completed','id':'fake','output':[]})
            payload=mode['payload']
            if mode['wire']=='http':return web.json_response(payload,status=400)
            if mode['wire']=='json':return web.json_response({'status':'failed',**payload})
            frame={'type':'response.failed','response':{'status':'failed',**payload}}
            return web.Response(text='data: '+json.dumps(frame)+'\n\n',content_type='text/event-stream',headers={'Retry-After':'8'})
        app=web.Application();app.router.add_post('/v1/responses',upstream)
        native,m.GATEWAY=await serve(app)
        async def no_monitor():await asyncio.Event().wait()
        r.monitor=no_monitor
        runner,url=await serve(r.app())
        try:
            async with aiohttp.ClientSession() as client:
                async def run(payload,wire='sse'):
                    mode.update(payload=payload,wire=wire);hits.clear();r.state.items.clear();r.state.history.clear();r.state.active.clear()
                    async with client.post(url+'/v1/responses',headers={'Authorization':'Bearer synthetic'},json={'model':'test','input':'synthetic','stream':wire=='sse'}) as resp:
                        raw=await resp.text()
                        return resp.status,json.loads(raw) if resp.content_type=='application/json' else {}
                for wire in ('http','json','sse'):
                    status,_=await run({'error':{'code':'do_request_failed'}},wire)
                    check(wire+' upstream failure falls back',status==200 and hits==['a','b'])
                for code in ('context_length_exceeded','invalid_prompt','prompt_blocked','previous_response_not_found'):
                    status,body=await run({'error':{'code':code}})
                    check(code+' stops without fallback',status in (400,409) and hits==['a'])
                    check(code+' does not poison source',not r.state.get('a').get('consecutive') and not r.state.get('a/test').get('reason'))
                    check(code+' returns clear request error',body['error']['code'] in m.STOP_FAILURES)
                status,_=await run({'error':{'code':'rate_limit_exceeded'}})
                check('SSE rate header survives parser',status==200 and r.state.get('a')['until']>time.time()+6)
                status,_=await run({'error':{'code':'insufficient_user_quota'}})
                check('relay quota only isolates model',status==200 and r.state.get('a/test')['reason']=='model_unavailable' and not r.state.get('a').get('quota_blocked'))
                status,_=await run({'error':{'type':'invalid_request_error','message':'data: '+json.dumps({'error':{'code':'upstream_unavailable','type':'server_error','message':'synthetic secret-like message'}})}},'http')
                check('wrapped 400 switches after normalization',status==200 and hits==['a','b'])
                with contextlib.closing(r.db()) as db:
                    traces=[json.loads(row[0]) for row in db.execute('select trace from requests')]
                diagnostics=[t['upstream_error'] for trace in traces for t in trace if 'upstream_error' in t]
                check('diagnostic preserves outer HTTP and inner code',any(d.get('outer_http')==400 and d.get('code')=='upstream_unavailable' and d.get('outer_type')=='invalid_request_error' for d in diagnostics))
                check('stream error preserves actual HTTP 200 transport',any(d.get('outer_http')==200 and d.get('code')=='rate_limit_exceeded' for d in diagnostics))
                check('diagnostics never save raw message',all('message' not in d and 'synthetic secret-like message' not in json.dumps(d) for d in diagnostics))
        finally:
            await runner.cleanup();await native.cleanup()
    report={'passed':True,'count':len(checks),'checks':checks,'real_provider_calls':0}
    Path('reports/error-normalization-verification.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report))

if __name__=='__main__':asyncio.run(main())
