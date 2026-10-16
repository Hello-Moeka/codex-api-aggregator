"""Audit regressions against local synthetic providers; no real keys or traffic."""
import asyncio,ast,importlib.util,json,os,tempfile,time
from pathlib import Path
from unittest.mock import patch
import aiohttp
from aiohttp import web
from verify_routing import serve

async def main():
 checks=[]
 def ok(name,value):
  assert value,name
  checks.append(name)
 with tempfile.TemporaryDirectory() as td:
  p=Path(td);os.environ['ROUTER_DATA']=td
  def save(n,v):(p/n).write_text(json.dumps(v),encoding='utf-8')
  save('secrets.json',{'client':'synthetic','management':'synthetic-admin'})
  sources=[{'id':x,'name':x,'kind':'api','enabled':True,'models':[{'alias':'m'}]} for x in ['a','b']]
  save('control-settings.json',{'sources':sources,'session_affinity':True})
  spec=importlib.util.spec_from_file_location('audit_router',Path(__file__).with_name('router.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  r=m.Router();r.clock_scale=.01
  modes={'a':'ok'};hits=[]
  def frame(o):return 'data: '+json.dumps(o)+'\n\n'
  done={'type':'response.completed','response':{'id':'resp-local','status':'completed'}}
  async def upstream(req):
   b=await req.json();ident=b['model'].split('/')[0];hits.append(ident);mode=modes.get(ident,'ok')
   if mode=='nonstreamquota':return web.json_response({'status':'failed','error':{'code':'usage_limit_reached'}})
   parts=[{'type':'response.created','response':{}}]
   if mode=='missing':parts += [{'unknown':'no type'},done]
   elif mode=='shape':parts += [{'type':123},done]
   elif mode=='wrong':parts=[{'object':'chat.completion.chunk','choices':[]}]
   elif mode=='malformed':return web.Response(text=frame(parts[0])+'data: invalid-json\n\n'+frame(done),content_type='text/event-stream')
   elif mode=='chat':
    resp=web.StreamResponse(headers={'Content-Type':'text/event-stream'});await resp.prepare(req)
    await resp.write(frame({'object':'chat.completion.chunk','choices':[{'index':0,'delta':{'content':'OK'},'finish_reason':'stop'}]}).encode())
    try:await asyncio.sleep(2)
    except asyncio.CancelledError:pass
    return resp
   else:parts += [done]
   return web.Response(text=''.join(frame(x) for x in parts),content_type='text/event-stream')
  app=web.Application();app.router.add_post('/v1/responses',upstream);app.router.add_post('/v1/chat/completions',upstream)
  native,m.GATEWAY=await serve(app)
  async def no_monitor():await asyncio.Event().wait()
  r.monitor=no_monitor
  runner,url=await serve(r.app())
  try:
   async with aiohttp.ClientSession() as c:
    for mode in ['missing','shape','wrong','malformed']:
     r.state.items.clear();hits.clear();modes['a']=mode
     async with c.post(url+'/v1/responses',json={'model':'m','input':'test','stream':True},headers={'Authorization':'Bearer synthetic'}) as resp:
      raw=await resp.read()
      ok('business parser rejects '+mode+' and falls back',resp.status==200 and hits==['a','b'] and b'response.completed' in raw)
    modes['a']='malformed'
    async with c.post(url+'/internal/probe',json={'model':'a/m','input':'test','stream':True},headers={'Authorization':'Bearer synthetic-admin'}) as resp:
     result=await resp.json();ok('recovery rejects malformed stream even if completed follows',result.get('reason')=='invalid_response' and not result['ok'])
    modes['a']='ok'
    async with c.post(url+'/internal/probe',json={'model':'a/m','input':'test','stream':True},headers={'Authorization':'Bearer synthetic-admin'}) as resp:
     result=await resp.json();ok('recovery accepts valid stream using shared parser',result['ok'] and result['status']==200)
    r.state.items.clear();modes['a']='chat';t=time.monotonic()
    async with c.post(url+'/v1/chat/completions',json={'model':'m','messages':[],'stream':True},headers={'Authorization':'Bearer synthetic'}) as resp:
     raw=await resp.read();ok('chat terminal frame finishes without waiting for socket close',time.monotonic()-t<1 and b'[DONE]' in raw)
    r.state.items.clear();modes['a']='nonstreamquota'
    class Down:
     path='/v1/responses';headers={};transport=object()
    try:await r.attempt(Down(),{'model':'m','stream':False},{'id':'a','kind':'oauth'},'m',False,time.monotonic()+5)
    except m.Failure as e:ok('nonstream official quota classified as official rather than relay balance',e.kind=='quota_wait')
    # Monotonic revision guards an older success from erasing a newer failure.
    r.state.items.clear();versions={'a':0,'a/m':0};r.state.failure('a','m',m.Failure('upstream_error'))
    r.state.success('a','m',[],versions);ok('late old success cannot clear newer failure',r.state.get('a')['consecutive']==1)
    r.remember('b','session',None,200);r.remember('a','session',None,100)
    ok('late old request cannot overwrite newer session binding',r.sessions['session'][0]=='b')
    r.state.items['a']={'state':'closed','consecutive':1,'probe_model':'m','retry_check_at':0}
    modes['a']='ok'
    class Forward:
     def post(self,target,**kw):return c.post(target.replace('http://127.0.0.1:18319',url),**kw)
    real_client=r.client;r.client=Forward()
    try:
     await r.check('a');ok('non-tripped failure gets a recovery path without user traffic',r.state.get('a')['state']=='half_open')
     r.state.items['a']['until']=0;await r.check('a')
     ok('non-tripped recovery still needs two successes and observation',r.state.get('a')['state']=='closed' and r.state.get('a')['reclaim_after']>time.time())
    finally:r.client=real_client
    r.state.items['a']={'state':'half_open','successes':1};r.state.history['a'].extend([True]*8)
    r.state.success('a','m',['a']);ok('confirmed recovery clears stale rolling failures',len(r.state.history['a'])==0)
    r.state.items['a']={'state':'closed','consecutive':0,'reclaim_after':time.time()-100,'reclaim_verified':True}
    r.state.failure('a','m',m.Failure('upstream_error'));ok('old successful recovery does not make every later failure trip immediately',r.state.get('a')['state']=='closed')
    ok('nonfinite retry delay is bounded',m.retry_after('inf')==60)
    official={'id':'o','kind':'oauth','slot':'A'};(p/'auths').mkdir();save('auths/plus-A.json',{'access_token':'synthetic'})
    class Reply:
     status=200
     allowed=True
     async def __aenter__(self):return self
     async def __aexit__(self,*args):pass
     async def json(self):return {'rate_limit':{'allowed':self.allowed,'primary_window':{'used_percent':1 if self.allowed else 100}}}
    class Client:
     def get(self,*args,**kw):return Reply()
    real=r.client;r.client=Client()
    try:
     r.state.items['o']={'state':'closed','consecutive':2,'reason':'first_event_timeout'}
     result=await r.quota(official);r.state.items['o'].update(result)
     ok('positive quota query preserves ordinary failure counter',r.state.get('o')['consecutive']==2)
     r.state.items['o'].update(state='open',until=12345)
     Reply.allowed=False;r.state.items['o'].update(await r.quota(official))
     ok('exhausted quota preserves independent open circuit',r.state.get('o')['state']=='open' and r.state.get('o')['quota_blocked'])
     Reply.allowed=True;r.state.items['o'].update(await r.quota(official))
     ok('restored quota does not erase independent open circuit',r.state.get('o')['state']=='open' and not r.state.get('o')['quota_blocked'])
    finally:r.client=real
    save('control-settings.json',{'sources':[dict(official,name='official',enabled=True,models=[{'alias':'m'}])]+sources})
    r.state.items['o']={'state':'closed','consecutive':1,'probe_model':'m','retry_check_at':0}
    async def quota_noop(*args,**kw):pass
    original_refresh=r.refresh_official;r.refresh_official=quota_noop;r.client=Forward()
    try:
     await r.check('o');ok('official non-tripped failure can recover without sticky user traffic',r.state.get('o')['state']=='half_open')
    finally:r.client=real;r.refresh_official=original_refresh
    r.log('malformed-usage',time.monotonic(),'/v1/responses','m',[],200,'success',['bad'])
    r.log('malformed-details',time.monotonic(),'/v1/responses','m',[],200,'success',{'input_tokens_details':['bad']})
    ok('malformed optional usage cannot break completed request logging',True)
    api_source={'id':'a','kind':'api','models':[{'alias':'gpt-6.1-sol'},{'alias':'gpt-5.6-luna'}]}
    ok('relay quota response is model unavailable',m.classify(403,{'error':{'code':'insufficient_quota'}},official=False).kind=='model_unavailable')
    ok('recovery probe defaults to gpt-6.1-sol',r.default_probe_model(api_source)=='gpt-6.1-sol')
    # Load only the pure probe function; never execute controller startup/config.
    tree=ast.parse(Path('scripts/controller.py').read_text(encoding='utf-8-sig'))
    fn=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='probe_stream')
    class Stream:
     def __enter__(self):return self
     def __exit__(self,*args):pass
     def __iter__(self):
      yield b'data:{"type":"response.output_text.delta","delta":"GATEWAY_OK"}\n'
      yield b'data:{"type":"response.completed"}\n'
      raise AssertionError('read continued after completion')
    from urllib.request import Request
    from urllib.error import HTTPError
    ns={'time':time,'json':json,'Request':Request,'HTTPError':HTTPError,'ROUTER':'http://synthetic','GATEWAY':'','KEYS':{'client':'synthetic'},'urlopen':lambda *a,**k:Stream(),'now':lambda:'synthetic'}
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'probe_stream','exec'),ns)
    ok('manual test accepts compact SSE and stops at completed event',ns['probe_stream']('m')['passed'])
  finally:await runner.cleanup();await native.cleanup()
 report={'passed':True,'count':len(checks),'checks':checks,'real_provider_calls':0}
 Path('reports/code-audit-verification.json').write_text(json.dumps(report,indent=2),encoding='utf-8');print(json.dumps(report))
asyncio.run(main())
