"""Synthetic integration checks for the six review findings."""
import asyncio, ast, importlib.util, json, os, tempfile, threading, time
from pathlib import Path
from types import SimpleNamespace

async def main():
 checks=[]
 def check(name,value):assert value,name;checks.append(name)
 with tempfile.TemporaryDirectory() as td:
  os.environ['ROUTER_DATA']=td
  def save(n,v):(Path(td)/n).write_text(json.dumps(v))
  save('secrets.json',{'management':'synthetic','client':'synthetic'})
  source={'id':'a','name':'Synthetic','kind':'api','enabled':True,'models':[{'alias':'gpt-6.1-sol'},{'alias':'broken'}]}
  save('control-settings.json',{'sources':[source],'session_affinity':True})
  save('routing-state.json',{'a/broken':{'state':'model_balance_wait','probe_model':'broken'}})
  spec=importlib.util.spec_from_file_location('review_router',Path('scripts/router.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  r=m.Router()
  check('legacy model wait migrates to recoverable breaker',r.state.get('a/broken')['state']=='open')
  hits=[];response_result=[{'ok':True,'status':200}]
  class Response:
   async def __aenter__(self):return self
   async def __aexit__(self,*args):pass
   async def json(self):return response_result[0]
  class Client:
   def post(self,url,**kwargs):hits.append(kwargs['json']['model']);return Response()
  r.client=Client()
  r.state.items={'a':{'state':'open','until':0,'probe_model':'broken','reason':'invalid_response'},'a/broken':{'state':'open','until':time.time()+1000,'probe_model':'broken','reason':'model_unavailable'}}
  await r.check('a')
  check('source recovery uses default despite broken model cooldown',hits==['a/gpt-6.1-sol'])
  check('source probe does not cure broken model',r.state.get('a/broken')['state']=='open')
  r.state.items['a']['until']=0
  await r.check('a')
  check('two source probes restore source',r.state.get('a')['state']=='closed')
  hits.clear();r.state.items['a/broken']['until']=0
  await r.check('a')
  check('model recovery still verifies exact affected model',hits==['a/broken'])
  response_result[0]={'ok':False,'status':403,'reason':'model_unavailable'}
  r.state.items={'a':{'state':'open','until':0,'probe_model':'broken','reason':'invalid_response'}}
  await r.check('a')
  check('source model rejection keeps source breaker open',r.state.get('a')['state']=='open')
  check('source model rejection schedules cooldown instead of tight loop',r.state.get('a')['until']>=time.time()+29)
  response_result[0]={'ok':True,'status':200}
  r.state.items={'a':{'state':'closed','reclaim_after':time.time()-1,'reclaim_verified':True}}
  r.state.failure('a','broken',m.Failure('upstream_error'))
  r.state.success('a','broken')
  check('one ordinary success cannot restore reclaim eligibility',r.state.get('a')['state']=='half_open' and r.state.get('a')['reclaim_required'])
  r.state.success('a','broken',['a'])
  check('second success starts a fresh observation',r.state.get('a')['reclaim_after']>time.time() and not r.state.get('a')['reclaim_verified'] and not r.state.get('a')['reclaim_required'])
  class InvalidResponse:
   status=200
   headers={}
   async def __aenter__(self):return self
   async def __aexit__(self,*args):pass
   async def read(self):return json.dumps(payload[0]).encode()
  class InvalidClient:
   async def post(self,*args,**kwargs):return InvalidResponse()
  r.client=InvalidClient();payload=[{}]
  async def attempt():
   return await r.attempt(SimpleNamespace(path='/v1/chat/completions',headers={}),{'model':'broken','stream':False},source,'broken',False,time.monotonic()+10)
  for value in (['bad'],{'bad':1},[{'message':'bad'}],[{'message':{'content':42}}]):
   payload[0]={'choices':value}
   try:await attempt()
   except m.Failure as e:check('malformed choices rejected '+str(value),e.kind=='invalid_response')
   else:raise AssertionError('malformed response accepted')
  payload[0]={'choices':[{'message':{'role':'assistant','content':None,'tool_calls':[]},'finish_reason':'tool_calls'}]}
  await attempt();check('valid tool response with null content remains supported',True)
  routing={'a/observing':{'state':'closed','reclaim_after':time.time()+300},'a/broken':{'state':'open'}}
  fn=next(n for n in ast.parse(Path('scripts/controller.py').read_text(encoding='utf-8-sig')).body if isinstance(n,ast.FunctionDef) and n.name=='status')
  ns={'request_json':lambda *a,**k:{'data':[]},'GATEWAY':'synthetic','KEYS':{'client':'synthetic'},'ROUTER':'','LOCK':threading.RLock(),'SETTINGS':{'sources':[source],'session_affinity':True,'official_reserve':{}},'original':lambda s:{'base_url':'synthetic','name':'Synthetic'},'QUOTAS':{'quotas':{}},'DISCOVERY':{},'load':lambda *a:routing,'DATA':Path(td),'TESTS':{},'JOB':{},'EVENTS':[],'BATCH':{},'json':json,'APPLY_ERROR':'','CSRF':'synthetic','now':lambda:'synthetic'}
  exec(compile(ast.Module(body=[fn],type_ignores=[]),'controller-status','exec'),ns)
  exposed=ns['status']()['sources'][0]
  check('controller exposes recovered model observation separately',list(exposed['model_recovery'])==['observing'])
  check('controller fault list excludes recovered observations',list(exposed['model_routing'])==['broken'])
 result={'passed':True,'count':len(checks),'checks':checks,'real_provider_calls':0}
 Path('reports/review-fixes-verification.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
asyncio.run(main())
