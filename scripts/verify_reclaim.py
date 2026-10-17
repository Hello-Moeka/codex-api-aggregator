import tempfile,json,os,importlib.util,hashlib
from pathlib import Path
from unittest.mock import patch
with tempfile.TemporaryDirectory() as td:
 p=Path(td);os.environ['ROUTER_DATA']=td
 def save(n,v):(p/n).write_text(json.dumps(v))
 save('secrets.json',{})
 sources=[{'id':x,'name':x,'kind':'api','enabled':True,'models':[{'alias':'m'}]} for x in ['a','b','c']]
 save('control-settings.json',{'sources':sources,'session_affinity':True})
 spec=importlib.util.spec_from_file_location('r',Path('scripts/router.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);r=m.Router();checks=[]
 def check(name,cond):assert cond,name;checks.append(name)
 sid=hashlib.sha256(b's').hexdigest()
 def order(body=None):return [s['id'] for s in r.candidates(body or {'model':'m'},{'session_id':'s'})[1]]
 with patch.object(m.time,'time',return_value=10000):
  r.sessions[sid]=('c',9990,9401);r.state.items['a/m']={'last_success_at':'ok'}
  check('under 10 minutes remains sticky',order()[0]=='c')
  r.remember('c',sid,None);check('normal request preserves switch clock',r.sessions[sid][2]==9401)
  r.sessions[sid]=('c',9990,9400)
  r.state.items['a']={'reclaim_after':9900,'reclaim_verified':False}
  check('observation requires extra successful probe',order()[0]=='c')
  r.state.items['a']['reclaim_verified']=True
  check('exactly 10 minutes allows reclaim evaluation',order()[0]=='a')
  r.sessions[sid]=('c',9990,9400.001)
  check('less than 10 minutes remains sticky',order()[0]=='c')
  r.sessions[sid]=('c',9990,9399.999)
  check('earliest recovered provider reclaims after hold',order()[0]=='a')
  check('explicit request remains directed',order({'model':'c/m'})==['c'])
  r.responses['resp']=('c',9990)
  check('previous response retains owner',order({'model':'m','previous_response_id':'resp'})==['c'])
  r.state.items['a/m']['state']='open';check('model fault prevents reclaim',order()[0]=='c')
  r.state.items['a/m']['state']='closed'
  r.state.items['b']={'state':'closed','reclaim_after':9900,'reclaim_verified':True}
  r.state.failure('b','m',m.Failure('upstream_error'))
  check('new failure invalidates a completed observation',not r.state.get('b')['reclaim_verified'] and r.state.get('b')['reclaim_after']==0)
  r.state.items['a']['reclaim_active_until']=10001
  r.state.failure('a','m',m.Failure('upstream_error'))
  check('failed observation reopens circuit and doubles delay',r.state.get('a')['state']=='open' and r.state.get('a')['reclaim_delay']==600)
  r.state.items['a'].update(state='half_open',successes=1)
  r.state.success('a','m',['a']);check('recovery starts increased observation',r.state.get('a')['reclaim_after']==10600)
  official={'id':'o','name':'o','kind':'oauth','enabled':True}
  save('official-models.json',{'o':['m']});save('control-settings.json',{'sources':[official]+sources,'session_affinity':True})
  r.sessions[sid]=('c',9990,9990)
  r.state.items['o']={'reserve_known':True}
  check('official bypasses relay hold',order()[0]=='o')
print(json.dumps({'passed':True,'count':len(checks),'checks':checks}))
Path('reports/reclaim-verification.json').write_text(json.dumps({'passed':True,'checks':checks},indent=2))
