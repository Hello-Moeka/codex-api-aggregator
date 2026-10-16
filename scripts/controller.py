"""Local-only container control plane. No Docker socket or host Codex access."""
import datetime, hashlib, json, math, os, re, secrets, sqlite3, subprocess, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from urllib.parse import urlparse
import yaml

ROOT=Path(__file__).resolve().parents[1]; DATA=ROOT/'private'; DATA.mkdir(exist_ok=True)
GATEWAY=os.environ.get('GATEWAY_URL','http://127.0.0.1:18317')
ROUTER=os.environ.get('ROUTER_URL','')
EXE=os.environ.get('GATEWAY_EXE','/usr/local/bin/cli-proxy-api')
KEYS=json.loads((DATA/'secrets.json').read_text(encoding='utf-8'))
UPSTREAMS=json.loads((DATA/'upstreams.json').read_text(encoding='utf-8'))
LOCK=threading.RLock(); JOB_LOCK=threading.Lock(); LOGIN_LOCK=threading.Lock()
DEVICE={}; JOB={'busy':False,'message':'就绪'}; EVENTS=deque(maxlen=40)
CSRF=secrets.token_urlsafe(24)
APPLY_ERROR=''
IDS=[p.get('id') or ('provider-'+str(i+1)) for i,p in enumerate(UPSTREAMS)]
OFFICIAL=['gpt-6.1-sol','gpt-6-astra','gpt-6-sol','gpt-6-luna','gpt-5.6-sol','gpt-5.6-terra','gpt-5.6-luna']

def now():return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat(timespec='seconds')
def load(path,default):
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else default
def save(path,value):
    tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8');os.replace(tmp,path)
def event(message):
    with LOCK:
        EVENTS.appendleft({'time':now(),'message':message})
        save(DATA/'events.json',list(EVENTS))
def request_json(url,headers=None,body=None,timeout=15):
    request=Request(url,headers=headers or {},data=json.dumps(body).encode() if body is not None else None)
    with urlopen(request,timeout=timeout) as response:return json.load(response)

def defaults():
    sources=[{'id':'plus-a','name':'Plus A','kind':'oauth','slot':'A','enabled':True},
             {'id':'plus-b','name':'Plus B','kind':'oauth','slot':'B','enabled':True}]
    for ident,p in zip(IDS,UPSTREAMS):
        sources.append({'id':ident,'name':p['name'],'kind':'api','enabled':True,
                        'models':[{'upstream':m['model'],'alias':m['model'],**{k:m[k] for k in ('contextWindow','reasoningLevels') if k in m}} for m in p['models']]})
    return {'sources':sources,'session_affinity':True,'official_reserve':{'enabled':True,'percent':10}}

SETTINGS=load(DATA/'control-settings.json',defaults())
SETTINGS.setdefault('official_reserve',{'enabled':True,'percent':10})
EVENTS.extend(load(DATA/'events.json',[]))
QUOTAS=load(DATA/'quota-state.json',{'disabled':[],'quotas':{}})
DISCOVERY=load(DATA/'discovered-models.json',{})
TESTS=load(DATA/'provider-tests.json',{})
OFFICIAL_CATALOG=load(DATA/'official-models.json',{})
BATCH=load(DATA/'batch-test.json',{})
BATCH_CANCEL=threading.Event()
TRAFFIC_LOCK=threading.RLock()
TRAFFIC_STATE={'enabled':True,'error':'','last_collected':None}
def original(source):return UPSTREAMS[IDS.index(source['id'])]
def find(ident):
    for source in SETTINGS['sources']:
        if source['id']==ident:return source
    raise ValueError('来源不存在')

def render():
    global APPLY_ERROR
    groups=[];chat_groups=[]
    for index,source in enumerate(SETTINGS['sources']):
        priority=(len(SETTINGS['sources'])-index)*100
        if source['kind']=='oauth':
            path=DATA/'auths'/('plus-'+source['slot']+'.json')
            if path.exists():
                credential=load(path,{})
                values={'priority':priority,'disabled':not source['enabled'],'prefix':source['id']}
                if any(credential.get(k)!=v for k,v in values.items()):
                    credential.update(values);save(path,credential)
            continue
        p=original(source)
        if not source['enabled'] or p['name'] in QUOTAS['disabled']:continue
        if not source['models']:continue
        models=[]
        for m in source['models']:
            item={'name':m['upstream'],'alias':m['alias']}
            if m.get('contextWindow'):item['max-context-length']=m['contextWindow']
            if m.get('reasoningLevels'):item['thinking']={'levels':m['reasoningLevels']}
            models.append(item)
        group={'name':source['name'],'prefix':source['id'],'priority':priority,'base-url':p['base_url'],
                       'request-retry':0,'keys':[{'api-key':p['api_key'],'priority':priority}], 'models':models,
                       'request-scoped-errors':[{'status':400,'match':['context_length_exceeded','context_window_exceeded'],'action':'stop'},
                                                {'status':403,'match':['insufficient_quota','余额不足'],'action':'continue-and-cooldown'}]}
        # This API lane accepts standard API requests rather than forced Codex client headers.
        if p.get('disable_codex_cloaking'):
            group['headers']=p.get('headers',{})
            group['keys'][0]['disable-codex-cloaking']=True
        (chat_groups if p.get('api_format')=='openai_chat' else groups).append(group)
    config={'config-version':8,'server':{'host':'0.0.0.0','port':18317},
            'management':{'allow-remote':True,'secret-key':KEYS['management'],'disable-control-panel':True},
            'access':{'api-keys':[KEYS['client']]},'oauth':{'auth-dir':str(DATA/'auths')},
            'api-keys':{'codex':groups,'openai-compatibility':chat_groups},'routing':{'strategy':'fill-first','force-model-prefix':False,
            'session-affinity':SETTINGS['session_affinity'],'session-affinity-rebind-on':'any',
            'retry':{'request-retry':0,'max-retry-credentials':0,'max-retry-interval':0},
            'cooldown':{'disable-cooling':bool(ROUTER),'save-cooldown-status':not bool(ROUTER),'transient-error-cooldown-seconds':30}},
            'requests':{'tls-handshake-timeout-seconds':10,'passthrough-headers':True,'streaming':{'bootstrap-retries':0}},
            'observability':{'logs':{'debug':False,'logging-to-file':True,'logs-max-total-size-mb':20,'request-log':False,'error-logs-max-files':3},
                             'usage':{'usage-statistics-enabled':True,'redis-usage-queue-retention-seconds':3600}}}
    path=DATA/'docker-config.yaml';tmp=path.with_suffix('.tmp')
    tmp.write_text(yaml.safe_dump(config,allow_unicode=True,sort_keys=False),encoding='utf-8');os.replace(tmp,path)
    save(DATA/'control-settings.json',SETTINGS)
    if os.environ.get('PREPARE_ONLY')!='1':
        try:
            request=Request(GATEWAY+'/v8/management/config',method='PUT',data=json.dumps(config).encode(),
                            headers={'Authorization':'Bearer '+KEYS['management'],'Content-Type':'application/json'})
            with urlopen(request,timeout=10) as response:response.read()
            # File-backed OAuth credentials need an explicit runtime update too;
            # Docker Desktop bind mounts do not reliably deliver file watch events.
            for index,source in enumerate(SETTINGS['sources']):
                if source['kind']!='oauth' or not (DATA/'auths'/('plus-'+source['slot']+'.json')).exists():continue
                name='plus-'+source['slot']+'.json'
                for endpoint,payload in [('/credentials/fields',{'name':name,'priority':(len(SETTINGS['sources'])-index)*100,'prefix':source['id']}),
                                         ('/credentials/status',{'name':name,'disabled':not source['enabled']})]:
                    request=Request(GATEWAY+'/v8/management'+endpoint,method='PATCH',data=json.dumps(payload).encode(),
                                    headers={'Authorization':'Bearer '+KEYS['management'],'Content-Type':'application/json'})
                    with urlopen(request,timeout=10) as response:response.read()
            APPLY_ERROR=''
        except Exception as error:
            APPLY_ERROR='配置已保存，等待网关应用：'+type(error).__name__

def quota_one(p):
    if not p.get('usage_url') or p.get('usage_kind')=='none':
        return {'status':'unknown','error':'not_configured','checked_at':now()}
    raw=request_json(p['usage_url'],p['usage_headers'])
    if raw.get('success') is not True or not isinstance(raw.get('data'),dict):raise ValueError('查询接口返回失败')
    d=raw['data']
    if p['usage_kind']=='topapi':
        balance,used,currency=float(d['balance']),float(d['used']),str(d['currency']).upper()
        if currency not in ('USD','CNY','RMB'):raise ValueError('未知币种')
    else:balance,used,currency=float(d['quota'])/500000,float(d['used_quota'])/500000,'CNY'
    if not all(math.isfinite(v) for v in (balance,used)):raise ValueError('无效余额')
    return {'status':'exhausted' if balance<=0 else 'available','remaining':balance,'used':used,'currency':currency,'checked_at':now()}

def refresh_quotas():
    for p in UPSTREAMS:
        try:result=quota_one(p)
        except Exception as error:result={'status':'unknown','error':type(error).__name__,'checked_at':now()}
        with LOCK:
            excluded=set(QUOTAS['disabled'])
            if result['status']=='exhausted':excluded.add(p['name'])
            if result['status']=='available':excluded.discard(p['name'])
            changed=set(QUOTAS['disabled'])!=excluded;QUOTAS['disabled']=sorted(excluded)
            result['excluded']=p['name'] in excluded;QUOTAS['quotas'][p['name']]=result
            save(DATA/'quota-state.json',QUOTAS)
            if changed:render()
    event('余额查询已完成')

def discover(source):
    if source['kind']!='api':raise ValueError('官方模型由网关目录提供')
    p=original(source)
    raw=request_json(p['base_url'].rstrip('/')+'/models',{'Authorization':'Bearer '+p['api_key'],'User-Agent':'cc-switch/1.0','Accept':'application/json'})
    if not isinstance(raw.get('data'),list):raise ValueError('上游未返回有效模型目录')
    models=sorted(set(m['id'] for m in raw['data'] if isinstance(m,dict) and isinstance(m.get('id'),str) and m['id']))
    with LOCK:
        current=find(source['id'])
        previous={m['upstream']:m for m in current['models'] if m['upstream']==m['alias']}
        current['models']=[{**previous.get(m,{}),'upstream':m,'alias':m} for m in models]
        DISCOVERY[source['id']]={'models':models,'checked_at':now(),'status':'synced'}
        save(DATA/'discovered-models.json',DISCOVERY);render()
    event(source['name']+'：已完整同步上游 '+str(len(models))+' 个模型')

def sync_all_models():
    for source in list(SETTINGS['sources']):
        if source['kind']!='api':continue
        try:discover(dict(source))
        except Exception as error:
            with LOCK:
                cached=DISCOVERY.setdefault(source['id'],{})
                cached.update(status='sync_failed',last_attempt=now(),error=type(error).__name__)
                save(DATA/'discovered-models.json',DISCOVERY)
            event(source['name']+'：目录同步失败，保留上次模型')

def probe_stream(model):
    start=time.monotonic();body={'model':model,'input':[{'role':'user','content':[{'type':'input_text','text':'Reply with exactly GATEWAY_OK.'}]}],
                              'stream':True,'reasoning':{'effort':'low'}}
    request=Request((ROUTER or GATEWAY)+'/v1/responses',data=json.dumps(body).encode(),headers={'Authorization':'Bearer '+KEYS['client'],'Content-Type':'application/json'})
    text='';completed=False
    try:
        with urlopen(request,timeout=120) as response:
            for line in response:
                if not line.startswith(b'data:'):continue
                try:item=json.loads(line[5:].strip())
                except ValueError:continue
                if not isinstance(item,dict):continue
                if item.get('type')=='response.output_text.delta':text+=item.get('delta','')
                if item.get('type')=='response.completed':completed=True;break
        result={'passed':completed and 'GATEWAY_OK' in text,'http':200,'completed':completed}
    except HTTPError as error:
        result={'passed':False,'http':error.code}
        try:
            payload=json.loads(error.read()).get('error',{})
            code=payload.get('code') or payload.get('type')
            if payload.get('attempted') is False:
                result.update(skipped=True,error='未执行：被当前熔断或额度规则拦截，可点击检查恢复')
            messages={'get_channel_failed':'该令牌分组没有此模型的可用渠道',
                      'model_unavailable':'该模型暂无渠道或未被此来源提供',
                      'permission_denied':'此来源没有该模型的使用权限',
                      'balance_wait':'上游额度不足，等待余额确认及恢复验证',
                      'quota_wait':'官方额度暂时耗尽，等待实际窗口恢复',
                      'reserve_wait':'已达到官方额度预留线，等待窗口恢复',
                      'reserve_unknown':'无法确认官方剩余额度，为保护预留暂时跳过',
                      'auth_required':'此来源需要更新密钥或重新授权',
                      'first_event_timeout':'等待上游有效事件超时',
                      'no_progress_timeout':'上游只有握手或心跳，未开始生成',
                      'invalid_response':'上游返回了无效协议内容',
                      'all_unavailable':'该来源当前在等待恢复；可点击检查恢复',
                      'upstream_unavailable':'上游服务暂时不可用',
                      'invalid_api_key':'上游凭据无效',
                      'insufficient_quota':'上游额度不足'}
            if code in messages and not result.get('skipped'):result['error']=messages[code]
            if code=='model_not_found':result['error']='该来源模型当前无可用路由'
            if 'auth_unavailable' in str(payload.get('message','')):result['error']='该来源当前无可用凭据（可能处于冷却）'
        except Exception:pass
        if not result.get('error'):result['error']={400:'上游拒绝请求，请检查模型与接口格式',403:'上游拒绝访问，请检查分组或模型权限',408:'上游请求超时',429:'上游限流或额度已耗尽'}.get(error.code,'上游请求失败')
    except Exception as error:result={'passed':False,'error':type(error).__name__}
    result.update(model=model,checked_at=now(),seconds=round(time.monotonic()-start,2));return result

def probe(model,timeout_seconds=60):
    """Bound the whole stream, including streams that keep emitting heartbeats."""
    start=time.monotonic()
    try:
        kwargs={'creationflags':subprocess.CREATE_NO_WINDOW} if os.name=='nt' else {}
        completed=subprocess.run([sys.executable,str(Path(__file__).resolve()),'--probe',model],
                                 stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=timeout_seconds,**kwargs)
        if completed.returncode!=0:raise RuntimeError('probe process failed')
        result=json.loads(completed.stdout)
    except subprocess.TimeoutExpired:
        result={'passed':False,'http':408,'timed_out':True,'error':'测试总耗时超过 '+str(timeout_seconds)+' 秒，已终止'}
    except Exception as error:result={'passed':False,'error':type(error).__name__}
    result.update(model=model,checked_at=now(),seconds=round(time.monotonic()-start,2));return result

def run_test(source,model):
    allowed=OFFICIAL_CATALOG.get(source['id'],OFFICIAL) if source['kind']=='oauth' else [m['alias'] for m in source['models']]
    if model not in allowed:raise ValueError('该来源没有配置此模型')
    result=probe(source['id']+'/'+model)
    with LOCK:TESTS[source['id']]=result;save(DATA/'provider-tests.json',TESTS)
    event(source['name']+'：请求测试'+('未执行' if result.get('skipped') else '通过' if result['passed'] else '失败'))

def batch_summary():
    rows=BATCH.get('results',[])
    BATCH.update(done=sum(r['status'] in ('passed','failed','skipped') for r in rows),
                 passed=sum(r['status']=='passed' for r in rows),failed=sum(r['status']=='failed' for r in rows),
                 skipped=sum(r['status']=='skipped' for r in rows))
    save(DATA/'batch-test.json',BATCH)

def run_batch(model):
    with LOCK:
        sources=json.loads(json.dumps(SETTINGS['sources']));BATCH_CANCEL.clear()
        BATCH.clear();BATCH.update(running=True,model=model,started_at=now(),total=len(sources),timeout_seconds=60,
                                  results=[{'id':s['id'],'name':s['name'],'status':'pending'} for s in sources])
        batch_summary()
    def test_one(source):
        with LOCK:
            row=next(r for r in BATCH['results'] if r['id']==source['id'])
            models=OFFICIAL_CATALOG.get(source['id'],OFFICIAL) if source['kind']=='oauth' else [m['alias'] for m in source['models']]
            reason='已取消后续测试' if BATCH_CANCEL.is_set() else '已暂停，保持暂停状态' if not source['enabled'] else '来源未提供该模型' if model not in models else None
            if reason:
                row.update(status='skipped',error=reason);batch_summary();return
            row.update(status='running',started_at=now());batch_summary()
        try:result=probe(source['id']+'/'+model)
        except Exception as error:result={'passed':False,'error':type(error).__name__,'model':source['id']+'/'+model,'checked_at':now(),'seconds':0}
        with LOCK:
            TESTS[source['id']]=result;save(DATA/'provider-tests.json',TESTS)
            row.update(result,status='skipped' if result.get('skipped') else 'passed' if result['passed'] else 'failed');batch_summary()
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(test_one,sources))
    finally:
        with LOCK:
            BATCH.update(running=False,finished_at=now(),cancelled=BATCH_CANCEL.is_set());batch_summary()
        event('全部测试结束：'+str(BATCH['passed'])+' 通过，'+str(BATCH['failed'])+' 失败，'+str(BATCH['skipped'])+' 跳过')

@contextmanager
def traffic_db():
    connection=sqlite3.connect(DATA/'request-logs.sqlite',timeout=10)
    connection.row_factory=sqlite3.Row
    connection.execute('''CREATE TABLE IF NOT EXISTS requests (
        request_key TEXT PRIMARY KEY,timestamp TEXT,endpoint TEXT,method TEXT DEFAULT 'POST',
        model TEXT DEFAULT '',source TEXT DEFAULT '',status_code INTEGER,latency_ms REAL,
        input_tokens INTEGER,output_tokens INTEGER,cached_tokens INTEGER,total_tokens INTEGER,
        attempts INTEGER DEFAULT 0,final_http INTEGER DEFAULT 0)''')
    connection.execute('CREATE TABLE IF NOT EXISTS offsets (path TEXT PRIMARY KEY,position INTEGER)')
    try:
        with connection:yield connection
    finally:connection.close()

def traffic_key(request_id,timestamp):
    try:
        date=datetime.datetime.fromisoformat(timestamp.replace('Z','+00:00')).astimezone(datetime.timezone(datetime.timedelta(hours=8))).date().isoformat()
    except Exception:date=timestamp[:10]
    return hashlib.sha256((date+'|'+request_id[-8:]).encode()).hexdigest()

def duration_ms(text):
    units={'ns':.000001,'us':.001,'µs':.001,'ms':1,'s':1000,'m':60000,'h':3600000}
    return sum(float(value)*units[unit] for value,unit in re.findall(r'([0-9.]+)(ns|us|µs|ms|s|m|h)',text))

def collect_traffic():
    headers={'Authorization':'Bearer '+KEYS['management']}
    credentials=request_json(GATEWAY+'/v8/management/credentials',headers,timeout=5).get('files',[])
    with LOCK:
        priorities={(len(SETTINGS['sources'])-i)*100:s['name'] for i,s in enumerate(SETTINGS['sources'])}
        by_prefix={s['id']:s['name'] for s in SETTINGS['sources']}
    names={str(f.get('auth_index')):priorities.get(f.get('priority'),'') for f in credentials}
    queued=request_json(GATEWAY+'/v8/management/observability/usage/queue?count=1000',headers,timeout=5)
    with TRAFFIC_LOCK,traffic_db() as db:
        for raw in queued:
            record=json.loads(raw) if isinstance(raw,str) else raw
            if not isinstance(record,dict):continue
            timestamp=str(record.get('timestamp') or now())
            try:timestamp=datetime.datetime.fromisoformat(timestamp.replace('Z','+00:00')).astimezone(datetime.timezone(datetime.timedelta(hours=8))).isoformat(timespec='milliseconds')
            except ValueError:pass
            request_id=str(record.get('request_id') or record.get('execution_id') or '')
            if not request_id:continue
            key=traffic_key(request_id,timestamp);alias=str(record.get('alias') or record.get('model') or '')
            source=names.get(str(record.get('auth_index')),'') or by_prefix.get(alias.split('/')[0],'')
            if not source:
                # Raw upstream source may be an API key; match it in memory and never save it.
                source=next((p['name'] for p in UPSTREAMS if record.get('source')==p['api_key']),'未识别来源')
            tokens=record.get('tokens') or {};fail=record.get('fail') or {}
            status=fail.get('status_code') or (500 if record.get('failed') else 200)
            endpoint=str(record.get('endpoint') or '/v1/responses').split('?')[0]
            method='POST'
            if ' ' in endpoint:method,endpoint=endpoint.split(' ',1)
            if not endpoint.startswith('/'):endpoint='/'+endpoint
            existing=db.execute('SELECT * FROM requests WHERE request_key=?',(key,)).fetchone()
            route=existing['source'].split(' → ') if existing and existing['source'] else []
            if source not in route:route.append(source)
            if existing:
                db.execute('''UPDATE requests SET model=?,source=?,status_code=CASE WHEN final_http=1 THEN status_code ELSE ? END,
                    latency_ms=CASE WHEN final_http=1 THEN latency_ms ELSE ? END,input_tokens=COALESCE(input_tokens,0)+?,
                    output_tokens=COALESCE(output_tokens,0)+?,cached_tokens=COALESCE(cached_tokens,0)+?,total_tokens=COALESCE(total_tokens,0)+?,attempts=attempts+1 WHERE request_key=?''',
                    (alias,' → '.join(route),status,record.get('latency_ms'),int(tokens.get('input_tokens') or 0),int(tokens.get('output_tokens') or 0),
                     int(tokens.get('cached_tokens') or 0),int(tokens.get('total_tokens') or 0),key))
            else:
                db.execute('''INSERT INTO requests(request_key,timestamp,endpoint,model,source,status_code,latency_ms,input_tokens,output_tokens,cached_tokens,total_tokens,attempts)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,1)''',(key,timestamp,endpoint,alias,source,status,record.get('latency_ms'),
                    tokens.get('input_tokens'),tokens.get('output_tokens'),tokens.get('cached_tokens'),tokens.get('total_tokens')))
        # Native access lines cover API errors rejected before an upstream was selected.
        pattern=re.compile(r'^\[([^\]]+)\] \[([^\]]+)\].*?\b(\d{3})\s*\|\s*([^|]+)\|[^|]+\|\s*([A-Z]+)\s+"([^\"]+)"')
        for path in (DATA/'logs').glob('main*.log'):
            stored=db.execute('SELECT position FROM offsets WHERE path=?',(path.name,)).fetchone();position=stored[0] if stored else 0
            if position>path.stat().st_size:position=0
            with path.open('rb') as file:
                file.seek(position)
                for line in file:
                    match=pattern.search(line.decode('utf-8',errors='replace'))
                    if not match:continue
                    timestamp,request_id,status,duration,method,endpoint=match.groups();endpoint=endpoint.split('?')[0]
                    if not endpoint.startswith('/v1/') or endpoint=='/v1/models':continue
                    timestamp=timestamp.replace(' ','T')+'+08:00'
                    if request_id=='--------':request_id=hashlib.sha256((path.name+str(file.tell())+line.decode(errors='replace')).encode()).hexdigest()
                    key=traffic_key(request_id,timestamp)
                    db.execute('''INSERT INTO requests(request_key,timestamp,endpoint,method,status_code,latency_ms,final_http) VALUES(?,?,?,?,?,?,1)
                        ON CONFLICT(request_key) DO UPDATE SET status_code=excluded.status_code,latency_ms=excluded.latency_ms,final_http=1,method=excluded.method''',
                        (key,timestamp,endpoint,method,int(status),duration_ms(duration)))
                position=file.tell()
            db.execute('INSERT OR REPLACE INTO offsets(path,position) VALUES(?,?)',(path.name,position))
        db.execute('DELETE FROM requests WHERE request_key IN (SELECT request_key FROM requests ORDER BY timestamp DESC LIMIT -1 OFFSET 5000)')
    TRAFFIC_STATE.update(error='',last_collected=now())

def traffic_worker():
    while True:
        try:collect_traffic()
        except Exception as error:TRAFFIC_STATE['error']=type(error).__name__
        time.sleep(2)

def traffic_rows():
    with TRAFFIC_LOCK,traffic_db() as db:
        rows=[dict(r) for r in db.execute('SELECT timestamp,endpoint,method,model,source,status_code,latency_ms,input_tokens,output_tokens,cached_tokens,total_tokens,attempts FROM requests ORDER BY timestamp DESC LIMIT 300')]
        total=db.execute('SELECT COUNT(*) FROM requests').fetchone()[0]
    path=DATA/'routing-requests.sqlite'
    if ROUTER and path.exists():
        connection=sqlite3.connect(path,timeout=5);connection.row_factory=sqlite3.Row
        try:
            recent=[]
            for record in connection.execute('SELECT * FROM requests ORDER BY timestamp DESC LIMIT 300'):
                item=dict(record);item['trace']=json.loads(item.get('trace') or '[]');recent.append(item)
            count=connection.execute('SELECT COUNT(*) FROM requests').fetchone()[0]
            rows=recent;total=count
        finally:connection.close()
    return {'rows':rows,'total':total,**TRAFFIC_STATE}

def job(action):
    if not JOB_LOCK.acquire(blocking=False):raise ValueError('已有操作执行中，请稍后')
    JOB.update(busy=True,message='正在执行，请稍候…')
    def worker():
        try:action();JOB.update(message='操作完成')
        except Exception as error:
            JOB.update(message='操作失败：'+(str(error) if isinstance(error,ValueError) else type(error).__name__))
            event(JOB['message'])
        finally:JOB['busy']=False;JOB_LOCK.release()
    threading.Thread(target=worker,daemon=True).start()

def login(slot):
    try:
        DEVICE.update(slot=slot,message='正在生成设备授权码')
        stage=DATA/('login-'+slot);stage.mkdir(exist_ok=True)
        cfg=DATA/('device-'+slot+'.yaml');cfg.write_text(yaml.safe_dump({'config-version':8,'oauth':{'auth-dir':str(stage)}}),encoding='utf-8')
        started=time.time();logpath=DATA/'logs'/('device-'+slot+'.log')
        with logpath.open('wb') as output:
            process=subprocess.Popen([EXE,'-config',str(cfg),'-codex-device-login','-no-browser'],stdin=subprocess.DEVNULL,stdout=output,stderr=output)
            deadline=time.monotonic()+900
            while process.poll() is None:
                if time.monotonic()>deadline:process.terminate();process.wait(timeout=10);raise ValueError('授权码已过期')
                match=re.search(r'Codex device code:\s*([A-Za-z0-9-]+)',logpath.read_text(encoding='utf-8',errors='replace'))
                if match:DEVICE.update(code=match[1],url='https://auth.openai.com/codex/device',message='等待官方授权')
                time.sleep(.5)
        if process.returncode!=0:raise ValueError('授权进程失败，请重试')
        files=sorted((p for p in stage.glob('*.json') if p.stat().st_mtime>=started-2),key=lambda p:p.stat().st_mtime,reverse=True)
        if not files:raise ValueError('未保存授权，请确认已完成官方页面操作')
        credential=load(files[0],{})
        if credential.get('type')!='codex' or not credential.get('access_token'):raise ValueError('凭据格式不正确')
        identity=credential.get('account_id') or credential.get('email')
        other=load(DATA/'auths'/('plus-'+('B' if slot=='A' else 'A')+'.json'),{})
        if not identity or identity==(other.get('account_id') or other.get('email')):raise ValueError('请使用与另一个槽位不同的账号')
        with LOCK:
            credential['label']='Plus '+slot
            # Register the fresh access/refresh pair in the live gateway before
            # changing its fields; do not depend on a bind-mount file watcher.
            request=Request(GATEWAY+'/v8/management/credentials?name=plus-'+slot+'.json',method='POST',
                            data=json.dumps(credential).encode(),headers={'Authorization':'Bearer '+KEYS['management'],'Content-Type':'application/json'})
            with urlopen(request,timeout=15) as response:response.read()
            render()
        for path in files:path.unlink()
        event('Plus '+slot+' 授权已更新')
    except Exception as error:event('设备授权失败：'+(str(error) if isinstance(error,ValueError) else type(error).__name__))
    finally:DEVICE.clear();LOGIN_LOCK.release()

def status():
    try:
        raw=request_json(GATEWAY+'/v1/models',{'Authorization':'Bearer '+KEYS['client']},timeout=3)
        models=sorted(m['id'] for m in raw.get('data',[]));healthy=True
    except Exception:models=[];healthy=False
    if ROUTER:
        try:request_json(ROUTER+'/health',timeout=2)
        except Exception:healthy=False
    with LOCK:
        sources=[]
        for index,source in enumerate(SETTINGS['sources']):
            s=dict(source);s['priority']=(len(SETTINGS['sources'])-index)*100
            if s['kind']=='oauth':
                s['authorized']=(DATA/'auths'/('plus-'+s['slot']+'.json')).exists()
                discovered=[m[len(s['id'])+1:] for m in models if m.startswith(s['id']+'/')]
                if discovered and OFFICIAL_CATALOG.get(s['id'])!=discovered:
                    OFFICIAL_CATALOG[s['id']]=discovered;save(DATA/'official-models.json',OFFICIAL_CATALOG)
                s['models']=[{'alias':m,'upstream':m} for m in OFFICIAL_CATALOG.get(s['id'],OFFICIAL)]
            else:
                p=original(source);s['base_url']=p['base_url'];s['api_format']=p.get('api_format','openai_responses');s['quota']=QUOTAS['quotas'].get(p['name'],{})
                s['discovered']=DISCOVERY.get(s['id'],{})
            routing=load(DATA/'routing-state.json',{})
            s['routing']=routing.get(s['id'],{})
            s['model_routing']={k[len(s['id'])+1:]:v for k,v in routing.items() if k.startswith(s['id']+'/') and v.get('state','closed')!='closed'}
            s['last_test']=TESTS.get(s['id']);sources.append(s)
        return {'gateway_healthy':healthy,'runtime':'Docker','base_url':'http://127.0.0.1:18317/v1',
                'sources':sources,'models':models,'job':dict(JOB),'events':list(EVENTS),
                'session_affinity':SETTINGS['session_affinity'],'apply_error':APPLY_ERROR,'batch':json.loads(json.dumps(BATCH)),
                'official_reserve':dict(SETTINGS['official_reserve']),
                'csrf':CSRF,'checked_at':now()}

class Handler(BaseHTTPRequestHandler):
    def log_message(self,*_):pass
    def allowed(self):return self.headers.get('Host') in ('127.0.0.1:18318','localhost:18318')
    def send(self,data,code=200,mime='application/json; charset=utf-8'):
        body=json.dumps(data,ensure_ascii=False).encode() if not isinstance(data,bytes) else data
        self.send_response(code);self.send_header('Content-Type',mime);self.send_header('Content-Length',str(len(body)))
        self.send_header('Cache-Control','no-store');self.send_header('X-Frame-Options','DENY');self.send_header('X-Content-Type-Options','nosniff')
        self.end_headers();self.wfile.write(body)
    def do_GET(self):
        if not self.allowed():self.send({'error':'无效主机'},403);return
        if self.path=='/health':self.send({'ok':True});return
        if self.path in ('/api/status','/status'):self.send(status());return
        if self.path=='/api/request-logs':self.send(traffic_rows());return
        if self.path in ('/api/device','/device'):self.send(dict(DEVICE));return
        files={'/presentation.js':('presentation.js','application/javascript; charset=utf-8'),'/':('index.html','text/html; charset=utf-8'),'/app.js':('app.js','application/javascript; charset=utf-8'),'/routing.js':('routing.js','application/javascript; charset=utf-8'),'/style.css':('style.css','text/css; charset=utf-8')}
        if self.path in files:
            filename,mime=files[self.path];self.send((ROOT/'web'/filename).read_bytes(),mime=mime);return
        self.send({'error':'不存在'},404)
    def do_POST(self):
        origin=self.headers.get('Origin')
        if not self.allowed() or origin not in ('http://127.0.0.1:18318','http://localhost:18318') or self.headers.get('X-CSRF-Token')!=CSRF:
            self.send({'error':'请刷新控制台后重试'},403);return
        try:
            size=int(self.headers.get('Content-Length',0))
            if size>128000:raise ValueError('提交内容过大')
            data=json.loads(self.rfile.read(size) or b'{}')
            with LOCK:
                if self.path=='/api/key':self.send({'key':KEYS['client']});return
                if self.path=='/api/refresh':job(refresh_quotas)
                elif self.path=='/api/recheck':
                    source=dict(find(data['id']))
                    if not source['enabled']:raise ValueError('请先启用该来源，再检查恢复状态')
                    def recheck():
                        if source['kind']=='api':refresh_quotas()
                        request_json(ROUTER+'/internal/check',{'Authorization':'Bearer '+KEYS['management']},{'id':source['id']},timeout=5)
                        event(source['name']+'：已安排状态检查；恢复探测可能消耗少量额度')
                    job(recheck)
                elif self.path=='/api/sync-models':job(sync_all_models)
                elif self.path=='/api/discover':
                    source=dict(find(data['id']));job(lambda:discover(source))
                elif self.path=='/api/test':
                    source=dict(find(data['id']));model=str(data['model']);job(lambda:run_test(source,model))
                elif self.path=='/api/test-all':
                    model=str(data.get('model','gpt-6.1-sol')).strip()
                    if not model or len(model)>200:raise ValueError('测试模型无效')
                    job(lambda:run_batch(model))
                elif self.path=='/api/test-all/cancel':
                    BATCH_CANCEL.set()
                elif self.path=='/api/toggle':
                    source=find(data['id']);source['enabled']=bool(data['enabled']);render();event(source['name']+(' 已启用' if source['enabled'] else ' 已暂停'))
                elif self.path=='/api/move':
                    sources=SETTINGS['sources'];index=next(i for i,s in enumerate(sources) if s['id']==data['id']);target=index+int(data['direction'])
                    if int(data['direction']) not in (-1,1) or not 0<=target<len(sources):raise ValueError('已到边界')
                    sources[index],sources[target]=sources[target],sources[index];render();event('来源优先顺序已更新')
                elif self.path=='/api/affinity':SETTINGS['session_affinity']=bool(data['enabled']);render();event('会话保持设置已更新')
                elif self.path=='/api/official-reserve':
                    percent=data.get('percent');enabled=data.get('enabled')
                    if not isinstance(enabled,bool) or isinstance(percent,bool) or not isinstance(percent,int) or not 1<=percent<=50:
                        raise ValueError('请设置 1 到 50 的整数预留百分比，并选择启用或关闭')
                    SETTINGS['official_reserve']={'enabled':enabled,'percent':percent}
                    save(DATA/'control-settings.json',SETTINGS)
                    request_json(ROUTER+'/internal/reserve',{'Authorization':'Bearer '+KEYS['management']},{},timeout=5)
                    event('官方额度预留'+('已开启：'+str(percent)+'%' if enabled else '已关闭'))
                elif self.path=='/api/models':
                    raise ValueError('模型目录采用上游原名，请手动点击同步操作')
                elif self.path=='/api/login':
                    slot=data.get('slot')
                    if slot not in ('A','B'):raise ValueError('无效账号槽位')
                    if not LOGIN_LOCK.acquire(blocking=False):raise ValueError('请先完成当前设备授权')
                    threading.Thread(target=login,args=(slot,),daemon=True).start()
                else:self.send({'error':'不存在'},404);return
            self.send({'ok':True})
        except (ValueError,KeyError,TypeError,StopIteration) as error:self.send({'error':str(error) if isinstance(error,ValueError) else '提交格式错误'},400)
        except Exception:self.send({'error':'操作失败，请检查服务状态'},500)

def monitor():
    while True:
        try:
            if JOB_LOCK.acquire(blocking=False):
                JOB.update(busy=True,message='正在自动刷新余额…')
                try:refresh_quotas()
                finally:JOB.update(busy=False,message='就绪');JOB_LOCK.release()
        except Exception:event('定时额度检查失败，将在下次重试')
        time.sleep(300)

def main():
    (DATA/'auths').mkdir(exist_ok=True);(DATA/'logs').mkdir(exist_ok=True)
    with LOCK:render()
    if os.environ.get('PREPARE_ONLY')=='1':return
    if BATCH.get('running'):
        for row in BATCH.get('results',[]):
            if row['status'] in ('running','pending'):row.update(status='skipped',error='控制台重启，测试已中断')
        BATCH.update(running=False,interrupted=True,finished_at=now());batch_summary()
    event('控制台已启动，配置与凭据使用持久化目录')
    threading.Thread(target=monitor,daemon=True).start()
    if not ROUTER:threading.Thread(target=traffic_worker,daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0',18318),Handler).serve_forever()

if __name__=='__main__':
    if len(sys.argv)>2 and sys.argv[1]=='--probe':print(json.dumps(probe_stream(sys.argv[2]),ensure_ascii=True))
    else:main()
