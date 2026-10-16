"""Cancellable execution boundary in front of the existing, prefix-directed gateway.

Only metadata is persisted. Credentials and conversation content stay in memory.
"""
import asyncio
import collections
import contextlib
import datetime
import email.utils
import hashlib
import json
import math
import os
from pathlib import Path
import random
import secrets
import sqlite3
import time

import aiohttp
from aiohttp import web

DATA = Path(os.environ.get('ROUTER_DATA', '/app/private'))
GATEWAY = os.environ.get('GATEWAY_URL', 'http://gateway:18317')
TZ = datetime.timezone(datetime.timedelta(hours=8))


def read(name, default=None):
    try:
        return json.loads((DATA / name).read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return default


def stamp():
    return datetime.datetime.now(TZ).isoformat(timespec='milliseconds')


def reserve_policy(settings):
    value = settings.get('official_reserve', {})
    percent = value.get('percent', 10)
    if isinstance(percent, bool) or not isinstance(percent, (int, float)) or not math.isfinite(percent) or not 1 <= percent <= 50:
        percent = 10
    return {'enabled': value.get('enabled', True) is True, 'percent': percent}


def reserve_status(policy, windows):
    """Independent of circuit health: quota availability never cures other failures."""
    valid = [w for w in windows if isinstance(w.get('used_percent'), (int, float))
             and not isinstance(w['used_percent'], bool) and math.isfinite(w['used_percent']) and 0 <= w['used_percent'] <= 100]
    complete = bool(valid) and len(valid) == len(windows)
    blocked = [w for w in valid if w['used_percent'] >= 100 - policy['percent']] if policy['enabled'] else []
    resets = [w['reset_at'] for w in blocked if isinstance(w.get('reset_at'), (int, float)) and not isinstance(w['reset_at'], bool)]
    return {'reserve_enabled': policy['enabled'], 'reserve_percent': policy['percent'],
            'reserve_known': complete, 'reserve_blocked': bool(blocked),
            'reserve_windows': [w['window'] for w in blocked], 'reserve_reset_at': max(resets or [0])}


def retry_after(value):
    try:
        seconds=float(value)
        return max(1,seconds) if math.isfinite(seconds) else 60
    except (TypeError, ValueError):
        try:
            return max(1, email.utils.parsedate_to_datetime(value).timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return 60


class Failure(Exception):
    def __init__(self, kind, status=502, delay=0, reset=0):
        super().__init__(kind)
        self.kind, self.status, self.delay, self.reset = kind, status, delay, reset


class ProbeSink:
    """Validate exactly the business stream without sending it to a client."""
    async def prepare(self, request): pass
    async def write(self, data): pass
    async def write_eof(self): pass


def classify(status, payload, headers=None, official=False):
    """Never persist upstream messages: only these fixed classification codes."""
    text = json.dumps(payload, ensure_ascii=False).lower()
    error = payload.get('error', {}) if isinstance(payload, dict) else {}
    error = error if isinstance(error, dict) else {}
    if any(w in text for w in ('context_length_exceeded', 'context_window_exceeded', 'invalid_request_error')) and status == 400:
        return Failure('invalid_request', 400)
    if any(w in text for w in ('get_channel_failed', 'model_not_found', 'model_not_supported', '无可用渠道')):
        return Failure('model_unavailable', status if status >= 400 else 503)
    if any(w in text for w in ('usage_limit_reached', 'insufficient_quota', 'quota_exceeded', '余额不足')):
        reset = error.get('resets_at') or error.get('reset_at') or 0
        try:
            reset = float(reset)
            if not math.isfinite(reset): reset=0
            if reset > 1e12: reset /= 1000
        except (ValueError, TypeError): reset = 0
        # Only the provider's dedicated balance endpoint is authoritative for
        # balance. A relay's inference response is model-specific evidence.
        return Failure('quota_wait' if official else 'model_unavailable', 429 if official else 403, reset=reset)
    if status == 401:
        return Failure('auth_required', 401)
    if status == 403:
        return Failure('permission_denied', 403)
    if status == 404:
        return Failure('model_unavailable', 404)
    if status == 429:
        return Failure('rate_limited', 429, delay=retry_after((headers or {}).get('Retry-After')))
    if status in (400, 413, 422):
        return Failure('invalid_request', status)
    return Failure('upstream_error', status if status >= 400 else 502)


class State:
    def __init__(self, directory=DATA):
        self.directory = directory
        path = directory / 'routing-state.json'
        try: self.items = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError): self.items = {}
        for item in self.items.values():
            item['probe_in_progress'] = False
            # Repair stale cooldowns left by an earlier failed recovery probe.
            if item.get('state', 'closed') == 'closed' and item.get('until', 0):
                if item.get('consecutive', 0) > 0:
                    item['state'] = 'open'
                else:
                    item['until'] = 0
        self.active = set()
        self.history = collections.defaultdict(lambda: collections.deque(maxlen=10))

    def persist(self):
        path = self.directory / 'routing-state.json'
        tmp = path.with_suffix('.tmp')
        tmp.write_text(json.dumps(self.items, ensure_ascii=False, indent=2), encoding='utf-8')
        os.replace(tmp, path)

    def key(self, ident, model=''):
        return ident + ('/' + model if model else '')

    def get(self, key):
        return self.items.get(key, {})

    def acquire(self, ident, model):
        """Half-open permits are per scope and never shared with normal traffic."""
        permits = []
        for key in (ident, self.key(ident, model)):
            item = self.get(key)
            state = item.get('state', 'closed')
            if state in ('quota_wait', 'balance_wait', 'model_balance_wait', 'auth_required'):
                return None, state
            if item.get('quota_blocked'): return None, 'quota_wait'
            if item.get('reserve_enabled'):
                if item.get('reserve_blocked'): return None, 'reserve_wait'
                if not item.get('reserve_known') or item.get('query_error'): return None, 'reserve_unknown'
            if state == 'closed': continue
            if item.get('until', 0) > time.time(): return None, item.get('reason', state)
            if key in self.active: return None, 'half_open_busy'
            permits.append(key)
        self.active.update(permits)
        return permits, ''

    def release(self, permits):
        self.active.difference_update(permits or [])

    def success(self, ident, model, permits=(), versions=None):
        for key in (ident, self.key(ident, model)):
            item = self.items.setdefault(key, {})
            if versions is not None and item.get('revision',0)!=versions.get(key,0): continue
            self.history[key].append(False)
            item['consecutive'] = 0
            item['last_success_at'] = stamp()
            # A model success only clears its own scope; source recovery needs its permit.
            if item.get('state', 'closed') == 'closed':
                item.update(reason='', until=0,retry_check_at=0)
                continue
            if key not in permits: continue
            item['successes'] = item.get('successes', 0) + 1
            if item['successes'] >= 2:
                item.update(state='closed', checked_at=stamp(), consecutive=0, reason='', until=0, level=0, successes=0,
                            reclaim_after=time.time()+item.get('reclaim_delay',300), reclaim_verified=False)
                self.history[key].clear()
            else:
                item.update(state='half_open', until=time.time() + 15, checked_at=stamp())
        self.persist()

    def failure(self, ident, model, failure):
        if failure.kind in ('invalid_request', 'client_cancelled', 'budget_exhausted'): return
        model_scope = failure.kind in ('model_unavailable', 'permission_denied', 'model_balance_wait')
        key = self.key(ident, model if model_scope else '')
        item = self.items.setdefault(key, {})
        item['revision']=item.get('revision',0)+1
        if (item.get('reclaim_after') and not item.get('reclaim_verified')) or item.get('reclaim_active_until',0)>time.time():
            item.update(state='open',reclaim_delay=min(1800,item.get('reclaim_delay',300)*2),reclaim_after=0,reclaim_verified=False)
        item['reclaim_active_until']=0
        history = self.history[key]
        history.append(True)
        count = item.get('consecutive', 0) + 1
        item.update(reason=failure.kind, checked_at=stamp(), consecutive=count, probe_model=model, successes=0)
        if failure.kind in ('quota_wait', 'balance_wait', 'model_balance_wait', 'auth_required'):
            item.update(state=failure.kind, reset_at=failure.reset, until=0, next_check=time.time() + 300)
        elif failure.kind == 'rate_limited':
            item.update(state='open', until=time.time() + failure.delay)
        elif model_scope or count >= 3 or (len(history) == 10 and sum(history) >= 6) or item.get('state') in ('open', 'half_open'):
            level = item.get('level', -1) + 1
            delay = (30, 60, 120, 300, 600, 900)[min(level, 5)] * random.uniform(.9, 1.1)
            item.update(state='open', until=time.time() + delay, level=level)
        else:
            item.update(state='closed', until=0,retry_check_at=time.time()+30)
        self.persist()


class Router:
    def __init__(self):
        self.keys = read('secrets.json', {})
        self.state = State()
        self.sessions = collections.OrderedDict()
        self.responses = collections.OrderedDict()
        self.client = None
        self.checking = set()
        self.quota_locks = collections.defaultdict(asyncio.Lock)
        self.clock_scale = float(os.environ.get('ROUTER_CLOCK_SCALE', '1'))
        self.migrate_logs()

    def migrate_logs(self):
        """Copy legacy metadata once; retain the old database as recovery evidence."""
        with contextlib.closing(self.db()) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS migrations (name TEXT PRIMARY KEY)')
            if db.execute("SELECT 1 FROM migrations WHERE name='legacy'").fetchone(): return
            path = DATA / 'request-logs.sqlite'
            if path.exists():
                db.execute('ATTACH DATABASE ? AS legacy', (str(path),))
                db.execute('''INSERT OR IGNORE INTO requests SELECT request_key,timestamp,endpoint,method,model,source,
                    status_code,latency_ms,input_tokens,output_tokens,cached_tokens,total_tokens,attempts,
                    CASE WHEN status_code>=400 THEN 'legacy_failure' ELSE 'success' END,'[]' FROM legacy.requests''')
            db.execute("INSERT INTO migrations VALUES('legacy')")

    def settings(self):
        return read('control-settings.json', {'sources': [], 'session_affinity': True})

    def default_probe_model(self, source):
        """Use the configured default model for recovery, never the failing model."""
        models = source.get('models', [])
        aliases = [m.get('alias') for m in models if isinstance(m, dict) and m.get('alias')]
        if source.get('kind') == 'oauth':
            aliases = read('official-models.json', {}).get(source['id'], aliases)
        return 'gpt-6.1-sol' if 'gpt-6.1-sol' in aliases else (aliases[0] if aliases else 'gpt-6.1-sol')

    def apply_reserve(self, source):
        if source['kind'] != 'oauth': return
        item = self.state.items.setdefault(source['id'], {})
        value = reserve_status(reserve_policy(self.settings()), item.get('windows', []))
        # Unknown data cannot release an existing reserve hold.
        if value['reserve_enabled'] and not value['reserve_known']:
            value['reserve_blocked'] = item.get('reserve_blocked', False)
            value['reserve_windows'] = item.get('reserve_windows', [])
            value['reserve_reset_at'] = item.get('reserve_reset_at', 0)
        item.update(value)

    async def refresh_official(self, source, force=False):
        async with self.quota_locks[source['id']]:
            item = self.state.items.setdefault(source['id'], {})
            # Concurrent requests share one read-only quota query for 15 seconds.
            if not force and time.time() - item.get('quota_attempt_at', 0) < 15:
                self.apply_reserve(source)
                return
            interval = 60 if reserve_policy(self.settings())['enabled'] else 300
            try: result = await self.quota(source)
            except (aiohttp.ClientError, TimeoutError, ValueError, TypeError): result = {'query_error': 'usage_query_failed'}
            result.setdefault('next_check', time.time() + interval)
            result['quota_attempt_at'] = time.time()
            item.update(result)
            self.apply_reserve(source)
            self.state.persist()

    def db(self):
        db = sqlite3.connect(DATA / 'routing-requests.sqlite', timeout=10)
        db.execute('''CREATE TABLE IF NOT EXISTS requests (
            request_key TEXT PRIMARY KEY,timestamp TEXT,endpoint TEXT,method TEXT,model TEXT,
            source TEXT,status_code INTEGER,latency_ms REAL,input_tokens INTEGER,output_tokens INTEGER,
            cached_tokens INTEGER,total_tokens INTEGER,attempts INTEGER,outcome TEXT,trace TEXT)''')
        return db

    def log(self, ident, started, endpoint, model, trace, status, outcome, usage):
        # No upstream URLs, raw error bodies, session IDs, prompts or tokens.
        usage = usage if isinstance(usage,dict) else {}
        details = usage.get('input_tokens_details') or usage.get('prompt_tokens_details') or {}
        if not isinstance(details,dict): details={}
        with contextlib.closing(self.db()) as db, db:
            db.execute('INSERT INTO requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                ident, stamp(), endpoint, 'POST', model,
                ' → '.join(t['source'] for t in trace if t['action'] == 'attempt'), status,
                round((time.monotonic() - started) * 1000, 2),
                usage.get('input_tokens', usage.get('prompt_tokens')), usage.get('output_tokens', usage.get('completion_tokens')),
                details.get('cached_tokens'), usage.get('total_tokens'),
                sum(t['action'] == 'attempt' for t in trace), outcome, json.dumps(trace, ensure_ascii=False)))
            db.execute('DELETE FROM requests WHERE request_key IN (SELECT request_key FROM requests ORDER BY timestamp DESC LIMIT -1 OFFSET 5000)')

    def candidates(self, body, headers):
        settings = self.settings()
        model = str(body.get('model', ''))
        sources = settings['sources']
        explicit = next((s for s in sources if model.startswith(s['id'] + '/')), None)
        if explicit:
            model = model[len(explicit['id']) + 1:]
            sources = [explicit]
        else:
            catalogs = read('official-models.json', {})
            sources = [s for s in sources if (model in catalogs.get(s['id'], []) if s['kind'] == 'oauth'
                       else model in [m['alias'] for m in s.get('models', [])])]
        # Hash affinity identifiers; bounded, one-hour idle expiration.
        session = headers.get('session_id') or headers.get('x-session-id') or headers.get('conversation_id') or body.get('prompt_cache_key')
        session = hashlib.sha256(str(session).encode()).hexdigest() if session else None
        affinity = self.sessions.get(session) if session and settings.get('session_affinity') else None
        if affinity and time.time() - affinity[1] < 3600 and not explicit:
            # Keep a session sticky until a higher-priority official account has
            # recovered. Reclaim happens only at the next request boundary; an
            # in-flight stream is never moved between providers.
            current = next((s for s in sources if s['id'] == affinity[0]), None)
            recovered_official = []
            for candidate in sources:
                if candidate['kind'] != 'oauth' or not candidate.get('enabled') or not candidate.get('authorized', True):
                    continue
                item = self.state.get(candidate['id'])
                if item.get('state', 'closed') != 'closed' or item.get('consecutive', 0):
                    continue
                if item.get('reserve_blocked') or item.get('quota_blocked') or item.get('query_error'):
                    continue
                if reserve_policy(settings)['enabled'] and not item.get('reserve_known'): continue
                if self.state.get(self.state.key(candidate['id'],model)).get('state','closed')!='closed': continue
                recovered_official.append(candidate)
            if current and current['kind'] != 'oauth' and recovered_official:
                recovered_ids = {s['id'] for s in recovered_official}
                sources.sort(key=lambda s: (0 if s['id'] in recovered_ids else 1,
                                            0 if s['id'] == affinity[0] else 1))
            else:
                reclaim_ids=set()
                held_since=affinity[2] if len(affinity)>2 else affinity[1]
                if current and current['kind']=='api' and time.time()-held_since>=1800:
                    for candidate in sources[:sources.index(current)]:
                        if candidate['kind']!='api' or not candidate.get('enabled'): continue
                        scopes=[self.state.get(candidate['id']),self.state.get(self.state.key(candidate['id'],model))]
                        if not scopes[1].get('last_success_at'): continue
                        if any(v.get('state','closed')!='closed' or v.get('consecutive',0) or
                               (v.get('reclaim_after') and (v['reclaim_after']>time.time() or not v.get('reclaim_verified'))) for v in scopes): continue
                        if scopes[0].get('probe_model') and scopes[0]['probe_model']!=model and scopes[0].get('reclaim_after'): continue
                        reclaim_ids.add(candidate['id'])
                sources.sort(key=lambda s: (0 if s['id'] in reclaim_ids else 1, s['id'] != affinity[0]))
        # previous_response_id may only go back to its original source in this process.
        previous = body.get('previous_response_id')
        if previous:
            owner = self.responses.get(previous)
            if not owner or time.time() - owner[1] > 3600:
                raise Failure('continuation_requires_full_history', 409)
            sources = [s for s in sources if s['id'] == owner[0]]
        elif not sources:
            raise Failure('model_unavailable',404)
        return model, sources, session, bool(previous)

    def remember(self, source, session, response_id, started=None):
        for mapping, key in ((self.sessions, session), (self.responses, response_id)):
            if key:
                old=mapping.get(key)
                if mapping is self.sessions and old and len(old)>3 and started is not None and old[3]>started: continue
                held_since=old[2] if mapping is self.sessions and old and old[0]==source and len(old)>2 and time.time()-old[1]<3600 else time.time()
                mapping[key] = (source, time.time(), held_since, started or time.monotonic())
                mapping.move_to_end(key)
                while len(mapping) > 5000: mapping.popitem(last=False)

    async def request(self, request):
        if not secrets.compare_digest(request.headers.get('Authorization', ''), 'Bearer ' + self.keys['client']):
            if request.method == 'POST': self.log(secrets.token_hex(16), time.monotonic(), request.path, '', [], 401, 'invalid_api_key', {})
            return web.json_response({'error': {'code': 'invalid_api_key'}}, status=401)
        if request.method == 'GET' and request.path == '/v1/models':
            async with self.client.get(GATEWAY + '/v1/models', headers={'Authorization': 'Bearer ' + self.keys['client']}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return web.Response(body=await resp.read(), status=resp.status, content_type='application/json')
        if request.method != 'POST' or request.path not in ('/v1/responses', '/v1/chat/completions'):
            return web.json_response({'error': {'code': 'unsupported_endpoint'}}, status=404)
        try:
            body = await request.json()
            if not isinstance(body, dict) or not isinstance(body.get('model'), str): raise ValueError()
            if body.get('reasoning') is not None and not isinstance(body['reasoning'], dict): raise ValueError()
            if 'n' in body and (type(body['n']) is not int or body['n']<1): raise ValueError()
            if body.get('previous_response_id') is not None and not isinstance(body['previous_response_id'],str): raise ValueError()
            if body.get('background'): raise Failure('background_not_supported', 400)
        except Failure as failure:
            return web.json_response({'error': {'code': failure.kind}}, status=failure.status)
        except (ValueError, TypeError):
            self.log(secrets.token_hex(16), time.monotonic(), request.path, '', [], 400, 'invalid_request', {})
            return web.json_response({'error': {'code': 'invalid_request'}}, status=400)
        started = time.monotonic()
        trace, usage, result, outcome, status = [], {}, None, 'all_unavailable', 503
        model = body['model']
        request_id = secrets.token_hex(16)
        try:
            model, sources, session, continuation = self.candidates(body, request.headers)
            effort = (body.get('reasoning') or {}).get('effort') or body.get('reasoning_effort')
            long = effort in ('high', 'xhigh', 'max', 'ultra')
            budget = started + (600 if long else 180) * self.clock_scale
            for source in sources:
                row = {'source': source['name'], 'action': 'skip', 'reason': ''}
                trace.append(row)
                quota = read('quota-state.json', {}).get('quotas', {}).get(source['name'], {})
                if not source.get('enabled'):
                    row['reason'] = 'manual_paused'; continue
                if quota.get('excluded'):
                    row['reason'] = 'balance_wait'; continue
                if source['kind'] == 'oauth':
                    self.apply_reserve(source)
                    if reserve_policy(self.settings())['enabled']:
                        try:
                            async with asyncio.timeout(max(.001, budget - time.monotonic())):
                                await self.refresh_official(source)
                        except TimeoutError: raise Failure('budget_exhausted', 504)
                permits, reason = self.state.acquire(source['id'], model)
                if permits is None:
                    row['reason'] = reason; continue
                if time.monotonic() >= budget:
                    self.state.release(permits)
                    raise Failure('budget_exhausted', 504)
                row.update(action='attempt', reason='in_progress')
                old=self.sessions.get(session) if session else None
                ordered=[s['id'] for s in self.settings()['sources']]
                if old and time.time()-old[1]<3600 and old[0]!=source['id'] and body['model']==model and not continuation and old[0] in ordered and ordered.index(source['id'])<ordered.index(old[0]):
                    row['selection']='official_reclaim' if source['kind']=='oauth' else 'relay_reclaim'
                    if source['kind']=='api':
                        for scope in (source['id'],self.state.key(source['id'],model)):
                            self.state.items.setdefault(scope,{})['reclaim_active_until']=time.time()+1800
                versions={key:self.state.get(key).get('revision',0) for key in (source['id'],self.state.key(source['id'],model))}
                tick = time.monotonic()
                try:
                    result, usage, response_id = await self.attempt(request, body, source, model, long, budget)
                    status, outcome = 200, 'success'
                    self.state.success(source['id'], model, permits, versions)
                    self.remember(source['id'], session, response_id, started)
                    row.update(reason='success', seconds=round(time.monotonic() - tick, 2))
                    return result
                except Failure as failure:
                    row.update(reason=failure.kind, seconds=round(time.monotonic() - tick, 2))
                    self.state.failure(source['id'], model, failure)
                    status, outcome = failure.status, failure.kind
                    if getattr(failure, 'committed', False):
                        # attempt has already sent an SSE error and closed the stream.
                        status = 200
                        return failure.response
                    if continuation or failure.kind == 'invalid_request': break
                finally:
                    self.state.release(permits)
            retry = min([max(1, int((v.get('until') or v.get('next_check') or 0) - time.time())) for k, v in self.state.items.items() if k.split('/')[0] in {s['id'] for s in sources} and (v.get('until') or v.get('next_check') or 0) > time.time()] or [60])
            return web.json_response({'error': {'code': outcome, 'message': '可用来源均未完成请求；请查看控制面板的切换记录。', 'request_id': request_id, 'attempted': any(t['action']=='attempt' for t in trace)}}, status=status, headers={'Retry-After': str(retry)})
        except asyncio.CancelledError:
            status, outcome = 499, 'client_cancelled'
            if trace and trace[-1].get('reason') == 'in_progress': trace[-1]['reason'] = outcome
            raise
        except Failure as failure:
            status, outcome = failure.status, failure.kind
            return web.json_response({'error': {'code': outcome}}, status=status)
        except Exception:
            status,outcome=500,'internal_router_error'
            return web.json_response({'error':{'code':outcome,'request_id':request_id}},status=status)
        finally:
            self.log(request_id, started, request.path, model, trace, status, outcome, usage)

    async def attempt(self, downstream, body, source, model, long, budget, probe=False):
        directed = dict(body, model=source['id'] + '/' + model)
        headers = {'Authorization': 'Bearer ' + self.keys['client'], 'Content-Type': 'application/json'}
        # Preserve protocol controls, but not an affinity key shared across directed lanes.
        for name in ('OpenAI-Beta', 'OpenAI-Version'):
            if downstream.headers.get(name): headers[name] = downstream.headers[name]
        response = None
        first = (120 if long else 60) * self.clock_scale
        progress_limit, idle_limit = (360 * self.clock_scale, 300 * self.clock_scale) if long else (180 * self.clock_scale, 120 * self.clock_scale)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10)
        precommit_deadline = min(budget, time.monotonic() + first)
        try:
            try:
                async with asyncio.timeout_at(asyncio.get_running_loop().time() + max(.001, precommit_deadline - time.monotonic())):
                    upstream = await self.client.post(GATEWAY + ('/v1/responses' if probe else downstream.path), json=directed, headers=headers, timeout=timeout)
            except TimeoutError: raise Failure('first_event_timeout', 504)
            async with upstream:
                if upstream.status >= 400:
                    try:
                        async with asyncio.timeout(10): raw = await upstream.content.read(65536)
                        error = json.loads(raw)
                    except (ValueError, TimeoutError): error = {}
                    raise classify(upstream.status, error, upstream.headers, source['kind'] == 'oauth')
                if not body.get('stream'):
                    try:
                        async with asyncio.timeout(max(.001, min(budget, time.monotonic() + (600 if long else 180)) - time.monotonic())):
                            raw = await upstream.read()
                        obj = json.loads(raw)
                    except TimeoutError: raise Failure('response_timeout', 504)
                    except ValueError: raise Failure('invalid_response', 502)
                    if not isinstance(obj, dict) or obj.get('error') or obj.get('status') in ('failed', 'incomplete'):
                        raise classify(502, obj if isinstance(obj, dict) else {}, official=source['kind']=='oauth')
                    if downstream.path.endswith('/responses') and obj.get('status') != 'completed': raise Failure('invalid_response', 502)
                    if downstream.path.endswith('/completions') and not obj.get('choices'): raise Failure('invalid_response', 502)
                    return web.Response(body=raw, content_type='application/json'), obj.get('usage', {}), obj.get('id')
                if 'text/event-stream' not in upstream.headers.get('Content-Type', ''):
                    raise Failure('invalid_response', 502)
                buffer, pending, valid, committed, complete = b'', [], False, False, False
                began, last_progress = time.monotonic(), time.monotonic()
                usage, response_id = {}, None
                finished_choices=set()
                # Bound precommit buffering; heartbeat bytes never reset clocks.
                buffered = 0
                while True:
                    now = time.monotonic()
                    deadline = last_progress + idle_limit if committed else min(budget, began + progress_limit, precommit_deadline if not valid else float('inf'))
                    try:
                        async with asyncio.timeout(max(.001, deadline - now)):
                            chunk = await upstream.content.readany()
                    except TimeoutError:
                        raise Failure('stream_idle_timeout' if committed else 'no_progress_timeout' if valid else 'first_event_timeout', 504)
                    if not chunk: break
                    buffer += chunk
                    if len(buffer) > 2 * 1024 * 1024: raise Failure('invalid_response', 502)
                    # Normalize SSE newlines. Parse whole events, never forward partial JSON.
                    buffer = buffer.replace(b'\r\n', b'\n')
                    while b'\n\n' in buffer:
                        frame, buffer = buffer.split(b'\n\n', 1)
                        data = b'\n'.join(line[5:].lstrip() for line in frame.split(b'\n') if line.startswith(b'data:'))
                        if not data: continue
                        if data == b'[DONE]':
                            if downstream.path.endswith('/completions') and complete: pass
                            elif not complete: raise Failure('empty_stream', 502)
                            continue
                        try: obj = json.loads(data)
                        except ValueError: raise Failure('invalid_response', 502)
                        if not isinstance(obj, dict): raise Failure('invalid_response', 502)
                        typ = obj.get('type', '')
                        if not isinstance(typ,str): raise Failure('invalid_response',502)
                        if typ in ('error', 'response.failed', 'response.incomplete') or obj.get('error'):
                            raise classify(502, obj.get('response') or obj, official=source['kind'] == 'oauth')
                        event_valid=typ.startswith('response.') if not downstream.path.endswith('/completions') else obj.get('object')=='chat.completion.chunk'
                        if not event_valid: raise Failure('invalid_response',502)
                        valid=True
                        resp = obj.get('response') or {}
                        if not isinstance(resp,dict): raise Failure('invalid_response',502)
                        if typ=='response.completed' and resp.get('status') in ('failed','incomplete'):
                            raise classify(502,resp,official=source['kind']=='oauth')
                        response_id = resp.get('id') or obj.get('id') or response_id
                        usage = resp.get('usage') or obj.get('usage') or usage
                        choices = obj.get('choices') or []
                        if not isinstance(choices,list) or any(not isinstance(c,dict) or not isinstance(c.get('delta',{}),dict) for c in choices): raise Failure('invalid_response',502)
                        for choice in choices:
                            if choice.get('finish_reason'):
                                index=choice.get('index',0)
                                if type(index) is not int: raise Failure('invalid_response',502)
                                finished_choices.add(index)
                        progress = bool(typ.endswith('.delta') and obj.get('delta'))
                        item = obj.get('item') or {}
                        if not isinstance(item,dict): raise Failure('invalid_response',502)
                        progress |= typ == 'response.output_item.added' and item.get('type') in ('function_call', 'custom_tool_call')
                        progress |= any(c.get('delta', {}).get('content') or c.get('delta', {}).get('tool_calls') or c.get('delta', {}).get('reasoning_content') for c in choices)
                        complete = typ == 'response.completed' or len(finished_choices)>=body.get('n',1) or complete
                        frame = frame + b'\n\n'
                        if not committed:
                            pending.append(frame); buffered += len(frame)
                            if buffered > 2 * 1024 * 1024: raise Failure('invalid_response', 502)
                            if progress or complete:
                                # Once any output or tool event reaches the client, replay is forbidden.
                                response = ProbeSink() if probe else web.StreamResponse(status=200, headers={'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})
                                await response.prepare(downstream)
                                committed = True
                                for part in pending: await response.write(part)
                                pending.clear()
                        else:
                            await response.write(frame)
                        if progress or complete: last_progress = time.monotonic()
                        if typ == 'response.completed' or (downstream.path.endswith('/completions') and complete):
                            if downstream.path.endswith('/completions'): await response.write(b'data: [DONE]\n\n')
                            await response.write_eof()
                            return response, usage, response_id
                if not complete: raise Failure('stream_interrupted' if committed else 'empty_stream', 502)
                if response:
                    if downstream.path.endswith('/completions'): await response.write(b'data: [DONE]\n\n')
                    await response.write_eof()
                    return response, usage, response_id
                raise Failure('empty_stream', 502)
        except (aiohttp.ClientError, ConnectionError) as exc:
            # Client disconnect cancellation is handled by aiohttp handler_cancellation.
            if downstream.transport is None or downstream.transport.is_closing():
                raise asyncio.CancelledError() from exc
            failure = Failure('connection_failed', 502)
            if response is not None: failure.committed, failure.response = True, response
            raise failure from exc
        except Failure as failure:
            if response is not None:
                failure.committed, failure.response = True, response
                with contextlib.suppress(ConnectionError, RuntimeError):
                    await response.write(('event: error\ndata: ' + json.dumps({'type': 'error', 'error': {'code': failure.kind, 'message': '上游流中断，请在当前会话中重试。'}}) + '\n\n').encode())
                    await response.write_eof()
            raise

    async def quota(self, source):
        """Read-only official account usage. Unknown results never unblock quotas."""
        auth = read('auths/plus-' + source['slot'] + '.json', {})
        if not auth.get('access_token'): return {'state': 'auth_required', 'reason': 'auth_required'}
        headers = {'Authorization': 'Bearer ' + auth['access_token'], 'ChatGPT-Account-Id': auth.get('account_id', ''), 'User-Agent': 'codex_cli_rs/0.114.0'}
        async with self.client.get('https://chatgpt.com/backend-api/wham/usage', headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as response:
            if response.status != 200:
                return {'query_error': 'usage_http_' + str(response.status)}
            obj = await response.json()
        limit = obj.get('rate_limit') if isinstance(obj,dict) else None
        if not isinstance(limit, dict) or 'allowed' not in limit: return {'query_error': 'usage_schema_unknown'}
        windows = []
        for name in ('primary_window', 'secondary_window'):
            window = limit.get(name)
            if isinstance(window, dict):
                windows.append({'window': name, 'used_percent': window.get('used_percent'), 'reset_at': window.get('reset_at'), 'limit_window_seconds': window.get('limit_window_seconds')})
        blocked = [w for w in windows if isinstance(w.get('used_percent'), (int, float)) and w['used_percent'] >= 100]
        allowed = limit.get('allowed') is True and not limit.get('limit_reached') and not blocked
        policy = reserve_policy(self.settings())
        interval = 60 if policy['enabled'] else 300
        reserve = reserve_status(policy, windows)
        if policy['enabled'] and not reserve['reserve_known']: return {'query_error': 'usage_schema_unknown'}
        result = {'windows': windows, 'usage_checked_at': stamp(), 'next_check': time.time() + interval, 'query_error': '', 'quota_blocked':not allowed, **reserve}
        if allowed:
            current = self.state.get(source['id'])
            # A usage endpoint cannot cure unrelated network/permission/auth errors.
            if current.get('state')=='quota_wait':
                result.update(state='closed', reason='', reset_at=0, until=0, consecutive=0)
            elif current.get('state')=='auth_required':
                result.update(state='open',until=0)
        else:
            result['reset_at']=max([w.get('reset_at') or 0 for w in blocked] or [0])
            if self.state.get(source['id']).get('state','closed') in ('closed','quota_wait'):
                result.update(state='quota_wait', reason='quota_wait', until=0)
            future = [w['reset_at'] for w in blocked if isinstance(w.get('reset_at'), (int, float)) and w['reset_at'] > time.time()]
            if future: result['next_check'] = min(time.time() + interval, max(future) + 2)
        if reserve['reserve_blocked'] and reserve['reserve_reset_at'] > time.time():
            result['next_check'] = min(result['next_check'], reserve['reserve_reset_at'] + 2)
        return result

    async def check(self, ident, manual=False):
        if ident in self.checking: return
        source = next((s for s in self.settings()['sources'] if s['id'] == ident), None)
        if not source or not source.get('enabled'): return
        self.checking.add(ident)
        try:
            if source['kind'] == 'oauth':
                await self.refresh_official(source, force=manual)
            elif manual and self.state.get(ident).get('state') == 'auth_required':
                self.state.items[ident].update(state='open', until=0)
            elif manual and self.state.get(ident).get('state', 'closed') == 'closed' and self.state.get(ident).get('consecutive', 0) > 0:
                self.state.items[ident].update(state='open', until=0)
            # Model probes only run for due breakers. Manual checks can bring the next check forward.
            for key, item in list(self.state.items.items()):
                observing=source['kind']=='api' and item.get('state','closed')=='closed' and item.get('reclaim_after',0)>0 and not item.get('reclaim_verified')
                unconfirmed=item.get('state','closed')=='closed' and item.get('consecutive',0)>0 and item.get('retry_check_at',0)<=time.time()
                if key.split('/')[0] != ident or (item.get('state') not in ('open', 'half_open') and not observing and not unconfirmed): continue
                if observing and item['reclaim_after']>time.time(): continue
                if manual: item['until'] = 0
                if item.get('until', 0) > time.time(): continue
                model = self.default_probe_model(source) if item.get('reason') in ('model_balance_wait', 'balance_wait') else item.get('probe_model')
                if not model: continue
                permits, _ = self.state.acquire(ident, model)
                if permits is None: continue
                probing = [self.state.items[scope] for scope in permits] or [item]
                versions={scope:self.state.get(scope).get('revision',0) for scope in (ident,self.state.key(ident,model))}
                for scope in probing:
                    scope.update(probe_in_progress=True, probe_started_at=time.time())
                self.state.persist()
                try:
                    body = {'model': ident + '/' + model, 'input': 'Reply OK.', 'stream': True, 'reasoning': {'effort': 'low'}}
                    # Internal request traverses exactly the same protocol validation as user traffic.
                    async with self.client.post('http://127.0.0.1:18319/internal/probe', json=body,
                        headers={'Authorization': 'Bearer ' + self.keys['management']}, timeout=aiohttp.ClientTimeout(total=65)) as response:
                        result = await response.json()
                    for scope in probing:
                        scope['last_probe'] = {'passed':bool(result.get('ok')), 'reason':result.get('reason','success' if result.get('ok') else 'upstream_error'), 'http':result.get('status'), 'model':model}
                    if result.get('ok'):
                        self.state.success(ident, model, permits, versions)
                        if observing and item.get('revision',0)==versions.get(key,0): item['reclaim_verified']=True
                        if unconfirmed and item.get('revision',0)==versions.get(key,0): item.update(state='half_open',successes=1,until=time.time()+15)
                    else: self.state.failure(ident, model, Failure(result.get('reason', 'upstream_error'), result.get('status',502), delay=result.get('delay',0), reset=result.get('reset',0)))
                except (aiohttp.ClientError, TimeoutError, ValueError):
                    for scope in probing: scope['last_probe'] = {'passed':False,'reason':'probe_timeout','http':504,'model':model}
                    self.state.failure(ident, model, Failure('probe_timeout', 504))
                finally:
                    for scope in probing:
                        scope.update(probe_in_progress=False, probe_finished_at=time.time())
                    self.state.release(permits)
                    self.state.persist()
        finally: self.checking.discard(ident)

    async def internal(self, request):
        if not secrets.compare_digest(request.headers.get('Authorization', ''), 'Bearer ' + self.keys['management']):
            return web.json_response({'error': 'unauthorized'}, status=401)
        body = await request.json()
        if request.path == '/internal/reserve':
            for source in self.settings()['sources']:
                if source['kind'] != 'oauth': continue
                self.apply_reserve(source)
                self.state.items[source['id']]['next_check'] = 0
            self.state.persist()
            return web.json_response({'ok': True})
        if request.path == '/internal/check':
            ident = str(body.get('id', ''))
            asyncio.create_task(self.check(ident, True))
            return web.json_response({'ok': True})
        # Recovery follows the same parser, deadlines and error classification.
        model = str(body.get('model',''))
        source = next((s for s in self.settings()['sources'] if model.startswith(s['id']+'/')),None)
        if not source or not source.get('enabled'):
            return web.json_response({'ok':False,'reason':'manual_paused','status':403})
        quota = read('quota-state.json',{}).get('quotas',{}).get(source['name'],{})
        if quota.get('excluded'):
            return web.json_response({'ok':False,'reason':'balance_wait','status':403})
        if source['kind']=='oauth':
            await self.refresh_official(source)
            item=self.state.get(source['id'])
            if item.get('quota_blocked') or item.get('state')=='quota_wait':
                return web.json_response({'ok':False,'reason':'quota_wait','status':429})
            if item.get('reserve_enabled') and (item.get('reserve_blocked') or not item.get('reserve_known') or item.get('query_error')):
                return web.json_response({'ok':False,'reason':'reserve_wait' if item.get('reserve_blocked') else 'reserve_unknown','status':503})
        try:
            async with asyncio.timeout(60):
                await self.attempt(request,body,source,model[len(source['id'])+1:],False,time.monotonic()+60,probe=True)
            return web.json_response({'ok':True,'status':200})
        except Failure as failure:
            return web.json_response({'ok':False,'reason':failure.kind,'status':failure.status,'delay':failure.delay,'reset':failure.reset})
        except (aiohttp.ClientError,TimeoutError):
            return web.json_response({'ok':False,'reason':'probe_timeout','status':504})

    async def monitor(self):
        while True:
            now = time.time()
            for source in self.settings()['sources']:
                ident = source['id']
                self.apply_reserve(source)
                if source['kind'] == 'api' and self.state.get(ident).get('state') == 'balance_wait':
                    quota = read('quota-state.json', {}).get('quotas', {}).get(source['name'], {})
                    # Old positive balances cannot erase a newer upstream quota rejection.
                    if quota.get('status') == 'available' and not quota.get('excluded') and quota.get('checked_at', '') > self.state.get(ident).get('checked_at', ''):
                        self.state.items[ident].update(state='open', until=0, consecutive=0, successes=0)
                        self.state.persist()
                due = source['kind'] == 'oauth' and self.state.get(ident).get('next_check', 0) <= now
                due |= any(k.split('/')[0] == ident and v.get('state') in ('open', 'half_open') and v.get('until', 0) <= now for k, v in self.state.items.items())
                due |= source['kind']=='api' and any(k.split('/')[0]==ident and v.get('state','closed')=='closed' and v.get('reclaim_after',0)>0 and v['reclaim_after']<=now and not v.get('reclaim_verified') for k,v in self.state.items.items())
                due |= any(k.split('/')[0]==ident and v.get('state','closed')=='closed' and v.get('consecutive',0)>0 and v.get('retry_check_at',0)<=now for k,v in self.state.items.items())
                due |= source['kind']=='api' and any(k.split('/')[0]==ident and v.get('state')=='model_balance_wait' and v.get('next_check',0)<=now for k,v in self.state.items.items())
                if due and ident not in self.checking:
                    asyncio.create_task(self.check(ident))
            await asyncio.sleep(5)

    async def lifecycle(self, app):
        self.client = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100), trust_env=False)
        worker = asyncio.create_task(self.monitor())
        yield
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError): await worker
        await self.client.close()

    def app(self):
        app = web.Application(client_max_size=32 * 1024 * 1024)
        app.cleanup_ctx.append(self.lifecycle)
        app.router.add_get('/health', lambda _: web.json_response({'ok': True}))
        app.router.add_post('/internal/{action}', self.internal)
        app.router.add_route('*', '/{path:.*}', self.request)
        return app


if __name__ == '__main__':
    web.run_app(Router().app(), host='0.0.0.0', port=18319, handler_cancellation=True, access_log=None)
