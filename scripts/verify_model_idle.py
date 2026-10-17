"""Synthetic idle model fault expiration checks; no provider calls."""
import importlib.util
import json
import tempfile
import time
from pathlib import Path

spec = importlib.util.spec_from_file_location('idle_router', Path(__file__).with_name('router.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
checks = []
def check(name, condition):
    assert condition, name
    checks.append(name)

with tempfile.TemporaryDirectory() as folder:
    state = m.State(Path(folder))
    state.mark_client_request('a', 'broken')
    state.failure('a', 'broken', m.Failure('model_unavailable', 403))
    check('recent business attempt retains model fault', not state.expire_idle_model_errors())
    state.items['a/broken']['last_client_request_at'] = time.time() - 3601
    anchor = state.items['a/broken']['last_client_request_at']
    state.failure('a', 'broken', m.Failure('model_unavailable', 403))
    check('background failure does not renew business activity', state.items['a/broken']['last_client_request_at'] == anchor)
    state.items['a/broken']['probe_in_progress'] = True
    check('in flight recovery is not deleted', not state.expire_idle_model_errors())
    state.items['a/broken']['probe_in_progress'] = False
    state.items['a'] = {'state': 'open', 'reason': 'invalid_response', 'until': 99}
    state.items['b'] = {'state': 'quota_wait', 'quota_blocked': True}
    state.items['c'] = {'state': 'balance_wait'}
    state.items['d/other'] = {'state': 'open', 'reason': 'other', 'last_client_request_at': anchor}
    check('expired model fault is removed', state.expire_idle_model_errors() == ['a/broken'])
    check('fault history is also cleared', 'a/broken' not in state.history)
    check('source protocol and quota states preserved', state.items['a']['reason'] == 'invalid_response' and state.items['b']['quota_blocked'] and state.items['c']['state'] == 'balance_wait')
    check('unrelated model state preserved', 'd/other' in state.items)
    state.mark_client_request('a', 'broken')
    state.failure('a', 'broken', m.Failure('permission_denied', 403))
    check('next rejected business request establishes fresh fault', state.items['a/broken']['consecutive'] == 1)
    state.items['a/broken']['last_client_request_at'] = anchor
    state.mark_client_request('a', 'broken')
    check('new real attempt extends idle period', not state.expire_idle_model_errors())
    state.items['a/broken']['last_client_request_at'] = anchor
    state.persist()
    restored = m.State(Path(folder))
    check('restart preserves activity timestamp', restored.items['a/broken']['last_client_request_at'] == anchor)
    check('restart does not renew idle period', restored.expire_idle_model_errors() == ['a/broken'])
    restored.failure('a', 'probe-only', m.Failure('model_unavailable', 403))
    check('probe created fault receives finite initial expiry anchor', restored.items['a/probe-only']['last_client_request_at'] > 0)
    restored.items['a/probe-only']['last_client_request_at'] -= 3601
    check('probe only fault eventually expires', restored.expire_idle_model_errors() == ['a/probe-only'])
    (Path(folder)/'routing-state.json').write_text(json.dumps({'a/old': {'state': 'open', 'reason': 'model_unavailable', 'checked_at': '2026-01-01T00:00:00+08:00'}}))
    legacy = m.State(Path(folder))
    check('old persisted faults migrate and expire', legacy.expire_idle_model_errors() == ['a/old'])
    check('cleared fault does not grant reclaim qualification', not legacy.get('a/old').get('last_success_at') and not legacy.get('a/old').get('reclaim_verified'))

report = {'passed': True, 'count': len(checks), 'checks': checks, 'real_provider_calls': 0}
Path('reports/model-idle-verification.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps(report))
