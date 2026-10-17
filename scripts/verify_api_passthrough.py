"""Render policy + real gateway executable against a local synthetic upstream."""
import asyncio
import ast
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile

import aiohttp
from aiohttp import web
import yaml
from verify_routing import serve


async def main():
    checks, requests = [], []

    def check(name, value):
        assert value, name
        checks.append(name)

    async def fake(request):
        requests.append((await request.json(), dict(request.headers)))
        result = {'id': 'resp_synthetic', 'object': 'response', 'status': 'completed',
                  'model': 'gpt-6.1-sol', 'output': []}
        return web.Response(text='data: ' + json.dumps({'type': 'response.completed', 'response': result}) + '\n\n',
                            content_type='text/event-stream')

    app = web.Application()
    app.router.add_post('/v1/responses', fake)
    runner, upstream = await serve(app)
    try:
        with tempfile.TemporaryDirectory() as folder:
            data = Path(folder)
            sources = [{'id': ident, 'name': ident, 'kind': 'api', 'enabled': True,
                        'models': [{'alias': 'gpt-6.1-sol', 'upstream': 'gpt-6.1-sol'}]}
                       for ident in ('synthetic-a', 'topapi-plus', 'legacy')]
            providers = {s['id']: {'name': s['name'], 'base_url': upstream + '/v1', 'api_key': 'synthetic-upstream'} for s in sources}
            providers['legacy']['disable_codex_cloaking'] = False
            token = secrets.token_urlsafe(24)
            fn = next(node for node in ast.parse(Path('scripts/controller.py').read_text(encoding='utf-8-sig')).body
                      if isinstance(node, ast.FunctionDef) and node.name == 'render')
            namespace = {'SETTINGS': {'sources': sources, 'session_affinity': True}, 'DATA': data,
                         'QUOTAS': {'disabled': []}, 'KEYS': {'management': 'synthetic-admin', 'client': token},
                         'ROUTER': 'synthetic', 'os': os, 'yaml': yaml, 'load': lambda *args: {},
                         'original': lambda s: providers[s['id']],
                         'save': lambda p, value: p.write_text(json.dumps(value)), 'APPLY_ERROR': ''}
            exec(compile(ast.Module(body=[fn], type_ignores=[]), 'controller-render', 'exec'), namespace)
            previous = os.environ.get('PREPARE_ONLY')
            os.environ['PREPARE_ONLY'] = '1'
            try:
                namespace['render']()
            finally:
                if previous is None:
                    os.environ.pop('PREPARE_ONLY', None)
                else:
                    os.environ['PREPARE_ONLY'] = previous
            config = yaml.safe_load((data / 'docker-config.yaml').read_text(encoding='utf-8'))
            groups = config['api-keys']['codex']
            check('image tools use passthrough policy', config['multimedia']['disable-image-generation'] == 'passthrough')
            check('no TopAPI UA exception', all('headers' not in group for group in groups))
            check('API header policy shared by all providers', all(group['keys'][0]['disable-codex-cloaking'] for group in groups[:2]))
            check('explicit provider override retained', groups[2]['keys'][0]['disable-codex-cloaking'] is False)
            check('official OAuth is not globally uncloaked', not config.get('upstream', {}).get('codex', {}).get('disable-codex-cloaking', False))
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            config['server'] = {'host': '127.0.0.1', 'port': port}
            config['oauth'] = {'auth-dir': str(data / 'auths')}
            (data / 'auths').mkdir()
            config['observability']['logs'].update(**{'logging-to-file': False, 'request-log': False, 'error-logs-max-files': -1})
            (data / 'native.yaml').write_text(yaml.safe_dump(config), encoding='utf-8')
            exe = os.environ.get('NATIVE_EXE', str(Path('runtime/bin/cli-proxy-api.exe').resolve()))
            flags = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}
            proc = subprocess.Popen([exe, '-config', str(data / 'native.yaml'), '-local-model'], cwd=data,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **flags)
            try:
                url = 'http://127.0.0.1:' + str(port)
                async with aiohttp.ClientSession(headers={'Authorization': 'Bearer ' + token}) as client:
                    for _ in range(100):
                        try:
                            async with client.get(url + '/') as response:
                                if response.status == 200:
                                    break
                        except aiohttp.ClientError:
                            pass
                        await asyncio.sleep(.05)
                    else:
                        raise RuntimeError('native startup failed')
                    for ident in ('synthetic-a', 'topapi-plus'):
                        for tools in (None, [{'type': 'image_generation', 'output_format': 'png'}],
                                      [{'type': 'function', 'name': 'echo', 'parameters': {'type': 'object', 'properties': {'text': {'type': 'string'}}, 'required': ['text']}}]):
                            body = {'model': ident + '/gpt-6.1-sol', 'input': 'Reply OK.', 'stream': True,
                                    'instructions': 'Keep this caller instruction.', 'reasoning': {'effort': 'low'}}
                            if tools is not None:
                                body['tools'] = tools
                            async with client.post(url + '/v1/responses', json=body) as response:
                                raw = await response.text()
                                check(ident + ' completed ' + str(tools is not None), response.status == 200 and 'response.completed' in raw)
                            actual, headers = requests[-1]
                            if tools is None:
                                check(ident + ' text does not acquire image tool', not actual.get('tools'))
                            elif tools[0]['type'] == 'image_generation':
                                check(ident + ' caller image tool preserved', actual['tools'] == tools)
                            else:
                                check(ident + ' only caller function tool present', len(actual['tools']) == 1 and actual['tools'][0]['name'] == 'echo' and actual['tools'][0]['type'] == 'function')
                            check(ident + ' caller instructions retained', actual['instructions'] == body['instructions'])
                            check(ident + ' reasoning retained', actual['reasoning']['effort'] == 'low')
                            check(ident + ' no forced Codex or CC Switch UA', not any(s in headers.get('User-Agent', '').lower() for s in ('codex-tui', 'cc-switch')))
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
    finally:
        await runner.cleanup()
    report = {'passed': True, 'count': len(checks), 'checks': checks, 'real_provider_calls': 0}
    Path('reports/api-passthrough-verification.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report))


if __name__ == '__main__':
    asyncio.run(main())
