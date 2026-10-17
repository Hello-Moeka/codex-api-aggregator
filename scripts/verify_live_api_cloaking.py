"""Isolated upstream comparison. Credentials are read locally, never printed."""
import asyncio
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time

import aiohttp
import yaml


async def main():
    data = Path('/app/private')
    providers = json.loads((data / 'upstreams.json').read_text())
    selected = os.environ.get('VERIFY_SOURCES', '').split(',')
    if selected != ['']:
        providers = [provider for provider in providers if provider['id'] in selected]
    folder = Path(tempfile.mkdtemp(prefix='api-cloaking-'))
    token = secrets.token_urlsafe(24)
    process = None
    results = []
    try:
        groups = []
        for provider in providers:
            for disabled in (False, True):
                ident = provider['id'] + ('-plain' if disabled else '-cloak')
                groups.append({'name': ident, 'prefix': ident, 'base-url': provider['base_url'],
                               'request-retry': 0,
                               'keys': [{'api-key': provider['api_key'], 'disable-codex-cloaking': disabled}],
                               'models': [{'name': 'gpt-6.1-sol', 'alias': 'gpt-6.1-sol'}]})
        (folder / 'auths').mkdir()
        config = {'config-version': 8, 'server': {'host': '127.0.0.1', 'port': 18487},
                  'access': {'api-keys': [token]}, 'oauth': {'auth-dir': str(folder / 'auths')},
                  'api-keys': {'codex': groups},
                  'multimedia': {'disable-image-generation': 'passthrough'},
                  'routing': {'force-model-prefix': True, 'retry': {'request-retry': 0, 'max-retry-credentials': 0},
                              'cooldown': {'disable-cooling': True}},
                  'observability': {'logs': {'logging-to-file': False, 'request-log': False, 'error-logs-max-files': -1}}}
        path = folder / 'config.yaml'
        path.write_text(yaml.safe_dump(config))
        process = subprocess.Popen(['/usr/local/bin/cli-proxy-api', '-config', str(path), '-local-model'],
                                   cwd=folder, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        async with aiohttp.ClientSession() as client:
            for _ in range(30):
                try:
                    async with client.get('http://127.0.0.1:18487/') as response:
                        if response.status == 200:
                            break
                except aiohttp.ClientError:
                    pass
                await asyncio.sleep(.2)
            limit = asyncio.Semaphore(3)

            async def call(provider, disabled):
                async with limit:
                    ident = provider['id'] + ('-plain' if disabled else '-cloak')
                    result = {'source': provider['id'], 'cloaking_disabled': disabled}
                    started = time.monotonic()
                    body = {'model': ident + '/gpt-6.1-sol', 'input': 'Reply only OK.',
                            'stream': True, 'reasoning': {'effort': 'low'}}
                    tool_test = os.environ.get('VERIFY_TOOLS') == '1'
                    if tool_test:
                        body.update(input='Call echo with text OK.',
                                    tools=[{'type': 'function', 'name': 'echo',
                                            'description': 'Return the supplied text.',
                                            'parameters': {'type': 'object', 'properties': {'text': {'type': 'string'}},
                                                           'required': ['text'], 'additionalProperties': False}, 'strict': True}],
                                    tool_choice='required')
                        result['test'] = 'function_tool'
                    try:
                        async with client.post('http://127.0.0.1:18487/v1/responses', json=body,
                                               headers={'Authorization': 'Bearer ' + token},
                                               timeout=aiohttp.ClientTimeout(total=float(os.environ.get('VERIFY_TIMEOUT', '40')))) as response:
                            result['http'] = response.status
                            raw = await response.text()
                            completed = False
                            output = ''
                            function_call = False
                            for line in raw.splitlines():
                                if not line.startswith('data:'):
                                    continue
                                try:
                                    event = json.loads(line[5:])
                                except ValueError:
                                    continue
                                completed |= event.get('type') == 'response.completed'
                                items = [event.get('item', {})] + event.get('response', {}).get('output', [])
                                function_call |= any(isinstance(item, dict) and item.get('type') == 'function_call' and item.get('name') == 'echo' for item in items)
                                if event.get('type') == 'response.output_text.delta':
                                    output += event.get('delta', '')
                            result.update(completed=completed, text_ok='OK' in output)
                            if tool_test:
                                result['function_call'] = function_call
                            if response.status != 200:
                                try:
                                    error = json.loads(raw).get('error', {})
                                    result['error'] = {key: str(error.get(key, ''))[:100] for key in ('code', 'type')}
                                except ValueError:
                                    pass
                    except (aiohttp.ClientError, TimeoutError) as error:
                        result['exception'] = type(error).__name__
                    result['seconds'] = round(time.monotonic() - started, 2)
                    results.append(result)
                    print(json.dumps(result), flush=True)

            for _ in range(int(os.environ.get('VERIFY_ROUNDS', '1'))):
                modes = (True,) if os.environ.get('VERIFY_DISABLE_ONLY') == '1' else (False, True)
                await asyncio.gather(*(call(provider, disabled) for provider in providers for disabled in modes))
        suffix = '-tools' if os.environ.get('VERIFY_TOOLS') == '1' else '-recheck' if selected != [''] else ''
        if os.environ.get('VERIFY_RECHECK') == '1':
            suffix += '-recheck'
        filename = 'api-cloaking-comparison' + suffix + '.json'
        (data / filename).write_text(json.dumps(results, indent=2))
    finally:
        if process:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        shutil.rmtree(folder, ignore_errors=True)


if __name__ == '__main__':
    asyncio.run(main())
