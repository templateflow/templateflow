#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["aiohttp"]
# ///
"""Verify git-annex files are accessible on S3."""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

import aiohttp


def run(*cmd, cwd=None, check=True):
    return subprocess.run(cmd, capture_output=True, text=True, check=check, cwd=cwd)


def get_submodules():
    result = run('git', 'submodule', 'foreach', '--quiet', 'echo $sm_path')
    return sorted(line.strip() for line in result.stdout.splitlines() if line.strip())


def get_annexed_files(path):
    res = run('git', 'annex', 'whereis', '--json', cwd=path, check=False)
    if res.returncode != 0:
        return []
    files = []
    for line in res.stdout.splitlines():
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        has_s3 = any(
            r.get('description', '') == '[s3]' for r in data.get('whereis', [])
        )
        if has_s3:
            files.append(data['file'])
    return files


async def check_file(session, sem, name, file_path):
    encoded_path = quote(f'{name}/{file_path}', safe='/')
    url = f'https://templateflow.s3.amazonaws.com/{encoded_path}'
    async with sem:
        try:
            async with session.head(url, allow_redirects=True) as r:
                deleted = r.headers.get('x-amz-delete-marker', '').lower() == 'true'
                match (deleted, r.status):
                    case (True, _):
                        status = 'deleted'
                    case (_, 404):
                        status = 'missing'
                    case (_, 200):
                        status = 'ok'
                    case _:
                        status = 'unknown'
        except aiohttp.ClientError:
            status = 'error'
    return name, file_path, status, url


async def main_async(submodules):
    tasks = []
    for submodule in submodules:
        name = Path(submodule).name
        print(f'Collecting files: {name}...')
        for file_path in get_annexed_files(Path(submodule)):
            tasks.append((name, file_path))

    print(f'\nChecking {len(tasks)} files across {len(submodules)} submodule(s)...\n')

    concurrent = int(os.getenv('CONCURRENT_REQUESTS', 50))
    sem = asyncio.Semaphore(concurrent)
    errors = 0

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[check_file(session, sem, name, file_path) for name, file_path in tasks]
        )

    by_submodule = {}
    issues = []
    for name, file_path, status, url in results:
        by_submodule.setdefault(name, {'ok': 0, 'total': 0})
        by_submodule[name]['total'] += 1
        if status == 'ok':
            by_submodule[name]['ok'] += 1
        else:
            issues.append((name, file_path, status, url))
            errors += 1

    for name, counts in sorted(by_submodule.items()):
        ok, total = counts['ok'], counts['total']
        print(f'{name}: {ok}/{total} {"OK" if ok == total else "ERROR"}')

    if issues:
        print()
        for name, file_path, status, url in sorted(issues):
            full_path = f'{name}/{file_path}'
            msg = {
                'deleted': f'Delete marker found on S3: {url}',
                'missing': f'File not found (404): {url}',
                'error': f'HTTP error: {url}',
                'unknown': f'Unexpected response from S3: {url}',
            }[status]
            if os.environ.get('GITHUB_ACTIONS') == 'true':
                print(f'::error file={full_path}::{msg}')
            else:
                print(f'  [ERROR] {full_path}: {msg}')

    print(f'\nDone. {errors} error(s) found.')
    return errors


def main():
    submodules = get_submodules()
    errors = asyncio.run(main_async(submodules))
    sys.exit(1 if errors else 0)


if __name__ == '__main__':
    main()
