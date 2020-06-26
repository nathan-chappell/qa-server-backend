# index_crud.py

import asyncio
from asyncio.subprocess import PIPE
import re
from typing import Optional, Coroutine
from pathlib import Path

from aiohttp import Response

from create_index import index_all
from util import Paragraph, INDEX_NAME, ANALYZER_NAME
from util import named_locks, es, SOURCE_DIR

DocId = str

GIT_CMD = ['git','-C',SOURCE_DIR]

# TODO index versioning, maybe?
async def reindex():
    await index_all(INDEX_NAME)

def initialize_with_lock(f):
    async def wrapped(*args,**kwargs):
        async with named_locks['source_docs']:
            return await f(*args, **kwargs)
    return wrapped

class GitError(RuntimeError):
    command: str
    error_message: str

    def __init__(self, command: str, error_message: str, *, log_error=True):
        super().__init__()
        self.command = command
        self.error_message = error_message
        if log_error:
            log.error(str(self))

    def __str__(self) -> str:
        message = re.sub('\s+',' ',self.error_message)
        return f'git error: {self.command:10}: {message}'

    def __repr__(self) -> str:
        cls = self.__class__.__name__
        cmd = self.command
        msg = self.error_message
        return f'<{cls}(command={cmd},error_message={msg})>'

class GitAddError(GitError):
    def __init__(self, error_message: str, *, log_error=True):
        super().__init__('add', error_message, log_error=log_error)

class GitResetError(GitError):
    def __init__(self, error_message: str, *, log_error=True):
        super().__init__('reset', error_message, log_error=log_error)

class GitCommitError(GitError):
    def __init__(self, error_message: str, *, log_error=True, reset=True):
        super().__init__('add', error_message, log_error=log_error)
        if reset:
            self.reset()

    def reset(self)
        try:
            git_reset()
        except GitResetError as e:
            self.error_message += ' RESET FAILED!'
        else:
            self.error_message += ' RESET SUCCEEDED'

async def _git_dispatch(args, GitErrorClass, *, log_error=True, reset=None)
    git = await asyncio.create_subprocess_exec(
            *GIT_CMD,*args,
            stdin=PIPE,stderr=PIPE
        )
    await git.wait()
    if git.returncode != 0:
        err = await git.stderr.read()
        args = (err,log_error)
        if reset is not None:
            args = args + (reset,)
        raise GitErrorClass(*args)

async def git_add(name: str):
    await _git_dispatch(('add',name), GitAddError)
    log.info('git SUCCESS: add {name}')

async def git_reset():
    await _git_dispatch(('reset','--hard'), GitResetError)
    log.info('git SUCCESS: reset')

async def git_commit(message: str, reset=True):
    await _git_dispatch(('commit','-m',message), GitCommitError, reset=reset)
    log.info('git SUCCESS: commit {message}')

def get_new_path(name: Optional[DocId]) -> Optional[Path]:
    """Get a path for creating a new paragraph in the source.

    If a name is passed, just return the Path object at that name.
    Otherwise, search a little bit for a filename that doesn't exists and
    return that.
    """
    if isinstance(name,DocId):
        return Path(SOURCE_DIR) / name
    else:
        name_candidate = '_'.join(re.split(r'\W+',text))
        i = 10
        while i < 20:
            path = Path(SOURCE_DIR) / name_candidate[:i]
            if not path.exists():
                return path
    return None


async def _create(text: Paragraph, name: Optional[DocId], *, need_lock=True) -> Response:
    """Add paragraph to source_dir, and reindex"""
    path = get_new_path(name)
    if path is None:
        reason = "could not create file (try passing a name)"
        return Response(status=500, reason=reason)
    path = cast(Path, path)
    if path.exists():
        return Response(status=409, reason="{name} already exists")
    with open(path,'w') as file:
        file.write(text)
    ## TODO: windows compatibility
    try:
        await git_add(path.name)
        await git_commit(f'created: {path.name}')
    except GitError as e:
        # rollback
        path.unlink()
        return Response(status=500, reason=str(e))

async def _read(docId: DocId) -> Response:
    """Retrieve docId (including wildcards)"""
    ...

async def _update(docId: DocId, text: Paragraph) -> Response:
    """Update paragraph in source_dir, and reindex"""
    ...

async def _delete(docId: DocId) -> Response:
    """Delete paragraph from source_dir, and reindex"""
    ...

create = initialize_with_lock(_create)

