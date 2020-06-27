# index_crud.py

import asyncio
from asyncio import Lock, StreamReader
from asyncio.subprocess import PIPE
import re
from typing import Optional, Coroutine, DefaultDict, Dict, Callable
from typing import cast, Tuple, Iterable, Union, List, Any
from pathlib import Path
from collections import defaultdict
import logging
from uuid import uuid4
import functools

from aiohttp.web import Response, json_response

from util import log

named_locks: DefaultDict[str,Lock] = defaultdict(Lock)
# should be an initialized git repo!

Doc = str
DocId = str

loop = asyncio.get_event_loop()

def initialize_with_lock(f):
    async def wrapped(*args, **kwargs):
        async with named_locks['source_docs']:
            return await f(*args, **kwargs)
    return wrapped

class GitError(RuntimeError):
    cmd_args: Iterable[str]
    message: str

    def __init__(
            self, cmd_args: Tuple, message: str = '', *, log_error=True
        ):
        super().__init__()
        self.cmd_args = cmd_args
        self.message = message
        if log_error:
            log.error(str(self))

    def __repr__(self) -> str:
        cls = self.__class__.__name__
        cmd = self.cmd_args
        msg = self.message
        return f'<{cls}:(cmd_args={cmd},message={msg})>'

    @property
    def response(self) -> Response:
        return Response(status=500, reason=repr(self))

class GitAddError(GitError): pass
class GitResetError(GitError): pass
class GitRmError(GitError): pass
class GitCommitError(GitError): pass

async def get_output(reader: Optional[StreamReader]) -> str:
    if isinstance(reader, StreamReader):
        output = await reader.read()
        return output.decode('utf-8')
    return ''
 
async def _git_dispatch(git_dir: str, args, GitErrorClass, *, log_error=True, reset=False):
    git = await asyncio.create_subprocess_exec(
            'git','-C',git_dir, *args,
            stdin=PIPE, stdout=PIPE, stderr=PIPE
        )
    await git.wait()
    if git.returncode != 0:
        err_str = await get_output(git.stderr)
        log.error(f'git error: {args}, {git.returncode}, code {err_str}')
        if reset:
            await git_reset(git_dir)
        args = (err_str,log_error)
        raise GitErrorClass(args)
    else:
        out_str = await get_output(git.stdout)
        log.info(out_str)

async def git_add(git_dir: str, docId: DocId):
    await _git_dispatch(git_dir, ('add',docId), GitAddError, reset=True)
    log.info(f'git SUCCESS: [add] {docId}')

async def git_rm(git_dir: str, docId: DocId):
    await _git_dispatch(git_dir, ('rm',docId), GitRmError, reset=True)
    log.info(f'git SUCCESS: [rm] {docId}')

async def git_reset(git_dir: str):
    await _git_dispatch(git_dir, ('reset','--hard'), GitResetError, reset=False)
    log.info(f'git SUCCESS: [reset]')

async def git_commit(git_dir: str, message: str, reset=True):
    await _git_dispatch(git_dir, ('commit','-m',message), GitCommitError, reset=reset)
    log.info(f'git SUCCESS: [commit] {message}')

async def git_init(git_dir: str):
    await _git_dispatch(git_dir, ('init',), GitError)
    log.info(f'git SUCCESS: [init]')

# TODO make this a little better (remote name and branch...)
async def git_pull(git_dir: str):
    await _git_dispatch(git_dir, ('pull','origin','master'), GitError)
    log.info(f'git SUCCESS: [init]')

def get_new_path(git_dir: str, doc: Doc, name: Optional[DocId]) -> Optional[Path]:
    """Get a path for creating a new paragraph in the source.

    If a name is passed, just return the Path object at that name.
    Otherwise, search a little bit for a filename that doesn't exists and
    return that.
    """
    if isinstance(name,DocId):
        return Path(git_dir) / name
    else:
        name_candidate = '_'.join(re.split(r'\W+',doc))
        i = 10
        while i < 20:
            path = Path(git_dir) / name_candidate[:i]
            if not path.exists():
                return path
    return None

async def _create(git_dir: str, doc: Doc, name: Optional[DocId]) -> Response:
    """Add paragraph to git_dir, and reindex"""
    path = get_new_path(git_dir, doc, name)
    if path is None:
        reason = "could not create file (try passing a name)"
        log.error(reason)
        return Response(status=500, reason=reason)
    path = cast(Path, path)
    if path.exists():
        reason = f'{name} already exists'
        log.error(reason)
        return Response(status=409, reason=reason)
    with open(path,'w') as file:
        print(doc,file=file)
    ## TODO: windows compatibility
    try:
        await git_add(git_dir, path.name)
        await git_commit(git_dir, f'created: {path.name}')
        reason = f'{path.name} created successfully'
        return Response(status=200, reason=reason)
    except GitError as e:
        # rollback
        log.error(f'GitError: {e}')
        path.unlink()
        return e.response

def check_path(git_dir: str, docId: DocId, should_exist: bool) -> Union[Response,Path]:
    path = Path(git_dir) / docId
    if path.exists() and should_exist:
        return path
    elif not path.exists and not should_exist:
        return path
    elif should_exist:
        reason = f'{docId} not found in source directory'
        log.error(reason)
        return Response(status=404, reason=reason)
    else:
        reason = f'{docId} already exists in source directory'
        log.error(reason)
        return Response(status=404, reason=reason)
    return None

async def _delete(git_dir: str, docId: Union[List[DocId],DocId]) -> Response:
    """Delete paragraph from git_dir, and reindex"""
    if isinstance(docId,list):
        return await _delete_multi(git_dir, docId)
    docId = cast(DocId, docId)
    path = check_path(git_dir, docId, should_exist=True)
    if isinstance(path, Response):
        return path
    try:
        await git_rm(git_dir, docId)
        await git_commit(git_dir, f'removed: {docId}')
        reason = f'{docId} created successfully'
        return Response(status=200, reason=reason)
    except GitRmError as e:
        reason = f'error removing {docId} from index (check index integrity)'
        log.error(reason)
        return e.response
    except GitError as e:
        return e.response

async def _delete_multi(git_dir: str, docIds: List[DocId]) -> Response:
    """Delete paragraph from git_dir, and reindex"""
    errors = []
    for docId in docIds:
        response = await _delete(git_dir, docId)
        if response.status != 200:
            errors.append(response.reason)
    return json_response({'errors':errors})

async def _read(git_dir: str, docId: Union[DocId,List[DocId]]) -> Response:
    """Retrieve docId (including wildcards)"""
    paths: List[Path] = []
    git_path = Path(git_dir)
    if isinstance(docId, DocId):
        docIds = [cast(DocId,docId)]
    else:
        docIds = cast(List[DocId],docId)
    for docId in docIds:
        paths.extend(list(git_path.glob(docId)))
    paths = list(filter(lambda path: path.name.endswith('.txt'), paths))
    docs: List[Dict[str,str]] = []
    for path in paths:
        with open(path) as file:
            docs.append({'docId':path.name, 'text': file.read()})
    data = {'docs': docs }
    return json_response(data)

def get_path_sequence(git_dir: str, docId: DocId, n: int) -> List[Path]:
    # Cheap, but hackey
    # get new path - sequence...
    if n == 1:
        return [Path(git_dir) / docId]
    stem = re.sub(r'(.*)(?:\.\w*)?.txt', r'\1', docId)
    #paths: List[Path] = []
    paths: List[Path] = [Path(git_dir) / (f'{stem}.{i}.txt')
                         for i in range(n)]
    j = 0
    while j == 0 or any(map(Path.exists, paths)) and j < 100:
        uid = str(uuid4())[:6]
        paths = [Path(git_dir) / (f'{stem}.{i}.{uid}.txt') 
                 for i in range(n)]
        j += 1
    if j == 100:
        raise RuntimeError('failed to get new path sequence')
    return paths

async def _update(git_dir: str, docId: DocId, docs: Union[Doc,List[Doc]]) -> Response:
    """Update paragraph in git_dir, and reindex"""
    path_or_response = check_path(git_dir, docId, should_exist=True)
    if isinstance(path_or_response, Response):
        return path_or_response
    try:
        # many vs one...
        paths: List[Path]
        if isinstance(docs, Doc):
            docs = [docs]
            paths = [Path(git_dir) / docId]
        else:
            docs = cast(List[Doc], docs)
            try:
                paths = get_path_sequence(git_dir, docId, len(docs))
            except RuntimeError:
                reason = "couldn't create a unique filename"
                log.error(f'update failure: {reason}')
                return Response(status=500, reason=reason)
        # write new files
        for path, doc in zip(paths, docs):
            with open(path, 'w') as file:
                print(doc, file=file)
        # update git index
        path_names: List[str] = [path.name for path in paths]
        for path_name in path_names:
            await git_add(git_dir, path_name)
        # many vs one...
        # If more than one, then the original needs to be removed
        if len(paths) > 1:
            await git_rm(git_dir, docId)
        msg = f'updated {docId} to {path_names[0]} ..  {len(path_names)-1}'
        await git_commit(git_dir, msg)
        reason = f'update success: {docId}'
        log.info(reason)
        data = {'docIds': path_names}
        return json_response(data, reason=reason)
    except GitError as e:
        log.error(e)
        return e.response

AsyncMethod = Callable[..., Coroutine[Any,Any,Response] ]

def check_initialized(f: AsyncMethod) -> AsyncMethod:
    @functools.wraps(f)
    async def wrapped(self, *args, **kwargs):
        await self.initialize()
        return await f(self, *args, **kwargs)
    return wrapped

def acquire_lock(f: AsyncMethod) -> AsyncMethod:
    @functools.wraps(f)
    async def wrapped(self, *args, **kwargs):
        lock = self.lock
        if lock is not None:
            await lock.acquire()
        result = await f(self, *args, **kwargs)
        if lock is not None:
            lock.release()
        return result
    return wrapped

class GitClient:
    source_dir: str
    # maybe remote will do something interesting in the future, for now our 
    # pull just uses '... pull origin master'
    remote: Optional[str] = None
    lock: Optional[Lock] = None
    initialized: bool

    def __init__(
            self, source_dir: str, remote: Optional[str] = None,
            lock: Optional[Lock] = None
        ):
        self.source_dir = source_dir
        self.remote = remote
        self.lock = lock
        self.initialized = False

    @acquire_lock
    async def initialize(self, *args):
        if self.initialized: return
        path = Path(self.source_dir)
        if not path.exists():
            try:
                path.mkdir()
            except Exception as e:
                log.error(f'error creating directory {self.source_dir}: {e}')
                raise
        async with named_locks[self.source_dir]:
            await git_init(self.source_dir, *args)
        self.initialized = True

    @check_initialized
    async def create(self, *args) -> Response:
        return await _create(self.source_dir, *args)

    @check_initialized
    async def read(self, *args) -> Response:
        return await _read(self.source_dir, *args)

    @check_initialized
    async def update(self, *args) -> Response:
        return await _update(self.source_dir, *args)

    @check_initialized
    async def delete(self, *args) -> Response:
        return await _delete(self.source_dir, *args)

    @check_initialized
    async def pull(self, *args) -> Response:
        return await git_pull(self.source_dir, *args)

async def test():
    import json
    import subprocess
    git_dir = str(uuid4())
    git_client = GitClient(git_dir)
    if Path(git_dir).exists():
        raise RuntimeError(f'about to step on {git_dir}')
    try:
        response = await git_client.create("this is a test",'test.txt')
        print(f"{response}")

        response = await git_client.update('test.txt',['this aint no test!', 'yo momma so fat'])
        try:
            body = json.loads(response.body)
        except:
            print('[JSON]: ' + response.body)
        print(f"{response}, {body}")
        docIds = body['docIds']

        response = await git_client.read('*')
        body = json.loads(response.body)
        print(f"{response}, {body}")

        for docId in docIds:
            response = await git_client.read(docId)
            body = json.loads(response.body)
            print(f"{response}, {body}")

            response = await git_client.delete(docId)
            print(f"{response}")
    finally:
        subprocess.run(['rm','-rf',git_dir],check=True)

if __name__ == '__main__':
    loop.run_until_complete(test())
