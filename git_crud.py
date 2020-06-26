# index_crud.py

import asyncio
from asyncio import Lock
from asyncio.subprocess import PIPE
import re
from typing import Optional, Coroutine, DefaultDict
from typing import cast, Tuple, Iterable, Union, List
from pathlib import Path
from collections import defaultdict
import logging
from uuid import uuid4

from aiohttp.web import Response, json_response

#try:
    #from util import named_locks, SOURCE_DIR
#except ImportError:

named_locks: DefaultDict[str,Lock] = defaultdict(Lock)
SOURCE_DIR = './source_docs'

log_format = '%(filename)s:%(lineno)d:%(funcName)s  %(message)s'
logging.basicConfig(format=log_format, level=logging.DEBUG)
log = logging.getLogger()

Doc = str
DocId = str

GIT_CMD = ['git','-C',SOURCE_DIR]

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

async def _git_dispatch(args, GitErrorClass, *, log_error=True, reset=False):
    git = await asyncio.create_subprocess_exec(
            *GIT_CMD, *args,
            stdin=PIPE, stdout=PIPE, stderr=PIPE
        )
    await git.wait()
    if git.returncode != 0:
        err = await git.stderr.read()
        err = err.decode('utf-8')
        log.error(f'git error: {args}, {git.returncode}, code {err}')
        args = (err,log_error)
        if reset:
            await git_reset()
        raise GitErrorClass(args)
    else:
        out = await git.stdout.read()
        out = out.decode('utf-8')
        if out != '':
            log.info(out)

async def git_add(docId: DocId):
    await _git_dispatch(('add',docId), GitAddError, reset=True)
    log.info(f'git SUCCESS: add {docId}')

async def git_rm(docId: DocId):
    await _git_dispatch(('rm',docId), GitRmError, reset=True)
    log.info(f'git SUCCESS: rm {docId}')

async def git_reset():
    await _git_dispatch(('reset','--hard'), GitResetError, reset=False)
    log.info(f'git SUCCESS: reset')

async def git_commit(message: str, reset=True):
    await _git_dispatch(('commit','-m',message), GitCommitError, reset=reset)
    log.info(f'git SUCCESS: commit {message}')

def get_new_path(doc: Doc, name: Optional[DocId]) -> Optional[Path]:
    """Get a path for creating a new paragraph in the source.

    If a name is passed, just return the Path object at that name.
    Otherwise, search a little bit for a filename that doesn't exists and
    return that.
    """
    if isinstance(name,DocId):
        return Path(SOURCE_DIR) / name
    else:
        name_candidate = '_'.join(re.split(r'\W+',doc))
        i = 10
        while i < 20:
            path = Path(SOURCE_DIR) / name_candidate[:i]
            if not path.exists():
                return path
    return None

async def _create(doc: Doc, name: Optional[DocId], *, need_lock=True) -> Response:
    """Add paragraph to source_dir, and reindex"""
    path = get_new_path(doc, name)
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
        await git_add(path.name)
        await git_commit(f'created: {path.name}')
        reason = f'{path.name} created successfully'
        return Response(status=200, reason=reason)
    except GitError as e:
        # rollback
        log.error(f'GitError: {e}')
        path.unlink()
        return e.response

def check_path(docId: DocId, should_exist: bool) -> Union[Response,Path]:
    path = Path(SOURCE_DIR) / docId
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

async def _delete(docId: DocId) -> Response:
    """Delete paragraph from source_dir, and reindex"""
    path = check_path(docId, should_exist=True)
    if isinstance(path, Response):
        return path
    try:
        await git_rm(docId)
        await git_commit(f'removed: {docId}')
        reason = f'{docId} created successfully'
        return Response(status=200, reason=reason)
    except GitRmError as e:
        reason = f'error removing {docId} from index (check index integrity)'
        log.error(reason)
        return e.response
    except GitError as e:
        return e.response

async def _read(docId: DocId) -> Response:
    """Retrieve docId (including wildcards)"""
    path = check_path(docId, should_exist=True)
    if isinstance(path, Response):
        return path
    else:
        path = cast(Path, path)
        with open(path) as file:
            text = file.read()
            data = {'docId': docId, 'text':text}
            return json_response(data)

async def _update(docId: DocId, docs: Union[Doc,List[Doc]]) -> Response:
    """Update paragraph in source_dir, and reindex"""
    path = check_path(docId, should_exist=True)
    if isinstance(path, Response):
        return path
    else:
        docs_: List[Doc]
        if isinstance(docs, Doc):
            docs_ = [docs]
        else:
            docs_ = cast(List[Doc],docs)
        try:
            # Cheap, but hackey
            # get new path - sequence...
            uid = str(uuid4())[:8]
            path_ = Path(SOURCE_DIR) / (path.stem + f'.{uid}.{0}.txt')
            j = 0
            while path_.exists() and j < 100:
                uid = str(uuid4())[:8]
                path_ = Path(SOURCE_DIR) / (path.stem + f'.{uid}.{0}.txt')
            if j == 100:
                reason = "couldn't create a unique filename"
                log.error(f'update failure: {reason}')
                return Response(status=500, reason=reason)
            # write new files
            path_names: List[str] = []
            for i,doc in enumerate(docs_):
                path_name = path.stem + f'.{uid}.{i}.txt'
                path_names.append(path_name)
                with open(Path(SOURCE_DIR) / path_name,'w') as file:
                    file.write(doc)
            # update git index
            for path_name in path_names:
                await git_add(path_name)
            await git_rm(docId)
            msg = f'updated {docId} to {path_names[0]} .. {len(path_names)}'
            await git_commit(msg)
            reason = f'update success: {docId}'
            log.info(reason)
            data = {'docIds': path_names}
            return json_response(data, reason=reason)
        except GitError as e:
            log.error(e)
            return e.response
                

create = initialize_with_lock(_create)
read = initialize_with_lock(_read)
update = initialize_with_lock(_update)
delete = initialize_with_lock(_delete)

async def test():
    import json
    response = await create("this is a test",'test.txt')
    print(f"{response}")
    response = await update('test.txt','yo momma so fat')
    body = json.loads(response.body)
    print(f"{response}, {body}")
    docId = body['docIds'][0]
    response = await read(docId)
    body = json.loads(response.body)
    print(f"{response}, {body}")
    response = await delete(docId)
    print(f"{response}")


if __name__ == '__main__':
    loop.run_until_complete(test())
