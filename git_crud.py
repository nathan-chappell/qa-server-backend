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
# should be an initialized git repo!
SOURCE_DIR = './source_docs'

#log_format = '[%(levelname)s]    %(filename)s:%(lineno)d:%(funcName)s  %(message)s'
class Formatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        #print(f'formatting message: {message}')
        lines = map(str.strip,re.split("\n", message))
        lines = list(filter(lambda s: s.strip() != '', lines))
        formatted = f'[{record.levelname}] [{record.filename}:{record.lineno}]'
        if len(lines) == 0:
            return formatted
        elif len(lines) == 1:
            return f'{formatted} - {lines[0]}'
        else:
            eol = "\n" + ' '*4 + '-'*4 + ' '*4
            return formatted + eol + eol.join(lines)
        
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger()
log.handlers[0].setFormatter(Formatter())

Doc = str
DocId = str

def GIT_CMD() -> List[str]:
    return ['git','-C',SOURCE_DIR]

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
            'git','-C',SOURCE_DIR, *args,
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
        log.info(out.decode('utf-8'))

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
    paths: Iterable[Path]
    print(SOURCE_DIR)
    source_dir = Path(SOURCE_DIR)
    paths = list(source_dir.glob(docId))
    paths = list(filter(lambda path: path.name.endswith('.txt'), paths))
    docs: List[Doc] = []
    for path in paths:
        with open(path) as file:
            docs.append(file.read())
    docs = [{'docId':path.name, 'text': doc} 
            for path,doc in zip(paths,docs)]
    data = { 'docs': docs }
    return json_response(data)

def get_path_sequence(docId: DocId, n: int) -> List[Path]:
    # Cheap, but hackey
    # get new path - sequence...
    stem = re.sub(r'(.*)(?:\.\w*)?.txt', r'\1', docId)
    paths: List[Path] = []
    j = 0
    while j == 0 or any(map(Path.exists, paths)) and j < 100:
        uid = str(uuid4())[:6]
        paths = [Path(SOURCE_DIR) / (f'{stem}.{i}.{uid}.txt') 
                 for i in range(n)]
        j += 1
    if j == 100:
        raise RuntimeError('failed to get new path sequence')
    return paths

async def _update(docId: DocId, docs: Union[Doc,List[Doc]]) -> Response:
    """Update paragraph in source_dir, and reindex"""
    path_or_response = check_path(docId, should_exist=True)
    if isinstance(path_or_response, Response):
        return path_or_response
    else:
        if isinstance(docs, Doc):
            docs = [docs]
        try:
            docs = cast(List[Doc], docs)
            try:
                paths: List[Path] = get_path_sequence(docId, len(docs))
            except RuntimeError:
                reason = "couldn't create a unique filename"
                log.error(f'update failure: {reason}')
                return Response(status=500, reason=reason)
            # write new files
            for path, doc in zip(paths, docs):
                with open(path,'w') as file:
                    print(doc, file=file)
            # update git index
            path_names: List[str] = [path.name for path in paths]
            for path_name in path_names:
                await git_add(path_name)
            await git_rm(docId)
            msg = f'updated {docId} to {path_names[0]} ..  {len(path_names)}'
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
    global SOURCE_DIR
    import json
    import subprocess
    SOURCE_DIR = str(uuid4())
    source_dir = Path(SOURCE_DIR)
    if source_dir.exists():
        raise RuntimeError(f'about to step on {SOURCE_DIR}')
    try:
        subprocess.run(['mkdir', SOURCE_DIR], check=True)
        subprocess.run(['git', '-C', SOURCE_DIR, 'init'], check=True)

        response = await create("this is a test",'test.txt')
        print(f"{response}")

        response = await update('test.txt',['this aint no test!', 'yo momma so fat'])
        try:
            body = json.loads(response.body)
        except:
            print('[JSON]: ' + response.body)
        print(f"{response}, {body}")
        docIds = body['docIds']

        response = await read('*')
        body = json.loads(response.body)
        print(f"{response}, {body}")

        for docId in docIds:
            response = await read(docId)
            body = json.loads(response.body)
            print(f"{response}, {body}")

            response = await delete(docId)
            print(f"{response}")
    finally:
        subprocess.run(['rm','-rf',SOURCE_DIR],check=True)

if __name__ == '__main__':
    loop.run_until_complete(test())
