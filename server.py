# server.py
"""
"""

from uuid import uuid4
from typing import Dict, Any, List, Iterable, Union, cast
from json.decoder import JSONDecodeError
import atexit
import json

from aiohttp import web
from aiohttp.web import Request, Response, json_response
from aiohttp.web import HTTPInternalServerError
from aiohttp.web_middlewares import _Handler
from markdown import markdown # type: ignore

from util import answer_to_complete_sentence, INDEX_NAME
from transformer_query import gpu_pipeline
from create_index import get_paragraphs_for_query
from canned_answer import no_answer, quick_answer_for_error, get_happy_employee

routes = web.RouteTableDef()

#
# API Documentation.
#
# The readme is served on: GET / HTTP/1.1
#
readme_path = 'README.md'
css_path = './github.css'

qa_log = open('qa_log.multi_json','a')
atexit.register(lambda : qa_log.close())

with open(readme_path) as file:
    md = markdown(file.read())
with open(css_path) as file:
    css = file.read()

print('md:')
print(md)

readme = f"""
<!doctype html>
<head>
    <meta charset="utf8" />
    <link rel="stylesheet" href="/github.css" />
</head>
<body>
    {md}
</body>
</html>
"""

@routes.get('/github.css')
async def get_stylesheet(request: Request) -> Response:
    return Response(text=css, content_type='text/css')

@routes.get('/')
async def serve_readme(request: Request) -> Response:
    return Response(text=readme, content_type='text/html')

#
# Exceptions and exception utilities
#

class APIError(RuntimeError):
    """Exception class to indicate API related errors."""
    message: str
    method: str
    path: str
    def __init__(self, request: web.Request, message: str):
        super().__init__()
        self.method = request.method
        self.path = request.path
        self.message = message

    @property
    def _api(self) -> str:
        return f'{self.method} {self.path}'

    def __str__(self) -> str:
        return self._api + ' - ' + self.message

    def __repr__(self) -> str:
        return f'<APIError: {str(self)}>'

class AnswerError(RuntimeError):
    """Exception class to indicate and error getting an answer"""
    exception: Exception
    question: Dict[str,Any]

    def __init__(self, exception: Exception, question: Dict[str,Any]):
        self.exception = exception
        self.question = question

    def __str__(self) -> str:
        return f'AnswerError: <{self.question}> <{self.exception}>'

    def __repr__(self) -> str:
        return '<' + str(self) + '>'

def exception_to_dict(exception: Exception) -> Dict[str,Any]:
    """Convert an exception into a dict for JSON response."""
    return {'error_type': type(exception).__name__, 'message': str(exception)}

to_json_exceptions = (
    JSONDecodeError,
    KeyError,
    RuntimeError,
    APIError,
)

#
# Middlewares
#
# middleware for server app.  Handles logging, errors, and other logic not
# directly related to the API
#

@web.middleware
async def exception_to_json_middleware(
        request: web.Request, 
        handler: _Handler
        ) -> web.StreamResponse:
    """Catch listed exceptions and convert them to json responses."""
    try:
        return await handler(request)
    except to_json_exceptions as e:
        return web.json_response(exception_to_dict(e))
    except Exception as e:
        print(e)
        return web.json_response({'whoops!':str(e)},status=500)

@web.middleware
async def answer_exception_middleware(
        request: web.Request, 
        handler: _Handler
        ) -> web.StreamResponse:
    try:
        return await handler(request)
    except AnswerError as e:
        #server_log.error(repr(e))
        reply = {}
        reply.update(e.question)
        reply['answers'] = []
        reply['quick_answer'] = quick_answer_for_error()
        return web.json_response(reply)

@web.middleware
async def attach_uuid_middleware(
        request: web.Request, 
        handler: _Handler
        ) -> web.StreamResponse:
    request['uuid'] = str(uuid4())
    return await handler(request)

@web.middleware
async def log_qa_middleware(
        request: web.Request, 
        handler: _Handler
        ) -> web.StreamResponse:
    response = await handler(request)
    response = cast(web.Response, response)
    text = response.text
    if isinstance(text, str):
        reply = json.loads(text)
        to_log = {}
        to_log['question'] = reply['question']
        to_log['quick_answer'] = reply['quick_answer']
        to_log['answers'] = [
                {k:answer[k] for k in answer if k != 'paragraph'}
                for answer in reply['answers']
            ]
        # TODO: in real life, we probably shouldn't flush this
        print(json.dumps(to_log),file=qa_log,flush=True)
    return response

#
# API
#
# all the question-answer related stuff.
#

def get_quick_answer(answers: List[Dict[str,Any]]) -> str:
    """Implement heuristic to choose an answer.

    Right now, it's unclear which answer to choose.  Given that the empty span
    is the best answer for a paragraph, then we can can ignore that paragraph.
    Comparing the ratings returned from the model for different paragraphs is
    problematic...
    Here are the two current ideas:

    1. Choose the first non-empty answer
    2. Choose best rated non-empty answer

    1. Means that we assume that the most relevant paragraph found by
       ElasticSearch is most likely to contain the right answer, so we pick it.
    2. Means that we take the best rated answer.

    Option 1 has experimentally demonstrated better results (see the reports
    on the blog qa), while 2 is the most commonly implemented heuristic.  As
    of right now, ElasticSearch needs to be better configured to make option 1
    work properly.  Option 2 may be a good option if the model can be trained
    to better identify when a paragraph is irrelevant.

    Better configuring ElasticSearch means incorporating boosts, stopwords,
    and making the base text better.
    More training of the model is a bit more interesting, but perhaps less
    promising...
    """
    def filter_no_answers(candidates: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
        return list(filter(lambda answer: answer['answer'] != '', candidates))
    def sort_by_rating(answers_: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
        return list(sorted(answers_, key=lambda a: a['rating'], reverse=True))
    candidates = answers
    candidates = filter_no_answers(candidates)
    candidates = sort_by_rating(candidates)
    if len(candidates) > 0:
        return candidates[0]['answer']
    else:
        return no_answer()

def make_answer(
        answer: str, rating: float = 0., paragraph: str = "",
        paragraph_rank: int = 0, docId: str = ''
        ) -> Dict[str,Union[str,float,int]]:
    return {
        'answer': answer,
        'rating': rating,
        'paragraph': paragraph,
        'paragraph_rank': paragraph_rank,
        'docId': docId,
    }

async def get_answers(query: str) -> List[Dict[str,Any]]:
    """Consult ES and the model to return potential answers."""
    result: List[Dict[str,Any]] = []
    answers = []
    # 
    # At Jelena's request...
    #
    # so this is sort of a toy implementation/ joke, but it's a glimpse of
    # what's to come if we're going to do much brute force chatbotting...
    #
    happy_employee = get_happy_employee(query)
    if happy_employee is not None:
        return [make_answer(happy_employee)]
    #
    paragraphs = await get_paragraphs_for_query(query, INDEX_NAME, topk=5)
    for rank,paragraph in enumerate(paragraphs):
        context = paragraph['text']
        answer = gpu_pipeline({'question': query, 'context': context},
                           handle_impossible_answer=True,
                           topk=1)
        answers.append(make_answer(
            answer=answer_to_complete_sentence(answer['answer'],context),
            rating=answer['score'],
            paragraph=context,
            paragraph_rank=rank,
            docId=paragraph['_id'],
        ))
    return answers

@routes.post('/question')
async def answer_question(request: Request) -> Response:
    """Implement QA API."""
    #import pdb
    #pdb.set_trace()
    try:
        uuid = request['uuid']
    except KeyError:
        uuid = str(uuid4())
    if request.content_type != 'application/json':
        raise APIError(request,'missing header: "content-type:application/json"')
    body = await request.json()
    try:
        question = body['question']
    except KeyError:
        raise APIError(request,'<question: str> required in json body')
    response: Dict[str,Any] = {'question': {'text': question, 'uuid': uuid}}
    try:
        response['answers'] = await get_answers(question)
    except Exception as e:
        raise AnswerError(e, question)
    response['quick_answer'] = get_quick_answer(response['answers'])
    return json_response(response)

#
# Server Boilerplate
#

middlewares = [
    exception_to_json_middleware,
    answer_exception_middleware,
    attach_uuid_middleware,
    log_qa_middleware,
]
app = web.Application(middlewares=middlewares)
app.add_routes(routes)

web.run_app(app,host='0.0.0.0',port=8080)
