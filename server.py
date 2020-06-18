# server.py
"""
"""

from uuid import uuid4
from typing import Dict, Any, List

from aiohttp import web
from aiohttp.web import Request, Response, json_response
from aiohttp.web import HTTPInternalServerError
from markdown import markdown # type: ignore

from util import answer_to_complete_sentence
from transformer_query import gpu_pipeline
from create_index import get_paragraphs_for_query

routes = web.RouteTableDef()
readme_path = '../README.md'
css_path = './github.css'

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

def get_quick_answer(answers: List[Dict[str,Any]]) -> str:
    candidates = list(filter(lambda answer: answer['answer'] != '', answers))
    return candidates[0]['answer']

# TODO make this actually async at some point
async def get_answers(query: str) -> Dict[str,Any]:
    result: Dict[str,Any] = {'question': {'text': query, 'uuid': str(uuid4()) }}
    answers = []
    paragraphs = get_paragraphs_for_query(query, topk=5)
    for rank,paragraph in enumerate(paragraphs):
        context = paragraph['text']
        answer = gpu_pipeline({'question': query, 'context': context},
                           handle_impossible_answer=True,
                           topk=1)
        answers.append({
            'answer': answer_to_complete_sentence(answer['answer'],context),
            'rating': answer['score'],
            'paragraph': context,
            'paragraph_rank': rank,
            'docId': paragraph['_id'],
        })
    result['quick_answer'] = get_quick_answer(answers)
    answers = list(sorted(answers, key=lambda a: a['rating'], reverse=True))
    result['answers'] = answers
    return result

# TODO
#   * error handling
#   * logging
#
@routes.post('/question')
async def answer_question(request: Request) -> Response:
    try:
        body = await request.json()
        reply_body = await get_answers(body['question'])
        return json_response(reply_body)
    except Exception as e:
        print(e)
        return HTTPInternalServerError(reason='unknown')

app = web.Application()
app.add_routes(routes)

web.run_app(app,host='0.0.0.0',port=8080)
