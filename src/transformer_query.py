# test.py

from pprint import pprint
from functools import partial
from typing import List, Dict, Any
import re

from transformers import AutoModelForQuestionAnswering, AutoTokenizer # type: ignore
from transformers import QuestionAnsweringPipeline # type: ignore

from util import INDEX_NAME
from util import answer_to_complete_sentence, print_paragraph, loop
from create_index import get_paragraphs_for_query

model_name = 'twmkn9/distilbert-base-uncased-squad2'

tokenizer = AutoTokenizer.from_pretrained(model_name)

# not currently used
#
# cpu_model = AutoModelForQuestionAnswering.from_pretrained(model_name)
# cpu_pipeline = QuestionAnsweringPipeline(model=cpu_model, tokenizer=tokenizer)

model = AutoModelForQuestionAnswering.from_pretrained(model_name)
#model.cuda()
model.eval()
#pipeline = QuestionAnsweringPipeline(model=model, tokenizer=tokenizer, device=0)
pipeline = QuestionAnsweringPipeline(model=model, tokenizer=tokenizer, device=-1)

def query(
        _query: str,
        pipeline: QuestionAnsweringPipeline = pipeline,
        topk=5,
        ):
    """query intended for use at the command line"""
    paragraph_coro = get_paragraphs_for_query(_query, INDEX_NAME, topk=topk)
    paragraphs: List[Dict[str,Any]] = loop.run_until_complete(paragraph_coro)
    for paragraph in paragraphs:
        context = paragraph['text']
        answer = pipeline({'question': _query, 'context': context},
                           handle_impossible_answer=True,
                           topk=1)
        if answer['answer'] == '':
            print('no answer found')
        else:
            complete_sentence = answer_to_complete_sentence(answer['answer'], context)
            print(f'score: {answer["score"]:4.3f}, answer: {complete_sentence}')
        print('paragraph:')
        print_paragraph(context, _query, answer['answer'])
        print('-'*40)
    print('*'*40)
    print(' '*40)
    print('*'*40)

