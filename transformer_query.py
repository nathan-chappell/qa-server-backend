# test.py

from pprint import pprint
from functools import partial
from typing import List
import re

from transformers import AutoModelForQuestionAnswering, AutoTokenizer # type: ignore
from transformers import QuestionAnsweringPipeline # type: ignore

from index_all import get_paragraphs_for_query

model_name = 'twmkn9/distilbert-base-uncased-squad2'

tokenizer = AutoTokenizer.from_pretrained(model_name)

# not currently used
#
# cpu_model = AutoModelForQuestionAnswering.from_pretrained(model_name)
# cpu_pipeline = QuestionAnsweringPipeline(model=cpu_model, tokenizer=tokenizer)

gpu_model = AutoModelForQuestionAnswering.from_pretrained(model_name)
gpu_model.cuda()
gpu_pipeline = QuestionAnsweringPipeline(model=gpu_model, tokenizer=tokenizer, device=0)

def query(
        _query: str,
        pipeline: QuestionAnsweringPipeline = gpu_pipeline,
        topk=5,
        ):
    paragraphs = get_paragraphs_for_query(_query, topk=topk)
    for paragraph in paragraphs:
        answer = pipeline({'question': _query, 'context': paragraph},
                           handle_impossible_answer=True,
                           topk=1)
        if answer['answer'] == '':
            print('no answer found')
        else:
            complete_sentence = answer_to_complete_sentence(answer['answer'], paragraph)
            print(f'score: {answer["score"]:4.3f}, answer: {complete_sentence}')
        print('paragraph:')
        print_paragraph(paragraph, query, answer['answer'])
        print('-'*40)
    print('*'*40)
    print(' '*40)
    print('*'*40)

