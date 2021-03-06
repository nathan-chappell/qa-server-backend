# util.py
"""
Hodgepodge of utility functions and cross script dependencies
"""
import re
import sys
from termcolor import colored
from typing import List, Set, DefaultDict
from collections import defaultdict
import asyncio
from asyncio import Lock
import logging

from elasticsearch import Elasticsearch # type: ignore
#from elasticsearch import NotFoundError, RequestError

# GLOBALS

# default name of elasticsearch index
INDEX_NAME = 'site-txt-stem'
ANALYZER_NAME = 'myanalyzer'
SOURCE_DIR = './mono-qa-knowledge-base'

es = Elasticsearch()
named_locks: DefaultDict[str,Lock] = defaultdict(Lock)
loop = asyncio.get_event_loop()

# Common Types:
Paragraph = str

#log_format = '[%(levelname)s]    %(filename)s:%(lineno)d:%(funcName)s  %(message)s'
class LoggingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        #print(f'formatting message: {message}')
        lines: List[str] = list(map(str.strip,re.split("\n", message)))
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
log.handlers[0].setFormatter(LoggingFormatter())

def highlight(string: str, to_highlight: Set[str], color: str) -> str:
    """Ansi-escape string so that all substrings appear with given color."""
    for word in to_highlight:
        if word == '': continue
        old = fr'\b({word})\b'
        new = colored(r'\1', color, attrs=['bold'])
        string = re.sub(old, new, string, flags=re.IGNORECASE)
    return string

def get_tokens_from_analyzer(text: str) -> Set[str]:
    analysis = es.indices.analyze(
        index=INDEX_NAME,
        body={'analyzer': ANALYZER_NAME, 'text': text}
    )
    return set([t['token'] for t in analysis['tokens']])

def print_paragraph(paragraph: Paragraph, query: str, answer: str):
    """Print the paragraph, query tokens red and the answer blue"""
    # TODO:
    # This highlighting algorithm is all messed up.  I need to deal with
    # overlapping query tokens and the answer text.  Shouldn't be too hard,
    # but I'll deal with it later.
    try:
        query_tokens = get_tokens_from_analyzer(query)
    # It would be better to determine all the errors that could possible occur
    # here and specify them, but there seems to be a real diverse set of
    # exceptions that elasticsearch will raise...
    #except (NotFoundError, RequestError, NewConnectionError):
        #...
    except Exception as e:
        print(f'got exception in print_paragraph:')
        print(e)
        query_tokens = set(re.split(r'\W+',query))
    blue_answer = colored(answer, 'blue', attrs=['bold'])
    paragraph_ = paragraph.replace(answer, blue_answer)
    print(' '*5 + f'query: {query}')
    print(' '*5 + f'answer: {answer}')
    words = paragraph_.split()
    line = ''
    for word in words:
        line += ' ' + word
        if (len(line) > 60):
            print(' '*15 + highlight(line, query_tokens, 'red'))
            line = ''
    if line != '': 
        print(' '*15 + highlight(line, query_tokens, 'red'))

# TODO
# make this suck less.
def answer_to_complete_sentence(answer: str, paragraph: str) -> str:
    r"""Convert an answer into a complete sentence.

    Given an answer extracted from a paragraph, return the complete sentence
    containing the answer.
    Right now a particularly naive approach is taken, simply looking for
    either \n\n or . before and after the position where the answer is found.
    """
    # We don't want to mess up the good work bert already did not finding the
    # answer.
    if answer == '': return ''
    a_start = paragraph.find(answer)
    if a_start == -1:
        # oh well...
        return answer
    # split sentences on period or double newline
    sentence_split = re.compile(r"\.|\n\n")
    split_spans = [m.span() for m in sentence_split.finditer(paragraph)]
    s_start = max([0] + [span[1] for span in split_spans if span[1] < a_start])
    s_end = min([sys.maxsize] + [span[0] + 1 for span in split_spans if span[0] >= a_start])
    return paragraph[s_start: s_end].strip()

if __name__ == '__main__':
    paragraph = """
The last line of the error message indicates what happened. Exceptions come in
different types, and the type is printed as part of the message: the types in
the example are ZeroDivisionError, NameError and TypeError. The string printed
as the exception type is the name of the built-in exception that occurred.
This is true for all built-in exceptions, but need not be true for
user-defined exceptions (although it is a useful convention). Standard
exception names are built-in identifiers (not reserved keywords).

The rest of the line provides detail based on the type of exception and what
caused it.

The preceding part of the error message shows the context where the exception
happened, in the form of a stack traceback. In general it contains a stack
traceback listing source lines; however, it will not display lines read from
standard input.
""" 
    qa_pairs = [
        { 'query': 'What details does the remainder of the line provide?',
          'answer': 'type of exception'
        },
        { 'query': 'What does the preceding part of the message show?',
          'answer': 'the context'
        },
    ]

    for qa_pair in qa_pairs:
        query = qa_pair['query']
        answer = qa_pair['answer']
        sentence = answer_to_complete_sentence(answer, paragraph)
        print_paragraph(paragraph, query, sentence)
