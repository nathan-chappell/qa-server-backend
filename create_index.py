# index_all.py
"""
create an elasticsearch index containing all mono-site paragraphs

Run this script to create the index, and import the function
`get_paragraphs_for_query` to simply run queries.
"""

from pathlib import Path
from typing import List, Optional, Dict, Any, NamedTuple
from hashlib import md5
import asyncio

from elasticsearch import Elasticsearch # type: ignore

from util import Paragraph, INDEX_NAME, ANALYZER_NAME
from util import named_locks, es, loop, SOURCE_DIR, log

class ParagraphInfo(NamedTuple):
    text: str
    filename: str

async def get_paragraphs() -> List[ParagraphInfo]:
    """Turn return a list of strings - each .txt file from the directory"""
    async with named_locks['source_docs']:
        path = Path(SOURCE_DIR)
        result: List[ParagraphInfo] = []
        for filename in path.glob('*.txt'):
            print(f'processing: {filename}')
            #
            # TODO: check for encoding errors
            #
            with open(filename) as file:
                text = file.read()
                result.append(ParagraphInfo(text,filename.name))
        return result

def get_hash(s: Paragraph) -> str:
    """
    Return a hash of a site paragraph.

    This hash should be suitable for retrieving the exact paragraph later, and
    is intended to go into a `keyword` type field of the index.
    Currently uses md5.hexdigest
    """
    return md5(bytes(s, encoding='utf-8')).hexdigest()

# Make sure caller has a lock!
def create_index_with_stemmer(index: str):
    """Create index with name using custom text analysis."""
    with named_locks[index]:
        myanalyzer = {
            'type': 'custom',
            'tokenizer': 'standard',
            'filter': ['asciifolding','lowercase','porter_stem']
        }
        body = {
            'settings': {'analysis': {'analyzer': {ANALYZER_NAME: myanalyzer}}},
            'mappings': {'properties': {
                    'text': {'type': 'text', 'analyzer':'myanalyzer'},
                    'hash': {'type': 'keyword'}}}}
        es.indices.create(index=index,body=body)

async def index_all(index: str):
    """Create a new index and index all paragraphs.
    
    If the name of the index contains the string `stem`, it will be created
    using the function `create_index_with_stemmer`.
    """
    log.info(f'creating index named: {index}')
    async with named_locks[index]:
        if es.indices.exists(index=index):
            es.delete(index=index)
            log.info(f'deleted index: {index}')
        if 'stem' in index:
            create_index_with_stemmer(index)
        else:
            es.indices.create(index=index)
        data = await get_paragraphs()
        for paragraph,filename in data:
            # we were indexing with _id before in order to update the documents
            # rather than add new ones if we ran `index_all` more than once.
            # Probably not necessary anymore since we `delete_if_exists`.
            #
            # TODO: remove/ decide what to do
            #
            _id = filename
            _hash = get_hash(paragraph)
            body = {'text':paragraph, 'hash': _hash}
            es.index(index=index, body=body, id=_id)

async def get_paragraphs_for_query(
        query: str, index: str, topk=3
    ) -> List[Dict[str,Any]]:
    """Retrieve paragraphs from elasticsearch using query as search term.

    By default, uses the index `INDEX_NAME` and returns the top 3 results.
    """
    async with named_locks[index]:
        body = {'query':{'match':{'text':query}}, 'size':topk}
        reply = es.search(index=index, body=body)
        if reply['hits']['total']['value'] == 0:
            return []
        else:
            def get_hit(hit):
                return {'text': hit['_source']['text'], '_id': hit['_id']}
            return [get_hit(hit) for hit in reply['hits']['hits']]

if __name__ == '__main__':
    # directory containing the paragraphs for the site
    # this is a coroutine because someone else might be modifying the
    # paragraphs when we want to retrieve them...
    paragraphs = loop.run_until_complete(get_paragraphs())
    for p,fn in paragraphs:
        print('-'*10 + fn + '-'*10)
        print(f'len: {len(p.split()):4}')
        print(p[:40])
    index_all(index=INDEX_NAME)
