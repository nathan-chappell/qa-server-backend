# index_all.py
"""
create an elasticsearch index containing all mono-site paragraphs

Run this script to create the index, and import the function
`get_paragraphs_for_query` to simply run queries.
"""

from pathlib import Path
from typing import List, Optional
from hashlib import md5

from elasticsearch import Elasticsearch # type: ignore

from util import Paragraph, INDEX_NAME, ANALYZER_NAME

# globals

es = Elasticsearch()

def get_paragraphs(paragraphs_dir: str, split_token: str) -> List[Paragraph]:
    """Return split paragraphs from site.
    
    arguments:
    paragraphs_dir - directory where paragraphs are located
    split_token - token used to split paragraphs in files
    """
    path = Path(paragraphs_dir)
    result = []
    for filename in path.glob('*.txt'):
        print(f'processing: {filename}')
        with open(filename,encoding='cp1250') as file:
            result.extend(file.read().split(SPLIT_TOKEN))
    result = list(map(str.strip,result))
    return result

def get_hash(s: Paragraph) -> str:
    """
    Return a hash of a site paragraph.

    This hash should be suitable for retrieving the exact paragraph later, and
    is intended to go into a `keyword` type field of the index.
    Currently uses md5.hexdigest
    """
    return md5(bytes(s, encoding='utf-8')).hexdigest()

def get_id(paragraph: Paragraph, index: str) -> Optional[str]:
    """Get the elasticsearch _id for a paragraph."""
    body = {'query': {'term': {'hash': get_hash(paragraph)}}}
    reply = es.search(index=index, body=body)
    if reply['hits']['total']['value'] > 0:
        return reply['hits']['hits'][0]['_id']
    else:
        return None

def delete_if_exists(index: str):
    if es.indices.exists(index=index):
        es.delete(index=index)

def create_index_with_stemmer(index: str):
    """Create index with name using custom text analysis."""
    delete_if_exists(index)
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

def index_all(paragraphs: List[Paragraph], index: str = INDEX_NAME):
    """Create a new index and index all paragraphs.
    
    If the name of the index contains the string `stem`, it will be created
    using the function `create_index_with_stemmer`.
    """
    if 'stem' in index:
        create_index_with_stemmer(index)
    else:
        delete_if_exists(index)
        es.indices.create(index=index)
    for paragraph in paragraphs:
        # we were indexing with _id before in order to update the documents
        # rather than add new ones if we ran `index_all` more than once.
        # Probably not necessary anymore since we `delete_if_exists`.
        #
        # TODO: remove/ decide what to do
        #
        _id = get_id(paragraph, index)
        _hash = get_hash(paragraph)
        es.index(index=index, id=_id, body={'text':paragraph, 'hash': _hash})

def get_paragraphs_for_query(query: str, index=INDEX_NAME, topk=3) -> List[Paragraph]:
    """Retrieve paragraphs from elasticsearch using query as search term.

    By default, uses the index `INDEX_NAME` and returns the top 3 results.
    """
    body = {'query':{'match':{'text':query}}, 'size':topk}
    reply = es.search(index=index, body=body)
    if reply['hits']['total']['value'] == 0:
        return []
    else:
        hits = reply['hits']['hits']
        return list(map(lambda hit: hit['_source']['text'], hits))

if __name__ == '__main__':
    # directory containing the paragraphs for the site
    paragraphs_dir = '../site_prepared'
    # the "paragraph splitter" that is located in the site paragraphs
    split_token = 'PARAGRAPH'
    paragraphs = get_paragraphs(paragraphs_dir, split_token)
    for p in paragraphs:
        print('-'*30)
        print(f'len: {len(p.split()):4}')
        print(p[:40])
    index_all(paragraphs, index=INDEX_NAME)
