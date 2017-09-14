import utils
import re
import json

EXCLUDED_FILE = utils.get_argument_value('-excluded')
INCLUDED_FILE = utils.get_argument_value('-included')
OUTPUT_FILE = utils.get_argument_value('-o')

def get_dict(text, tag=None):
    data = {}
    end_position = 0
    current_tag = None
    regex = re.compile(r'^\s*[A-Z][A-Z0-9]{1,3}\s*[:−-]', re.MULTILINE)
    for m in regex.finditer(text):
        current_text = text[end_position:m.start()].strip()
        if current_tag in data.keys():
            data[current_tag].append(current_text)
        elif current_tag:
            data[current_tag] = [current_text]
        current_tag = text[m.start():m.end()-1].strip()
        end_position = m.end() + 1
    #GET last one
    current_text = text[end_position:].strip()
    if current_tag in data.keys():
        data[current_tag].append(current_text)
    elif current_tag:
        data[current_tag] = [current_text]
    return data
'POSIBLE MATCH \[(.+)\] \[(.+)\]: '
def split_ris(file_name):
    with open(file_name,'r') as f:
        text = f.read()
    ris_documents = re.split(r'ER  - ', text)
    if len(ris_documents) == 1:
        ris_documents = re.split(r'\r?\n\r?\n', text)
    if len(ris_documents) == 1:
        text = text.replace('\r', '\n')
        ris_documents = re.split(r'\n\n\n', text)
    #   ris_documents = [doc.replace('ER  -', '') for doc in ris_documents]
    return [doc.strip() for doc in ris_documents if doc.strip()]

def process_ris_entry(entry):
    _dict = get_dict(entry)
    document = {}
    found1 = re.search( 'Author 1 \[(.+)\]<br>Countries: \[(.*)\]<br>documents: \[(.+)\]<br><br><br>', _dict['AB'][0]).groups()
    found2 = re.search( '<br><br><br>Author 2 \[(.+)\]<br>Countries: \[(.*)\]<br>documents: \[(.+)\]', _dict['AB'][0]).groups()
    document['author1'] = {
        'name': found1[0],
        'countries': len(found1[1].split("', '")),
        'documents': found1[2].split(',')
    }
    document['author2'] = {
        'name': found2[0],
        'countries': len(found2[1].split("', '")),
        'documents': found2[2].split(',')
    }
    return document

included_docs = split_ris(INCLUDED_FILE)
excluded_docs = split_ris(EXCLUDED_FILE)
print('Included docs: %s' % len(included_docs))
print('Excluded docs: %s' % len(excluded_docs))
included_docs = [process_ris_entry(entry) for entry in included_docs]
excluded_docs = [process_ris_entry(entry) for entry in excluded_docs]

result_dict = {'replace': [], 'not_same': []}

found_authors = {}

for entry in included_docs:
    author_min = entry['author2']
    author_max = entry['author1']
    if len(entry['author1']['documents']) < len(entry['author2']['documents']):
        author_min = entry['author1']
        author_max = entry['author2']
    new_name = found_authors.get(author_max['name'], author_max['name'])
    result_dict['replace'].append({
        'author': author_min['name'],
        'document_number': [int(doc_number) for doc_number in author_min['documents']],
        'new_name': new_name
    })
    found_authors[entry['author1']['name']] = new_name
    found_authors[entry['author2']['name']] = new_name

for entry in excluded_docs:
    result_dict['not_same'].append({
        'author1': { 'name': entry['author1']['name'], 'documents': [int(doc_number) for doc_number in entry['author1']['documents']]},
        'author2': { 'name': entry['author2']['name'], 'documents': [int(doc_number) for doc_number in entry['author2']['documents']]},
    })

with open(OUTPUT_FILE, 'w') as outfile:
    json.dump(result_dict, outfile, sort_keys=True, indent=2)
#print(json.dumps(result_dict, sort_keys=True, indent=2))