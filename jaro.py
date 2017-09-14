# -*- coding: utf-8 -*-

from pyjarowinkler import distance
from multiprocessing import Pool
from collections import Counter
from countries import countries

import utils

import string
import json
import re
import os

print(distance.get_jaro_distance("hello", "haloa", winkler=True, scaling=.1))

C1_COLUMN_NUMBER = int(utils.get_argument_value('-c1', 8))
AUTHOR_COLUMN_NUMBER = int(utils.get_argument_value('-au', 0))
jaro_distance_min = float(utils.get_argument_value('-jaro_distance', 0.92))
IGNORE_COUNTRIES = utils.is_argument_present('-ignore_countries')
RUN_PARALLEL = utils.is_argument_present('-parallel')
EXPORT_AS_RIS = utils.is_argument_present('-ris')

csv_filename = utils.get_argument_value('-i', 'data/20170719muestra5k.csv')
#output_filename = utils.get_argument_value('-o')
authors_fix_filename = utils.get_argument_value('-authors_fix', 'data/authors_fix.json')

def extract_full_name_from_affiliations(author, affiliations_str):
    # if len(author.split()) > 2:
    #     print('WARNING; AUTHOR CON MAS DE DOS ESPACIOS: [%s]' % author)
    author = ', '.join(author.split(None, 1))
    regex_str = '(%s[A-Z ]*)' % ', '.join(author.split()).replace(')','\)').replace('(','\(')
    results = re.findall(regex_str, affiliations_str)
    if len(results) == 0:
        return author
    return max(results, key=len) # return longest

def extract_affilitation_country(affiliation):
    if not affiliation:
        return None
    for country in countries:
        if country['name'] in affiliation:
            return country.get('translation', country['name'])
    # affiliation_split = affiliation.split()
    # if len(affiliation_split) > 0:
    #     return affiliation_split[-1]
    # for word in affiliation.split():
    #     word = word.translate(None, string.punctuation)
    #     if word in countries:
    #         return word
    print("Not found!: ", affiliation)
    return None

def find_countries_for_user(author, affiliations):
    countries = []
    for affiliation in affiliations:
        if author in affiliation[0]:
            countries.append(affiliation[1])
    return countries

def load_authors_fix_file(filename):
    authors_fix = {}
    if os.path.isfile(authors_fix_filename):
        with open(authors_fix_filename) as data_file:    
            authors_list = json.load(data_file)
            for fix in authors_list['replace']:
                if fix['author'] not in authors_fix:
                    authors_fix[fix['author']] = {}
                if isinstance(fix['document_number'], list):
                    for doc_number in fix['document_number']:
                        authors_fix[fix['author']][doc_number] = fix
                else:
                    authors_fix[fix['author']][fix['document_number']] = fix
    return authors_fix

def load_not_same_author_fix(filename):
    authors_fix = {}
    if os.path.isfile(authors_fix_filename):
        with open(authors_fix_filename) as data_file:    
            authors_list = json.load(data_file)
            for fix in authors_list['not_same']:
                if fix['author1']['name'] not in authors_fix:
                    authors_fix[fix['author1']['name']] = {}
                for doc_number in fix['author1']['documents']:
                    if doc_number not in authors_fix[fix['author1']['name']]:
                        authors_fix[fix['author1']['name']][doc_number] = {}
                    if fix['author2']['name'] not in authors_fix[fix['author1']['name']][doc_number]:
                        authors_fix[fix['author1']['name']][doc_number][fix['author2']['name']] = {}
                    for doc_number2 in fix['author2']['documents']:
                        authors_fix[fix['author1']['name']][doc_number][fix['author2']['name']][doc_number2] = True
    return authors_fix


def complete_with_authors_fix(authors_fix, author, document_number):
    found = authors_fix.get(author['original_name'], authors_fix.get(author['name'], {})).get(document_number, {})
    found['used'] = True
    for key in ['countries']:
        if key in found:
            author[key] = found[key]
    author['name'] = found.get('new_name', author['name'])

def check_author_not_same_fix(authors_fix, author1_name, author1_docs, author2_name, author2_docs, reverse=True):
    if author1_name in authors_fix:
        for doc1 in author1_docs:
            if doc1 in authors_fix[author1_name]:
                if author2_name in authors_fix[author1_name][doc1]:
                    for doc2 in author2_docs:
                        if doc2 in authors_fix[author1_name][doc1][author2_name]:
                            return True
    if reverse:
        return check_author_not_same_fix(authors_fix, author2_name, author2_docs, author1_name, author1_docs, reverse=False)
    return False

def print_unused_authors_fix(authors_fix):
    for author_name in authors_fix:
        for doc_number in authors_fix[author_name]:
            if not authors_fix[author_name][doc_number].get('used'):
                print('Unused Author FIX: %s' % authors_fix[author_name][doc_number])

def load_authors(filename, authors_fix):
    extracted_authors = []
    doc_number = 0
    with open(filename, "r") as ins:
        next(ins) # skip first line
        for line in ins:
            line_split = line.split('\t')
            affiliations = line_split[C1_COLUMN_NUMBER].split(';')
            affiliations = [[affiliation, extract_affilitation_country(affiliation)] for affiliation in affiliations]
            # Sacar paises de cada autor, en un mismo documento sabesmos que ese autor es la misma persona, obtener todos los paises de esa persona
            authors = line_split[AUTHOR_COLUMN_NUMBER].split(';')
            #print([f[1] for f in affiliations])
            authors = [{'name': extract_full_name_from_affiliations(author, line_split[C1_COLUMN_NUMBER]), 'original_name': author} for author in authors]
            authors = [{'name': author['name'], 'original_name': author['original_name'], 'countries': find_countries_for_user(author['name'], affiliations)} for author in authors]
            for author in authors:
                complete_with_authors_fix(authors_fix, author, doc_number)
            extracted_authors.append(authors)
            doc_number += 1
    return extracted_authors

def build_author_document_index(authors):
    index = {}
    n = 0
    for document_authors in authors:
        for author in document_authors:
            if author['name'] not in index:
                index[author['name']] = {'countries': [], 'documents': []}
            index[author['name']]['documents'].append(n)
            index[author['name']]['countries'] += author['countries']
        n += 1
    return index

def split_numbers(n, times):
    splits = []
    for p in range(0, times):
        n_max = n // times if p < times -1 else n - (n // times * p)
        splits.append([n // times * p, n_max])
    return splits

def process_authors(start_n, n_max):
    possible_matches = []
    print(start_n, n_max)
    counts = {
        'total': 0,
        'posibles': 0,
        'descartados_mismo_doc': 0,
        'descartados_2nda_inicial': 0,
        'descartados_apellido': 0,
    }

    author_token_replace = {
        'junior': 'junior',
        'jshchnior': 'junior',
        'jr': 'junior',
        '2nd': 'ii',
        'ii': 'ii'
    }

    # TODO: Es inecesario probar todos con todos, usar un indice para los apellidos y los que tengan el mismo apellidos revisar la inicial del nombre
    for a1 in range(start_n, start_n + n_max):
        author1_compare = utils.replace_accented_characters_and_specials(author_names[a1])
        splitted_1 = [author_token_replace.get(sp, sp) for sp in author1_compare.split(' ')]
        splitted_1_no_replace = [sp for sp in splitted_1 if sp not in author_token_replace]
        splitted_1 = splitted_1_no_replace[0:1] + [sp for sp in splitted_1 if sp in author_token_replace] + splitted_1_no_replace[1:]
        #print('Trabajando author: %s' % author_names[a1])
        # if len(list(set(index[author_names[a1]]['countries']))) > 1:
        #     print('AUTOR CON MAS DE UN PAIS: [%s] - %s' % (author_names[a1], index[author_names[a1]]))
        for a2 in range(a1 + 1, len(author_names)):
            author2_compare = utils.replace_accented_characters_and_specials(author_names[a2])
            if check_author_not_same_fix(not_same_author_fix, author_names[a1], index[author_names[a1]]['documents'], author_names[a2], index[author_names[a2]]['documents']):
                print('Rechazado por estar en Not Same FIX! [%s] [%s]' % (author_names[a1], author_names[a2]))
            splitted_2 = [author_token_replace.get(sp, sp) for sp in author2_compare.split(' ')]
            splitted_2_no_replace = [sp for sp in splitted_2 if sp not in author_token_replace]
            splitted_2 = splitted_2_no_replace[0:1] + [sp for sp in splitted_2 if sp in author_token_replace] + splitted_2_no_replace[1:]

            min_splitted = splitted_1 if len(splitted_1) < len(splitted_2) else splitted_2
            max_splitted = splitted_1 if len(splitted_1) >= len(splitted_2) else splitted_2
            startswith_bad = False
            ratio = len(min_splitted[0]) / len(max_splitted[0])
            if ratio > 1:
                ratio = 1 / ratio
            startswith_bad = ratio < 0.7
            for idx_section, au1_section in enumerate(min_splitted):
                au2_section = max_splitted[idx_section]
                if not au1_section.startswith(au2_section) and not au2_section.startswith(au1_section):
                    startswith_bad = True
            if not startswith_bad:
                ## Good candidate
                #print('startswith_bad [%s] [%s]' % (min_splitted, max_splitted))
                counts['posibles'] += 1
                possible_matches.append([author_names[a1], author_names[a2], jaro_distance])
                continue

            jaro_distance = distance.get_jaro_distance(author1_compare, author2_compare, winkler=True, scaling=.001)
            if jaro_distance > jaro_distance_min:
                #if splitted_author_a1[0] == splitted_2[0]:
                if len(splitted_1) > 1 and len(splitted_2) > 1 and splitted_1[1][0] != splitted_2[1][0]:
                    counts['descartados_2nda_inicial'] += 1
                    #print('Descartados (2nda inicial): [%s] [%s]' % (author_names[a1], author_names[a2]))
                    continue
                # else:
                #     counts['descartados_apellido'] += 1
                #     print('Descartados (Apellido): [%s] [%s]' % (author_names[a1], author_names[a2]))
                #     continue
                # if bool(set(author_names[a1]) & set(author_names[a2])):
                #     counts['descartados_mismo_doc'] += 1
                #     print('Descartados (Ambos autores mismo documento): [%s] [%s]' % (author_names[a1], author_names[a2]))
                #     continue
                if not IGNORE_COUNTRIES:
                    if len(index[author_names[a1]]['countries']) > 0 and len(index[author_names[a2]]['countries']) > 0:
                        if not bool(set(index[author_names[a1]]['countries']) & set(index[author_names[a2]]['countries'])):
                            #print('DESCARTADOS por no tener pais en comun: [%s] [%s]' % (author_names[a1], author_names[a2]), index[author_names[a1]]['countries'], index[author_names[a2]]['countries'])
                            continue
                counts['posibles'] += 1
                possible_matches.append([author_names[a1], author_names[a2], jaro_distance])
                # print('POSIBLE MATCH [%s] [%s]: %s\n' % (author_names[a1], author_names[a2], jaro_distance))
                # print('\t\tAuthor1 [%s]\n\t\tcountries: %s\n\t\tdocuments: %s\n' % (author_names[a1], index[author_names[a1]]['countries'], index[author_names[a1]]['documents']))
                # print('\t\tAuthor2 [%s]\n\t\tcountries: %s\n\t\tdocuments: %s\n\n\n' % (author_names[a2], index[author_names[a2]]['countries'], index[author_names[a2]]['documents']))
        counts['total'] += 1
    return {'counts': counts, 'possible_matches': possible_matches}

def process_authors_with_dict():
    results = {
        'no_first_name': [],
        'possible_same_author': []
    }
    last_names = {}
    for author in index:
        splitted_name = author.split(',', 1)
        if splitted_name[0] not in last_names:
            last_names[splitted_name[0]] = []
        if len(splitted_name) == 1:
            # print('Sin apellido! [%s]' % author)
            # print(last_names[splitted_name[0]])
            results['no_first_name'].append({
                'author': author,
                'data': index[author],
                'others': last_names[splitted_name[0]]
            })
            continue
        found = False
        for first_names in last_names[splitted_name[0]]:
            reason = None
            if first_names.startswith(splitted_name[1]) or splitted_name[1].startswith(first_names):
                reason = 'startswith'
            else:
                jaro_distance = distance.get_jaro_distance(splitted_name[1], first_names, winkler=True, scaling=.001)
                if jaro_distance >= 0.7:
                    reason = 'jaro: %s' % jaro_distance
            if reason is not None:
                found = True
                results['possible_same_author'].append({
                    'type': reason,
                    'author1': {'name': author, 'data': index[author]},
                    'author2': {'name': ','.join([splitted_name[0], first_names]), 'data': index[','.join([splitted_name[0], first_names])]}
                })
                #print('Mismo Autor, startswith: [%s] [%s]' % (first_names, splitted_name[1]))
        if not found:
            last_names[splitted_name[0]].append(splitted_name[1])
    for candidate in results['no_first_name']:
        candidate['others'] = [{'name': ','.join([candidate['author'], o]), 'data': index[','.join([candidate['author'], o])]} for o in candidate['others']]
    results['no_first_name'] = [c for c in results['no_first_name'] if len(c['others']) > 0]
    return results



authors_fix = load_authors_fix_file(authors_fix_filename)
not_same_author_fix = load_not_same_author_fix(authors_fix_filename)
authors = load_authors(csv_filename, authors_fix)
print_unused_authors_fix(authors_fix)

index = build_author_document_index(authors)
# for author_name in index:
#     if len(index[author_name]['countries']) == 0:
#         print('NO_COUNTRY_AUTHOR [%s]: %s' % (author_name, index[author_name]))

print('AUTORES CON MAS DE UN PAIS: %s' % len([author_name for author_name in index if len(list(set(index[author_name]['countries']))) > 1]))
print('AUTORES SIN PAIS: %s' % len([author_name for author_name in index if len(index[author_name]['countries']) == 0]))
author_names = list(index.keys())
n_authors = len(author_names)
print('Loaded authors: %s' % n_authors)


results = []
if utils.is_argument_present('-jaro'):
    n_process = 4 # 4
    pool = Pool()
    authors_splits = split_numbers(n_authors, n_process)
    print('AUTHOR SPLITS: %s' % authors_splits)
    for author_split in authors_splits:
        if RUN_PARALLEL:
            results.append(pool.apply_async(process_authors, author_split))
        else:
            results.append(process_authors(author_split[0], author_split[1]))
else:
    possible_same_author = process_authors_with_dict()
    for index, p in enumerate(possible_same_author['possible_same_author']):
        print('Pareja %s:' % index)
        print('\tAuthor1 [%s]\n\tcountries: %s\n\tdocuments: %s\n\n' % (p['author1']['name'], p['author1']['data']['countries'], p['author1']['data']['documents']))
        print('\tAuthor2 [%s]\n\tcountries: %s\n\tdocuments: %s\n\n' % (p['author2']['name'], p['author2']['data']['countries'], p['author2']['data']['documents']))
    for index, p in enumerate(possible_same_author['no_first_name']):
        print('\n\nAutor sin primer nombre %s:' % index)
        print('\tAuthor [%s]\n\tcountries: %s\n\tdocuments: %s\n\n' % (p['author'], p['data']['countries'], p['data']['documents']))
        print('\tOthers:\n')
        for o in p['others']:
            print('\t\tAuthor2 [%s]\n\t\tcountries: %s\n\t\tdocuments: %s\n\n' % (o['name'], o['data']['countries'], o['data']['documents']))


total_counts = {
    'total': 0,
    'posibles': 0,
    'descartados_mismo_doc': 0,
    'descartados_2nda_inicial': 0,
}

if RUN_PARALLEL:
    results = [result.get() for result in results]

for result in results:
    total_counts = dict(Counter(result['counts']) + Counter(total_counts))

for result in results:
    for possible_match in result['possible_matches']:
        if EXPORT_AS_RIS:
            print('TI  - POSIBLE MATCH [%s] [%s]: %s' % (possible_match[0], possible_match[1], possible_match[2]))
            print('AB  - Author 1 [%s]<br>Countries: %s<br>documents: %s<br><br><br>Author 2 [%s]<br>Countries: %s<br>documents: %s' % (possible_match[0], index[possible_match[0]]['countries'], index[possible_match[0]]['documents'], possible_match[1], index[possible_match[1]]['countries'], index[possible_match[1]]['documents']))
            print('ER  -\n')
        else:
            print('POSIBLE MATCH [%s] [%s]: %s\n' % (possible_match[0], possible_match[1], possible_match[2]))
            print('\t\tAuthor1 [%s]\n\t\tcountries: %s\n\t\tdocuments: %s\n' % (possible_match[0], index[possible_match[0]]['countries'], index[possible_match[0]]['documents']))
            print('\t\tAuthor2 [%s]\n\t\tcountries: %s\n\t\tdocuments: %s\n\n\n' % (possible_match[1], index[possible_match[1]]['countries'], index[possible_match[1]]['documents']))


print(total_counts)

# for a1 in range(0, len(author_names)):
#   print('Trabajando author: %s' % author_names[a1])
#   for a2 in range(a1 + 1, len(author_names)):
#       jaro_distance = distance.get_jaro_distance(author_names[a1], author_names[a2], winkler=True, scaling=.001)
#       if jaro_distance > 0.9:
#           splitted_author_a1 = author_names[a1].split(' ')
#           splitted_author_a2 = author_names[a2].split(' ')
#           if splitted_author_a1[0] == splitted_author_a2[0] and splitted_author_a1[1][0] != splitted_author_a2[1][0]:
#               counts['descartados']['2nda_inicial'] += 1
#               print('Descartados (2nda inicial): [%s] [%s]' % (author_names[a1], author_names[a2]))
#               continue
#           if bool(set(author_names[a1]) & set(author_names[a2])):
#               counts['descartados']['mismo_doc'] += 1
#               print('Descartados (Ambos autores mismo documento): [%s] [%s]' % (author_names[a1], author_names[a2]))
#               continue
#           counts['posibles'] += 1
#           print('[%s] [%s]: %s' % (author_names[a1], author_names[a2], jaro_distance))
#   counts['total'] += 1
# print(counts)