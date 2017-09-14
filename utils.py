# -*- coding: utf-8 -*-

import sys
import unidecode
import re

def is_argument_present(key):
    return key in sys.argv

def get_argument_value(key, default=None):
    if key in sys.argv:
        return sys.argv[sys.argv.index(key) + 1]
    if default is None:
        sys.exit("Argument %s not specified" % key)
    return default


def replace_accented_characters_and_specials(text):
    text = unidecode.unidecode(text)
    text = re.sub(r'[^a-zA-Z0-9\s\']', ' ', text)
    return re.sub('\s+', ' ', text).strip()