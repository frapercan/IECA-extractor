# Python program to read
# json file
import os

import deepl

traductor = deepl.Translator("9d607fdb-762b-fe7a-2e55-d8b23bbc18a4:fx")
import json

# Opening JSON file
f = open('translation.json')

# returns JSON object as
# a dictionary
data = json.load(f)


# def recursive_items(dictionary):
#     for key, value in dictionary.items():
#         print(key,value)
#         if type(value) is dict:
#             yield from recursive_items(value)
#         else:
#             if key in ['label','title']:
#                 # traduccion = traductor.translate_text(value,target_lang='ES').text
#                 yield (key, 'hola')
#
#
#             yield (key, value)

import collections.abc

def traducir(dic,inicial,lista):
    for k,v in dic.items():
        if isinstance(v, collections.abc.Mapping):
            inicial[k] = traducir(dic.get(k, {}),v, lista)
        else:
            if k in ['label','title']:
                traduccion = traductor.translate_text(v,target_lang='ES').text
                print(traduccion)
                inicial[k] = traduccion
            else:
                inicial[k] = v

    return inicial

datos = traducir(data,{},['title','label'])

# datos = dict(recursive_items(data))
print(data)

with open("traducido.json", "w") as outfile:
    json.dump(data,outfile)
