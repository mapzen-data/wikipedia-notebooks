
# coding: utf-8

# In[1]:

import os
import csv
import pandas as pd
import requests
import json

# In[2]:

import sys
from all_functions import *

input_path = sys.argv[1]
output_path_names = sys.argv[2]
output_path_ids = sys.argv[3]

# ## Import dataset of interest :Needs a 'wk:page' column with wiki names
if os.path.isfile(output_path_names) and os.path.isfile(output_path_ids):
    print 'File %s, %s already exists, skipping...' % (output_path_names,output_path_ids)
    sys.exit()

wof_latest=read_data(input_path)


# In[5]:
if 'wk:page' in wof_latest.columns:
	pass
else:
	wof_latest['wk:page'] = wof_latest['name']

# ## Run language command to get all languages for all names in dataframe 

# In[6]:

language_dictionary, names_failed = execute_languages_in_dictionary_from_names(wof_latest)


# ## Export dictionary with languages to file 

# In[37]:

with open(output_path_names, 'w') as outfile:
    json.dump(language_dictionary, outfile)


if 'id' in wof_latest.columns:
	pass
elif 'wof:id' in wof_latest.columns:
	wof_latest['id'] = wof_latest['wof:id']


mapping_dic_wof_name={}
for index, row in wof_latest.iterrows():
    key = row['wk:page']
    value = str(int(row['id']))
    mapping_dic_wof_name.update({key:value})

dictionary_wof_languages={}
for key, value in language_dictionary.iteritems():
    try:
        a = key.encode('utf8')
        new_key=mapping_dic_wof_name[a]
        dictionary_wof_languages.update({new_key:value})
    except UnicodeDecodeError:
        a = key
        new_key=mapping_dic_wof_name[a]
        dictionary_wof_languages.update({new_key:value})
    except KeyError:
    	pass
    	
# ## Create dictionary of languages with wof:id

with open(output_path_ids, 'w') as outfile:
    json.dump(dictionary_wof_languages, outfile)





