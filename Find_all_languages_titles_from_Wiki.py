
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
output_path_wofids = sys.argv[3]
# ## Import dataset of interest :Needs a 'wk:page' column with wiki names
if os.path.isfile(output_path_names):
    print 'File %s already exists, skipping...' % output_path_names
    sys.exit()

wof_country_latest=read_data(input_path)


# In[5]:
if 'wk:page' in wof_country_latest.columns:
	pass
else:
	wof_country_latest['wk:page'] = wof_country_latest['name']

# ## Run language command to get all languages for all names in dataframe 

# In[6]:

language_dictionary, names_failed = execute_languages_in_dictionary_from_names(wof_country_latest)


# ## Export dictionary with languages to file 

# In[37]:

with open(output_path_names, 'w') as outfile:
    json.dump(language_dictionary, outfile)


# ## Create dictionary of languages with wof:id

# In[32]:

dictionary_wof_languages={}
for key, value in language_dictionary.iteritems():
    new_key=str(wof_country_latest[wof_country_latest['wk:page']==key]['id'].iloc[0])
    dictionary_wof_languages.update({new_key:value})


# In[36]:

with open(output_path_wofids, 'w') as outfile_2:
    json.dump(dictionary_wof_languages, outfile_2)


# In[ ]:



