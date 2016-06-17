
# coding: utf-8

# In[1]:

import csv
import pandas as pd
import requests
import json
import numpy as np


# In[2]:

import sys
sys.path.insert(0, 'C:\Users\Olga\Documents\MAPZEN_data\Projects\wikipedia-notebooks')
from all_functions import *


# ## Import dataset of interest :Needs a 'wk:page' column with wiki names

# In[35]:

input_path = "C:\Users\Olga\Documents\MAPZEN_data\whosonfirst-data\meta\\wof-country-latest.csv"
output_path_names = "C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\all_languages_country.json"
output_path_wofids = "C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\all_languages_country_wof.json"


# In[4]:

wof_country_latest=read_data(input_path)


# In[5]:

wof_country_latest['wk:page']=wof_country_latest['name']


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

with open(output_path_wofids, 'w') as outfile:
    json.dump(dictionary_wof_languages, outfile)


# In[ ]:



