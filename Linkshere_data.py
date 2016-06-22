
# coding: utf-8

# In[1]:

import csv
import pandas as pd
import requests
import json
import time
import sys
import os



# In[2]: Import functions

from all_functions import *
input_path = sys.argv[1]
output_path = sys.argv[2]
output_path_wofids = sys.argv[3]

# In[12]: Set path names

data = read_data(input_path)


# In[5]: Find all link incoming to names in input data

data_unique_names = find_unique(data,'wk:page')
all_names = combine_page_names(data_unique_names)
linkshere_dictionary={}
for name in all_names:
    request_data = request_API_linkshere_by_name(name)
    title_name, all_titles_linked = find_lks_name(request_data)
    linkshere_dictionary[title_name] = all_titles_linked
    time.sleep(10)


# In[13]: Save as json in file

with open(output_path, 'w') as outfile:
    json.dump(linkshere_dictionary, outfile)

dictionary_wof_linkshere={}
for key, value in linkshere_dictionary.iteritems():
    new_key=str(data[data['wk:page']==key]['id'].iloc[0])
    dictionary_wof_linkshere.update({new_key:value})


with open(output_path_wofids, 'w') as outfile_2:
    json.dump(dictionary_wof_linkshere, outfile_2)





