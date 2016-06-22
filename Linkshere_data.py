
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

# In[12]: Set path names

data = read_data(input_path)


# In[5]: Find all link incoming to names in input data

data_unique_names = find_unique(data,'wk:page')
linkshere_dictionary={}
for i in range(0, len(data_unique_names),50):
    all_names_1=data_unique_names[i:i+50]
    linkshere_dictionary, names_failed = execute_linkshere_in_table_from_names(all_names_1)
    linkshere_dictionary_all.update(linkshere_dictionary)
    time.sleep(10)


# In[13]: Save as json in file

with open(output_path, 'w') as outfile:
    json.dump(linkshere_dictionary_all, outfile)







