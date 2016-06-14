
# coding: utf-8

# In[1]:

import csv
import pandas as pd
import requests
import json
import numpy as np
import wikipedia
import time


# In[2]:

from all_functions import *


# In[3]:

input_path = "C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\new_data_joid_ids.csv"
data = read_data(input_path)


# In[7]:

data_unique_names = find_unique(data,'wk:page')
all_names = combine_page_names(data_unique_names)
linkshere_dictionary={}
for name in all_names:
    request_data = request_API_linkshere_by_name(name)
    title_name, all_titles_linked = find_lks_name(request_data)
    linkshere_dictionary[title_name] = all_titles_linked
    time.sleep(10)

