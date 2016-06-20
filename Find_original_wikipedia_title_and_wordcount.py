
# coding: utf-8

# In[5]:

import csv
import pandas as pd
import requests
import json
import os


# In[6]:

import sys
from all_functions import *
DATA_PATH = 'all_data_not_concordances'
OUPUT_PATH = 'Language_outputs'

# ## Import dataset of interest :Needs a 'name' column with names
input_path = os.path.join(DATA_PATH, sys.argv[1])
output_path = os.path.join(OUPUT_PATH, sys.argv[2])

# In[8]:

wof_country_latest=read_data(input_path)


# In[9]:

countries_titles_wordcount=find_actual_title_wordcount(wof_country_latest)


# In[11]:

countries_titles_wordcount.to_csv(output_path, encoding='utf-8')


# In[ ]:









