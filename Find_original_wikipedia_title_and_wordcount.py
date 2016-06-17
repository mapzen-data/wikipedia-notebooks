
# coding: utf-8

# In[5]:

import csv
import pandas as pd
import requests
import json
import numpy as np


# In[6]:

import sys
sys.path.insert(0, 'C:\Users\Olga\Documents\MAPZEN_data\Projects\wikipedia-notebooks')
from all_functions import *


# ## Import dataset of interest :Needs a 'name' column with names

# In[7]:

input_path = "C:\Users\Olga\Documents\MAPZEN_data\whosonfirst-data\meta\\wof-country-latest.csv"
output_path = "C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\original_wiki_titles_wordcount_country.csv"


# In[8]:

wof_country_latest=read_data(input_path)


# In[9]:

countries_titles_wordcount=find_actual_title_wordcount(wof_country_latest)


# In[11]:

countries_titles_wordcount.to_csv(output_path, encoding='utf-8')


# In[ ]:









