
# coding: utf-8

# In[1]:

import csv
import pandas as pd
import requests
import json
#import numpy as np
import time
import os


# In[2]:

import sys
from all_functions import *


# ## import WOF file that you want to get wikidata ids for. Needs a 'wk:page' column and a 'wd:id' column - one of them filled and one with NaN values

# ## Import dataset of interest :Needs a 'name' column with names
input_path = sys.argv[1]
output_path = sys.argv[2]

if os.path.isfile(output_path):
    print 'File %s already exists, skipping...' % output_path
    sys.exit()
# In[3]:

batch_length = 50 ##Set the batch length you want to do API queries for


#(no_data=neither wd:id or wp:page, only_wd=only wd:id, only_wp=only wk:page, both_wd_wp=both)
data = read_data(input_path)
if 'wk:page' in data.columns:
    pass
else:
    data['wk:page'] = np.nan

if 'wd:id' in data.columns:
    pass
else:
    data['wd:id'] = np.nan

if 'wof:id' in data.columns:
    pass
else:
    data['wof:id'] = data['id']

no_data, only_wd, only_wp, both_wd_wp = split_into_groups(data)


# ## Find wp:page for the ones that have wd:id
if len(only_wd)!=0:
    result_id, names_failed = run_API_find_titles_in_batch(only_wd, batch_length) #API request for wikidata ids
    all_dataframes_only_wd, dataframe_failed = combine_dataframes_from_batch(result_id, names_failed) #combine all batches into one dataframe
    if len(dataframe_failed)!=0:
        all_dataframes_only_wp_second,names_cant_find = execute_titles_from_ids_one_by_one(data,dataframe_failed)
        if len(all_dataframes_only_wp_second)!=0:
            all_dataframes_only_wd=all_dataframes_only_wd.append(all_dataframes_only_wp_second)
else:
    all_dataframes_only_wd=only_wd


# ## Find wd:id for the ones that have wp:page

# In[56]:

if len(only_wp)!=0:
    result_wp, names_that_failed = run_API_find_ids_in_batch(only_wp,batch_length)
    all_dataframes_only_wp, dataframe_names_failed = combine_dataframes_from_batch(result_wp, names_that_failed)
    if len(dataframe_names_failed)!=0:
        all_dataframes_only_wp_second, names_cant_find = execute_ids_in_table_from_names_one_by_one(data,dataframe_names_failed)
        if len(all_dataframes_only_wp_second)!=0:
            all_dataframes_only_wp=all_dataframes_only_wp.append(all_dataframes_only_wp_second)
else:
    all_dataframes_only_wp=only_wp


# ## Merge all the new dataframes ( wd:id, wp:page, both)

# In[ ]:
frames=[]
if len(all_dataframes_only_wd)!=0:
    frames.append(all_dataframes_only_wd)
if len(all_dataframes_only_wp)!=0:
    frames.append(all_dataframes_only_wp)
if len(both_wd_wp)!=0:
    frames.append(both_wd_wp)

all_data_wd_wkpage = pd.concat(frames)


# ## Export to a csv file

# In[ ]:

all_data_wd_wkpage.to_csv(output_path, encoding='utf-8')

