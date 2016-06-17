
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

import sys
sys.path.insert(0, 'C:\Users\Olga\Documents\MAPZEN_data\Projects\wikipedia-notebooks')
from all_functions import *


# ## import WOF concordances file that you want to get wikidata ids for

# In[38]:

input_path = "C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\all_geonames_wof.csv"
output_path = "C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\joid_ids_notnull.csv"
output_path_failed = "C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\all_failed_names.csv"
batch_length = 50 ##Set the batch length you want to do API queries for


# In[39]:

#(no_data=neither wd:id or wp:page, only_wd=only wd:id, only_wp=only wk:page, both_wd_wp=both)
data = read_data(input_path)
no_data, only_wd, only_wp, both_wd_wp = split_into_groups(data)


# ## Find wp:page for the ones that have wd:id

# In[53]:

result_id, names_failed = run_API_find_titles_in_batch(only_wd, batch_length) #API request for wikidata ids
all_dataframes_only_wd, dataframe_failed = combine_dataframes_from_batch(result_id, names_failed) #combine all batches into one dataframe
if len(dataframe_failed)!=0:
    all_dataframes_only_wp_second,names_cant_find = execute_titles_from_ids_one_by_one(data,dataframe_failed)
if len(all_dataframes_only_wp_second)!=0:
    all_dataframes_only_wd=all_dataframes_only_wd.append(all_dataframes_only_wp_second)


# ## Find wd:id for the ones that have wp:page

# In[56]:

result_wp, names_that_failed = run_API_find_ids_in_batch(only_wp,batch_length)
all_dataframes_only_wp, dataframe_names_failed = combine_dataframes_from_batch(result_wp, names_that_failed)
if len(dataframe_names_failed)!=0:
    all_dataframes_only_wp_second, names_cant_find = execute_ids_in_table_from_names_one_by_one(data,dataframe_names_failed)
if len(all_dataframes_only_wp_second)!=0:
    all_dataframes_only_wp=all_dataframes_only_wp.append(all_dataframes_only_wp_second)


# ## Merge all the new dataframes ( wd:id, wp:page, both)

# In[ ]:

frames = [all_dataframes_only_wd, all_dataframes_only_wp, both_wd_wp]
all_data_wd_wkpage = pd.concat(frames)
failed_frames = [dataframe_failed, dataframe_names_failed]
all_failed_frames = pd.concat(failed_frames)


# ## Join with original data and export - THIS IS THE NEW DATASET

# In[ ]:

joid_ids = data.join(all_data_wd_wkpage.set_index( [ 'wof:id' ] ), on='wof:id', how='left', rsuffix='_right')
joid_ids['wd:id'] = np.where(joid_ids['wd:id_right'].notnull(), joid_ids['wd:id_right'], joid_ids['wd:id'])
joid_ids['wk:page'] = np.where(joid_ids['wk:page_right'].notnull(), joid_ids['wk:page_right'], joid_ids['wk:page'])
new_data_joid_ids = joid_ids.drop(['gn:id_right','wd:id_right','wk:page_right','wk_page_id_right'],1)
new_data_joid_ids_unique_wof = find_unique(new_data_joid_ids, 'wof:id')
joid_ids_notnull = new_data_joid_ids_unique_wof[~(new_data_joid_ids_unique_wof['wd:id'].isnull() & new_data_joid_ids_unique_wof['wk:page'].isnull())]


# ## Export to a csv file

# In[ ]:

all_dataframes_only_wd.to_csv(output_path, encoding='utf-8')
all_failed_frames.to_csv(output_path_failed, encoding='utf-8')

