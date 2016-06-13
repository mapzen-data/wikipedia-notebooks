
# coding: utf-8

# In[10]:

import csv
import pandas as pd
import requests
import json
import numpy as np
import wikipedia
import time


# In[11]:

from all_functions import *


# In[12]:

input_data_path = "C:\Users\Olga\Documents\MAPZEN_data\whosonfirst-data\meta\\wof-concordances-latest.csv"
input_geonames_path = 'C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\geonames\\alternateNames.txt'
output_path = "C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\all_geonames_wof.csv"


# In[13]:

wof_data = read_data(input_data_path)
geonames_data = pd.read_csv(input_geonames_path, header=None, sep='\t')


# In[14]:

# From Geonames get entries with url data
geonames_link=geonames_data[geonames_data[2]=='link']
geonames_link.columns=['geo_id','a','type','link','b','c','d','e']


# In[15]:

# Get the name from the url of Geonames and populate 'name' column
geonames_data_names=[]
for index, line in geonames_link.iterrows():
    url=line['link']
    rhs=url.rsplit('/',1)
    name=rhs[1]
    line['name']=name
    geonames_data_names.append(line)
geonames_data_names=pd.DataFrame(geonames_data_names)


# In[16]:

#Geonames find duplicates and get rid of them
duplicate_index=geonames_data_names.duplicated('geo_id')
geonames_unique=geonames_data_names[~duplicate_index]


# In[19]:

## Join our data with Geonames
wof_items_merge=wof_data.join ( geonames_unique.set_index( [ 'geo_id' ] ),on=[ 'gn:id' ], how='left' )
wof_items_merge['wk:page']=np.where(wof_items_merge['wk:page'].isnull(),wof_items_merge['name'],wof_items_merge['wk:page'])
all_geonames_wof_wkpage=wof_items_merge.drop(['a', 'type', 'b', 'c', 'd', 'e', 'name'],1)


# In[ ]:

##Export data
all_geonames_wof_wkpage.to_csv(output_path, sep=',')


# In[ ]:



