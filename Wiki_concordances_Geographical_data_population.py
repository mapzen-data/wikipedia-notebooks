
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


# ## Get the SPARQL data - run this command in SPARQL

# #### Change (with wd:Q839954 for archaeological sites, Q515 for cities, Q6256 for countries, Q3957 for towns, Q123705 for neighborhoods, Q532 for villages)
# PREFIX schema: <http://schema.org/>
# 
# SELECT ?cid ?name ?article ?_coordinate_location ?_elevation_above_sea_level ?_population ?_area WHERE {
#   ?cid wdt:P31 wd:Q3957.
#   OPTIONAL {
#     ?cid rdfs:label ?name.
#     FILTER((LANG(?name)) = "en")
#   }
#   OPTIONAL {
#     ?article schema:about ?cid.
#     ?article schema:inLanguage "en".
#     FILTER((SUBSTR(STR(?article), 1, 25)) = "https://en.wikipedia.org/")
#   }
#   OPTIONAL { ?cid wdt:P625 ?_coordinate_location. }
#   OPTIONAL { ?cid wdt:P2044 ?_elevation_above_sea_level. }
#   OPTIONAL { ?cid wdt:P1082 ?_population. }
#   OPTIONAL { ?cid wdt:P2046 ?_area. }
# }

# In[ ]:




# ## From wiki countries import wiki ID, wikipedia page, coordinates, elevation, population

# In[39]:

countries = read_data("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\SPARQL_countries.csv")


# In[40]:

countries_from_wiki=SPARQL_create_page_id_coordinates(countries)


# In[41]:

countries_from_wiki.to_csv("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\countries_wiki.csv")


# ## From wiki cities import wiki ID, wikipedia page, coordinates, elevation, population

# In[33]:

cities = read_data("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\SPARQL_cities.csv")


# In[34]:

cities_from_wiki = SPARQL_create_page_id_coordinates(cities)


# In[38]:

cities_from_wiki.to_csv("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\cities_wiki.csv")


# ## From wiki towns import wiki ID, wikipedia page, coordinates, elevation, population

# In[43]:

towns = read_data("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\SPARQL_towns.csv")
towns_from_wiki = SPARQL_create_page_id_coordinates(towns)


# In[44]:

towns_from_wiki.to_csv("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\towns_wiki.csv")


# In[ ]:




# ## From wiki archaeological sites import wiki ID, wikipedia page, coordinates, elevation, population

# In[52]:

SPARQL_archaeological_sites= read_data("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\SPARQL_archaelogical.csv")


# In[53]:

archaeological_sites_wiki=SPARQL_create_page_id_coordinates(SPARQL_archaeological_sites)


# In[54]:

archaeological_sites_wiki.to_csv("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\archaeological_sites_wiki.csv")


# In[ ]:




# ## From wiki villages import wiki ID, wikipedia page, coordinates, elevation, population

# In[45]:

villages = read_data("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\SPARQL_villages.csv")
villages_from_wiki = SPARQL_create_page_id_coordinates(villages)


# In[46]:

villages_from_wiki.to_csv("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\villages_wiki.csv")


# In[ ]:




# ## From wiki neighborhoods import wiki ID, wikipedia page, coordinates, elevation, population

# In[47]:

neighborhoods = read_data("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\SPARQL_neighborhoods.csv")
neighborhoods_from_wiki = SPARQL_create_page_id_coordinates(neighborhoods)


# In[48]:

neighborhoods_from_wiki.to_csv("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\neighborhoods_wiki.csv")


# In[ ]:




# ## Merge all the dataframes and join with concordances file 

# In[97]:

frames = [countries_from_wiki,cities_from_wiki,towns_from_wiki,archaeological_sites_wiki,villages_from_wiki,neighborhoods_from_wiki]
all_wiki_data=pd.concat(frames)


# In[98]:

wof_data_concordances=read_data("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\new_data_joid_ids.csv")
wof_data_concordances_unique=find_unique(wof_data_concordances,'wd:id')


# In[99]:

joid_wof_wiki=wof_data_concordances_unique.join(all_wiki_data.set_index( [ 'wd:id' ] ),on='wd:id' ,how='left', rsuffix='_right')
joid_wof_wiki_out=joid_wof_wiki[['wof:id','wd:id','wk:page','_area','_population','_elevation_above_sea_level','article','lat','lon']]


# In[107]:

joid_wof_wiki_unique=find_unique(joid_wof_wiki_out, 'wof:id')


# In[117]:

joid_wof_wiki_unique.to_csv("C:\Users\Olga\Documents\MAPZEN_data\Projects\Wiki\\concordances_wiki_wof_population.csv")


# In[ ]:



