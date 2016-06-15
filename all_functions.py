import csv
import pandas as pd
import requests
import json
import numpy as np
import wikipedia
import time



def read_data(path):
    data=pd.read_csv(path, header=0, sep=',')
    return data

def split_into_groups(data):
    no_data=[]
    only_wd = []
    only_wp=[]
    both_wd_wp = []
    for index, row in data.iterrows():
        if (isNaN(row['wd:id']) and isNaN(row['wk:page'])):
            no_data.append(row)
        elif (isNaN(row['wd:id'])==False and isNaN(row['wk:page'])):
            only_wd.append(row)
        elif (isNaN(row['wd:id']) and isNaN(row['wk:page'])==False):
            only_wp.append(row)
        else:
            both_wd_wp.append(row)

    no_data=pd.DataFrame(no_data)
    only_wd=pd.DataFrame(only_wd)
    only_wp=pd.DataFrame(only_wp)
    both_wd_wp=pd.DataFrame(both_wd_wp)

    return no_data, only_wd, only_wp, both_wd_wp  
    
def _finditem(obj, key):
    if key in obj: return obj[key]
    for k, v in obj.items():
        if isinstance(v,dict):
            item = _finditem(v, key)
            if item is not None:
                return item
            
def isNaN(num):
    return num != num

def get_id(row):
    wd_id=row['wd:id']
    return wd_id

def get_page_name(row):
    wp_page=row['wk:page']
    return wp_page

def parse_name_from_url(url):
    right_hand_side=url.rsplit('/',1)
    name=right_hand_side[1]
    return name

def combine_ids_for_API(data):
    only_wd_new = []
    all_ids=[]
    data.index=range(len(data))
    for index, row in data.iterrows():
        if index != len(data)-1:
            data_ids=get_id(row)+'|'
        else:
            data_ids=get_id(row)
        all_ids.append(data_ids)
    return all_ids

def combine_page_names(data):
    all_names=[]
    data.index=range(len(data))
    for index, row in data.iterrows():
        data_name=get_page_name(row)
        all_names.append(data_name)
    return all_names

def combine_page_names_for_API(names):
    all_names_API=[]
    for index, item in enumerate(names):
        if  index != len(names)-1:
            data_name=item+'|'
        else:
            data_name=item
        all_names_API.append(data_name)
        all_names_for_API="".join(all_names_API)
    return all_names_for_API, names

def find_duplicates(data, field):
    data_not_null=data[data[field].notnull()]
    duplicate_index=data_not_null.duplicated(field)
    are_duplicate=data_not_null[duplicate_index]
    duplicate_wd_ids=np.asarray(are_duplicate[field])
    data_duplicates=data.loc[data[field].isin(duplicate_wd_ids)]
    return data_duplicates

def find_unique(data, field):
    data_not_null=data[data[field].notnull()]
    duplicate_index=data_not_null.duplicated(field)
    unique=data_not_null[~duplicate_index]
    return unique

def request_API_title(all_ids):
    all_ids_for_API="".join(all_ids)
    request = "https://www.wikidata.org/w/api.php?action=wbgetentities&ids=%s&props=sitelinks/urls&sitefilter=enwiki&languages=en&format=json" %all_ids_for_API
    result_request=requests.get(request)
    data_request = json.loads(result_request.content)
    return data_request


def request_API_id_by_name(names_API, names):
    try:
        request = "https://en.wikipedia.org/w/api.php?action=query&prop=pageprops&titles=%s&format=json" %names_API
        result_request=requests.get(request)
        data_request = json.loads(result_request.content)
        names_failed=[]
    except ValueError:
        data_request='null'
        names_failed=names
    return data_request, names_failed

def find_titles(data):
    all_titles=[]
    data_entities=data['entities']
    for item in data_entities:
        split=data_entities[item]
        title = _finditem(split,'title')
        final=(item,title)
        all_titles.append(final)
        titles=pd.DataFrame(all_titles)
        titles.columns=['wd:id','wiki_page']
    return titles

def find_page_id_urls(data):
    all_urls=[]
    if data=='null':
        urls= "null"
    else:
        data_entities=data['query']['pages']
        for item in data_entities:
            split=data_entities[item]
            title = _finditem(split,'title')
            title_=title.replace(" ", "_")
            ids = _finditem(split,'wikibase_item')
            final=(item,title_,ids)
            all_urls.append(final)
            urls=pd.DataFrame(all_urls)
            urls.columns=['wk_page_id','wk_name','wiki_id']
    return urls

def execute_title_in_table_from_ids(data):
    ids_data=combine_ids_for_API(data)
    request_data=request_API_title(ids_data)
    all_urls=find_titles(request_data)
    result=data.merge(all_urls,on='wd:id')
    result['wk:page']=result['wiki_page']
    result=result.drop('wiki_page',1)
    return result


def execute_ids_in_table_from_names(data):
    all_names=combine_page_names(data)
    name_data_for_API, names=combine_page_names_for_API(all_names)
    request_data, names_that_failed=request_API_id_by_name(name_data_for_API, names)
    all_ids=find_page_id_urls(request_data)
    if len(all_ids)==4:
        wof_items_merge=[]
    else:
        wof_items_merge=data.join ( all_ids.set_index( [ 'wk_name' ] ), on=[ 'wk:page' ],rsuffix='_right' )
        wof_items_merge['wd:id']=wof_items_merge['wiki_id']
        wof_items_merge=wof_items_merge.drop(['wiki_id'],1)
    return wof_items_merge, names_that_failed


def execute_ids_in_table_from_names_one_by_one(data, names_that_failed):
    API_result_second_try=[]
    names_cant_find=[]
    for name in dataframe_failed:
        API_result,names_that_failed=request_API_id_by_name(name,name)
        all_ids=find_page_id_urls(API_result)
        if len(all_ids)==4:
            wof_items_merge=[]
        else:
            wof_items_merge=data.join ( all_ids.set_index( [ 'wk_name' ] ), on=[ 'wk:page' ], how='inner', rsuffix='_right' )
            wof_items_merge['wd:id']=wof_items_merge['wiki_id']
            wof_items_merge=wof_items_merge.drop(['wiki_id'],1)
        API_result_second_try.append(wof_items_merge)
        if len(names_that_failed)>0:
            names_cant_find.append(names_that_failed)
            
    dataframe_list_second_try=[]
    for i in range (len(API_result_second_try)):
        if len(API_result_second_try[i])==0:
            pass
        else:
            dataframe_list_second_try.append(API_result_second_try[i])
    all_dataframes_only_wp_second = pd.concat(dataframe_list_second_try)
    return all_dataframes_only_wp_second, names_cant_find

def run_API_find_titles_in_batch(data_with_ids, batch_size):
    result=[]
    names_failed=[]
    for i in range(0,len(data_with_ids)+1,batch_size):
        only_wd_batch=data_with_ids[i:i+batch_size]
        new_data = execute_title_in_table_from_ids(only_wd_batch)
        result.append(new_data)
    return result, names_failed

def run_API_find_ids_in_batch(data_with_titles, batch_size):
    result=[]
    names_failed=[]
    for i in range(0,len(data_with_titles)+1,batch_size):
        only_wp_batch=data_with_titles[i:i+batch_size]
        new_data,names_that_failed = execute_ids_in_table_from_names(only_wp_batch)
        result.append(new_data)
        names_failed.append(names_that_failed)
        time.sleep(10)
    return result, names_that_failed

def combine_dataframes_from_batch(batch_result, names_failed):
    dataframe_list=[]
    dataframe_failed=[]
    for i in range (len(batch_result)):
        if len(batch_result[i])==0:
            dataframe_failed=dataframe_failed+names_failed[i]
        else:
            dataframe_list.append(batch_result[i])
    all_dataframes_final = pd.concat(dataframe_list)
    return all_dataframes_final, dataframe_failed


def execute_linkshere_in_table_from_names(data):
    all_names=combine_page_names(data)
    linkshere_dictionary={}
    for name in all_names:
        request_data=request_API_linkshere_by_name(name)
        title_name,all_titles_linked=find_lks_name(request_data)
        linkshere_dictionary.update({name:all_titles_linked})
    return linkshere_dictionary

def request_API_linkshere_by_name(name):
    try:
        request = "https://en.wikipedia.org/w/api.php?action=query&prop=linkshere|pageprops&titles=%s&format=json" %name
        result_request=requests.get(request)
        data_request = json.loads(result_request.content)
    except ValueError:
        data_request='null'
    return data_request

def find_lks_name(request):
    all_titles_linked=[]
    data_entities=request_data['query']['pages']
    for item in data_entities:
        if item=='-1':
            name=data_entities[item]['title']
            all_titles_linked=[]
        else:
            split=data_entities[item]
            linkshere = _finditem(split,'linkshere')
            name=_finditem(split,'title')
            all_titles_linked=[]
            for entry in linkshere:
                title_linked=entry['title']
                all_titles_linked.append(title_linked)
    return name, all_titles_linked

def get_wiki_page_wiki_id_SPARQL_data(data):
    name_id=[]
    for index, line in data.iterrows():
        url=line['article']
        url_name=str(url)
        cid=line['cid']
        if url_name!='nan':
            wk_name=parse_name_from_url(url_name)
            line['wk:page']=wk_name
            wk_id = parse_name_from_url(cid)
            line['wd:id'] = wk_id
            name_id.append(line)
        else:
            name_id.append(line)
    name_id_df=pd.DataFrame(name_id)               
    return name_id_df

def fix_coordinates_SPARQL_data(data):
    dataframe_all=[]
    for index, line in data.iterrows():
        lat_lon=line['_coordinate_location']
        lat_lon_str=str(lat_lon)
        if lat_lon_str!='nan':
            lat_lon_values=lat_lon_str[lat_lon_str.find("(")+1:lat_lon_str.find(")")]
            lat_lon_values_split=lat_lon_values.rsplit(' ',1)
            if len(lat_lon_values_split)==1:
                dataframe_all.append(line)
            else:
                lat=lat_lon_values_split[1]
                lon = lat_lon_values_split[0]
                line['lat']=lat
                line['lon']=lon
                dataframe_all.append(line)
        else:
            dataframe_all.append(line)

    dataframe_all_df = pd.DataFrame(dataframe_all)
    return dataframe_all_df

def SPARQL_create_page_id_coordinates(data):
    id_page=get_wiki_page_wiki_id_SPARQL_data(data)
    dataframe_all_df=fix_coordinates_SPARQL_data(id_page)
    return dataframe_all_df