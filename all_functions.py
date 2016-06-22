import csv
import pandas as pd
import requests
import json
import numpy as np
import io
import time



def read_data(path):
    data=pd.read_csv(path, header=0, sep=',')
    return data

def split_into_groups(data):
    no_data = data[data['wk:page'].isnull()&data['wd:id'].isnull()]
    only_wp = data[data['wk:page'].notnull()&data['wd:id'].isnull()]
    only_wd = data[data['wk:page'].isnull()&data['wd:id'].notnull()]
    both_wd_wp = data[data['wk:page'].notnull()&data['wd:id'].notnull()]
    return no_data, only_wd, only_wp, both_wd_wp

    
def finditem(obj, key):
    if key in obj: return obj[key]
    for k, v in obj.items():
        if isinstance(v,dict):
            item = finditem(v, key)
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
            data_ids=str(get_id(row))+'|'
        else:
            data_ids=str(get_id(row))
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
    all_names_for_API=[]
    for index, item in enumerate(names):
        if  index != len(names)-1:
            data_name=str(item)+'|'
        else:
            data_name=str(item)
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
    if data=='null':
        titles=[]
    elif 'error' in data.keys():
        titles=[]
    else:
        data_entities=data['entities']
        for item in data_entities:
            split=data_entities[item]
            title = finditem(split,'title')
            final=(item,title)
            all_titles.append(final)
            titles=pd.DataFrame(all_titles)
            titles.columns=['wd:id','wiki_page']
    return titles

def find_page_id_urls(data):
    all_urls=[]
    if data=='null':
        urls= "null"
    elif 'error' in data.keys():
        urls= "null"
    else:
        data_entities=data['query']['pages']
        for item in data_entities:
            split=data_entities[item]
            title = finditem(split,'title')
            ids = finditem(split,'wikibase_item')
            final=(item,title,ids)
            all_urls.append(final)
            urls=pd.DataFrame(all_urls)
            urls.columns=['wk_page_id','wk_name','wiki_id']
    return urls

def execute_title_in_table_from_ids(data):
    ids_data=combine_ids_for_API(data)
    request_data=request_API_title(ids_data)
    all_urls=find_titles(request_data)
    names_failed=[]
    if len(all_urls)!=0:
        result=data.merge(all_urls,on='wd:id')
        result['wk:page']=result['wiki_page']
        result=result.drop('wiki_page',1)
    else:
        result=[] 
        names_failed=ids_data
    return result, names_failed


def execute_ids_in_table_from_names(data):
    all_names=combine_page_names(data)
    name_data_for_API, names=combine_page_names_for_API(all_names)
    request_data, names_that_failed=request_API_id_by_name(name_data_for_API, names)
    all_ids=find_page_id_urls(request_data)
    
    return all_ids, names_that_failed

def execute_titles_from_ids_one_by_one(data, names_that_failed):
    API_result_second_try=[]
    ids_cant_find=[]
    all_dataframes_only_wp_second=[]
    for ids_data in names_that_failed:
        API_result=request_API_title(ids_data)
        all_names=find_titles(API_result)
        if len(all_names)==0:
            wof_items_merge=[]
            ids_cant_find.append(ids_data)
        else:
            API_result_second_try.append(all_names)
            
    dataframe_list_second_try=[]
    for i in range (len(API_result_second_try)):
        if len(API_result_second_try[i])==0:
            pass
        else:
            dataframe_list_second_try.append(API_result_second_try[i])
            all_dataframes_only_wp_second = pd.concat(dataframe_list_second_try)
    return all_dataframes_only_wp_second, ids_cant_find


def execute_ids_in_table_from_names_one_by_one(data, names_that_failed):
    API_result_second_try=[]
    names_cant_find=[]
    ll_dataframes_only_wp_second=[]
    for name in names_that_failed:
        API_result,names_that_failed=request_API_id_by_name(name,name)
        all_ids=find_page_id_urls(API_result)
        API_result_second_try.append(all_ids)
        if len(names_that_failed)>0:
            names_cant_find.append(names_that_failed)
            
    dataframe_list_second_try=[]
    for i in range (len(API_result_second_try)):
        if len(API_result_second_try[i])==0:
            pass
        else:
            dataframe_list_second_try.append(API_result_second_try[i])
    all_dataframes_only_wp_second = dataframe_list_second_try
    return all_dataframes_only_wp_second, names_cant_find

def run_API_find_titles_in_batch(data_with_ids, batch_size):
    result=[]
    ids_failed=[]
    for i in range(0,len(data_with_ids),batch_size):
        only_wd_batch=data_with_ids[i:i+batch_size]
        new_data, names_failed = execute_title_in_table_from_ids(only_wd_batch)
        result.append(new_data)
        if len(names_failed)!=0:
            for item in names_failed:
                new=item.replace("|", "")
                ids_failed.append(new)
    return result, ids_failed

def run_API_find_ids_in_batch(data_with_titles, batch_size):
    result=[]
    names_failed=[]
    for i in range(0,len(data_with_titles),batch_size):
        only_wp_batch=data_with_titles[i:i+batch_size]
        new_data,names_that_failed = execute_ids_in_table_from_names(only_wp_batch)
        result.append(new_data)
        names_failed.append(names_that_failed)
        time.sleep(10)
    return result, names_that_failed

def combine_dataframes_from_batch(batch_result, names_failed):
    dataframe_list=[]
    dataframe_failed=[]
    if len(batch_result)!=0:
        for i in range (len(batch_result)):
            if len(batch_result[i])==0 and len(names_failed)!=0:
                dataframe_failed=dataframe_failed+names_failed
            elif len(batch_result[i])==0 and len(names_failed)==0:
                dataframe_failed=dataframe_failed
            else:
                dataframe_list.append(batch_result[i])
        all_dataframes_final = pd.concat(dataframe_list)
    else:
        all_dataframes_final=batch_result
        dataframe_failed=names_failed
    return all_dataframes_final, dataframe_failed


def execute_linkshere_in_table_from_names(data):
    all_names=combine_page_names(data)
    linkshere_dictionary={}
    names_failed=[]
    for name in all_names:
        all_titles_linked_final=[]
        request_data=request_API_linkshere_by_name(name,'0')
        if request_data!='null':
            title_name,all_titles_linked=find_lks_name(request_data)
            all_titles_linked_final.extend(all_titles_linked)
            while request_data!='null' and request_data.keys()[0]!='batchcomplete':
                request_data=request_API_linkshere_by_name(name,request_data['continue']['lhcontinue'])
                if request_data!='null':
                    title_name,all_titles_linked=find_lks_name(request_data)
                    all_titles_linked_final.extend(all_titles_linked)
            linkshere_dictionary.update({name:all_titles_linked_final})
        else:
            names_failed.append(name)
    return linkshere_dictionary, names_failed

def request_API_linkshere_by_name(name,i):
    try:
        request = "https://en.wikipedia.org/w/api.php?action=query&prop=linkshere|pageprops&titles=%s&lhcontinue=%s&format=json" %(name,i)
        result_request=requests.get(request)
        data_request = json.loads(result_request.content)
    except ValueError:
        data_request='null'
    return data_request

def find_lks_name(request_data):
    all_titles_linked=[]
    if request_data=='null':
        name=[]
    elif 'error' in request_data.keys():
        name=[]
    else:
        data_entities=request_data['query']['pages']
        for item in data_entities:
            all_titles_linked=[]
            if item=='-1':
                name=data_entities[item]['title']
            else:
                split=data_entities[item]
                linkshere = finditem(split,'linkshere')
                name=finditem(split,'title')
                for entry in linkshere:
                    title_linked=entry['title']
                    all_titles_linked.append(title_linked)
    return name, all_titles_linked

def request_API_real_name(name):
    try:
        request = "https://en.wikipedia.org/w/api.php?format=json&action=query&list=search&srsearch=%s&srprop=wordcount&srlimit=1" %name
        result_request=requests.get(request)
        data_request = json.loads(result_request.content)
    except ValueError:
        data_request='null'
    return data_request


def find_actual_title_wordcount(data):
    new=[]
    for index, row in data.iterrows():
        name = row['name']
        data_request = request_API_real_name(name)
        if data_request=='null':
            new.append(row)
        elif 'error' in data_request.keys():
            new.append(row)
        else:
            for item in data_request['query']['search']:
                title = finditem(item,'title')
                wordcount=finditem(item,'wordcount')
                row['wk:page']=title
                row['wordcount'] = wordcount
                new.append(row)
        new_df=pd.DataFrame(new)
    return new_df

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

def request_API_name_all_languages(name):
    try:
        request = "https://en.wikipedia.org/w/api.php?action=query&titles=%s&prop=langlinks&format=json" %(name)
        result_request=requests.get(request)
        data_request = json.loads(result_request.content)
    except ValueError:
        data_request='null'
    return data_request

def request_API_name_all_languages_continue(name,i):
    try:
        request = "https://en.wikipedia.org/w/api.php?action=query&titles=%s&prop=langlinks&format=json&llcontinue=%s" %(name,i)
        result_request=requests.get(request)
        data_request = json.loads(result_request.content)
    except ValueError:
        data_request='null'
    return data_request

def find_languages_name(request_data):
    all_lang_linked={}
    if request_data=='null':
        name=" "
    elif 'error' in request_data.keys():
        name=" "
    else:
        data_entities=request_data['query']['pages']
        for item in data_entities:
            if item=='-1':
                name=" "
            else:
                split=data_entities[item]
                item=  finditem(split,'title')
                name = item
                if 'langlinks' in split.keys():
                    langlinks = finditem(split,'langlinks')
                    for entry in langlinks:
                        lang=entry['lang']
                        name_in_lang=entry['*']
                        all_lang_linked.update({lang:name_in_lang})
    return name, all_lang_linked

def execute_languages_in_dictionary_from_names(data):
    all_names=combine_page_names(data)
    all_titles_linked_final={}
    language_dictionary={}
    names_failed=[]
    for name in all_names:
        all_titles_linked_final={}
        request_data=request_API_name_all_languages(name)
        if request_data!='null':
            if 'error' in request_data.keys():
                names_failed.append(title_name)
            else:
                title_name,all_lang_linked=find_languages_name(request_data)
                all_titles_linked_final.update(all_lang_linked)
                while request_data!='null' and request_data.keys()[0]!='batchcomplete':
                    request_data=request_API_name_all_languages_continue(name,request_data['continue']['llcontinue'])
                    if request_data!='null':
                        if 'error' in request_data.keys():
                            names_failed.append(title_name)
                        else:
                            title_name,all_lang_linked=find_languages_name(request_data)
                            all_titles_linked_final.update(all_lang_linked)         
        else:
            names_failed.append(title_name)
        language_dictionary.update({name:all_titles_linked_final})
    return language_dictionary, names_failed