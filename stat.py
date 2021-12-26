'''
Author: Xiaocong Yang
LastEditors: Xiaocong Yang
'''
# -*- coding: utf-8 -*-
from typing import final
from numpy.core.numeric import indices
import requests
import re
from bs4 import BeautifulSoup as BS
import pandas as pd
import json
from collections import Counter
from urllib.error import HTTPError
import numpy as np
from tqdm import trange
import argparse
import time


def regex(name):
    if name == "和田地区和田县" or name == "和田地区和田市":
        return name[-3:]
    
    name = name.strip()
    name = name.strip('\t')
    special = ['城区','矿区','郊区'] ## with which patterns can only use exact match
    for item in special:
        if item in name:
            return name
    
    with open('./ethicity.json','r') as f:
        ethnicities = json.loads(f.read())
    ethnicity = [item["name"] for item in ethnicities] + ["各族"]

    for eth in ethnicity:
        if eth in name:
            name = name.replace(eth,'')
            
    if '地区' in name:
        name = [item for item in name.split("地区") if len(item)>0][-1]
    if '市' in name:
        name = [item for item in name.split("市") if len(item)>0][-1]
    if '自治州' in name:
        name = [item for item in name.split("自治州") if len(item)>0][-1]
    
    if len(name) > 2:
        name = name.strip('区')
        name = name.strip('旗')
        name = name.strip('自治县')
        name = name.strip('省')
    
    if len(name) > 2:
        name = name.strip('县')
        
    return name

## crawl data in areas listed in indices
def craw_and_write(web_path_root, to_be_matched_list, start, end):
    global All_data
    if "tongjigongbao" in web_path_root:
        entry = 'stat_report'
    else:
        entry = 'gov_report'
    count_dict = {str(key):0 for key in to_be_matched_list}
    
    ## main loop
    for i in trange(start, end, 1):
        ## try to catch any Exception and continue to ensure a finally outcome
        try:
            web_path = f'{web_path_root}/{i}.html'
            r = requests.get(web_path).text.encode('ISO-8859-1').decode('gbk')
            title = re.findall(r'<div class="title"><h1>(.*?)</h1></div>',r)
            if len(title) > 0:  ## if not "404 Not Found" returned
                title = title[0]
                ## try matching title first. if matched then call for body content
                for k in range(len(to_be_matched_list)): ## item: (t, area)
                    item = to_be_matched_list[k]
                    if (str(item[0]) in title) and (item[1] in title) and item[1] != '河南': ## if matched
                        body = craw_single(web_path)
                        All_data.loc[k,entry] = (title+'\t'+body) ## data: (title, body)
                        count_dict[str(item)] += 1
                        break
        except requests.exceptions.ConnectionError:
            print('pause')
            time.sleep(1000)
            pass
        except:
            pass
            
            
    return count_dict

def craw_single(web_path):
    d = pd.read_html(web_path,flavor='bs4')
    body = d[0].iloc[:,0].values[np.argmax([len(str(item)) for item in d[0].iloc[:,0].values])]
    j = 2
    while True:
        try:
            e = pd.read_html(web_path[:-5]+f'_{j}'+'.html',flavor='bs4')
            if len(e)>0:
                data_ = e[0].iloc[:,0].values[np.argmax([len(item) for item in e[0].iloc[:,0].values])]
                body = body + data_
            j += 1
        except:
            return body

    
    
            
def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()
    return args



if __name__ == '__main__':
    args = parse()
    ## from scratch
    
    # All_data = pd.read_excel('./Alldata.xlsx',keep_default_na=False).drop(columns=['Unnamed: 0'],axis=1)
    # indices = []
    # for i in range(len(All_data)):
    #     line = All_data.iloc[i]
    #     indices.append((line['t'],regex(line['area'])))
    # All_data['index'] = indices
    # All_data['gov_report'] = ''
    # All_data['stat_report'] = ''
    
    ## from intermediate output
    
    All_data = pd.read_excel('./outputs/WITH_GOV_REPORT.xlsx',keep_default_na=False).drop(columns=['Unnamed: 0'],axis=1)
    indices = []
    for i in range(len(All_data)):
        line = All_data.iloc[i]
        indices.append((line['t'],regex(line['area'])))
    
    
    if args.debug:
        count_dict = craw_and_write('https://www.ahmhxc.com/tongjigongbao/',indices, 18965, 18980)
    else:
        count_dict = craw_and_write('https://www.ahmhxc.com/tongjigongbao/',indices, 10000, 22000)
        
    
    if args.debug:
        All_data.to_csv('./outputs/debug.csv')
    else:
        try:
            All_data.to_csv('./outputs/WITH_STAT_REPORT_v2.csv')
        except:
            pass
        
        try:
            All_data.to_excel('./outputs/WITH_STAT_REPORT_v2.xlsx')
        except:
            pass
    
    # ts = count_dict.items()
    # final_dict = {2:[],1:[],0:[]}
    # for item in ts:
    #     key = item[1]
    #     value = item[0]
    #     if key in final_dict.keys():
    #         final_dict[key].append(value)
        
    # with open('./outputs/stat_report.json','w') as g:
    #     json.dump(final_dict,g,ensure_ascii=False)
