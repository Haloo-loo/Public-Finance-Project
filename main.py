import pandas as pd
import os
import re
import json



def regex(name):
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
    elif '自治州' in name:
        name = [item for item in name.split("自治州") if len(item)>0][-1]
    
    if len(name) > 2:
        name = name.strip('区')
        name = name.strip('旗')
        name = name.strip('自治县')
    if len(name) > 2:
        name = name.strip('县')
    return name


## fill in the blanks of source excel
def clean_excel(file):
    file.rename(columns={"Unnamed: 0":"entry","Unnamed: 1":"area"}, inplace = True)
    copy = None
    for i in range(len(file['entry'])):
        if file['entry'][i] != '':
            copy = file['entry'][i]
        else:
            file['entry'][i] = copy 
         
    return file

## create new columns in Alldata
def create_new_columns(file, new_entry, new_years):
    for item in new_entry:
        for year in new_years:
            file[f'{item}-{year}'] = ''
    return file
    

def add_new_data(source_file, target_line, entry, year, source_area):
    source_file.loc[source_area, f'{entry}-{year}'] = target_line[f'{year}']
    return source_file

## matching
def exact_match(query:str, keys:list):
    for key in keys:  ## exact match
        if regex(query) == regex(key):
            target = key
            return target
    return None

def fuzzy_match(query:str, keys:list):
    for key in keys:  ## fuzzy match
        if regex(query) in key:
            target = key
            return target
    return None


def match_and_write(exact:bool):
    global Alldata,keys,folder,done
    for file in set(os.listdir(folder)) :
        print(file)
        # if ('贵州' in file or '湖南' in file): ## for test only
        if file.endswith('xls') or file.endswith('xlsx'):
            df = pd.read_excel(folder+file, keep_default_na=False).iloc[:-3] # target file
            df = clean_excel(df)
            new_entry = []
            [new_entry.append(item) for item in df['entry'] if item not in new_entry]
            new_year = df.columns[2:]
            if not f'{new_entry[0]}-{new_year[0]}' in Alldata.columns:
                Alldata = create_new_columns(Alldata, new_entry,new_year)

            matched_keys = []
            for i in range(len(df)): ## within line
                current_entry = df.iloc[i]['entry']
                target = exact_match(df.iloc[i]['area'],keys) if exact else fuzzy_match(df.iloc[i]['area'],keys) ## single line
                if target is not None: ## if targeted
                    matched_keys.append(target)
                    ## check duplicating matches
                    if target in done:
                        pass
                    else:
                        if current_entry == '地方财政一般预算支出（万元）（2003年及之前为“财政总支出”）' or current_entry == '医院、卫生院床位数（床）':
                            ## the end of single file
                            done.append(target)
                        for year in new_year:
                            Alldata = add_new_data(Alldata,df.iloc[i],current_entry,year,target)
            ## drop the matched keys shown in current file to avoid duplicated match
            keys = list(set(keys).difference(set(matched_keys)))


def match_and_write_poverty_rate(exact:bool):
    global Alldata,keys,folder
    for file in set(os.listdir(folder)) :
        if file.endswith('xls') or file.endswith('xlsx'):
            print(file)
            # if ('贵州' in file or '湖南' in file): ## for test only
            df = pd.read_excel(folder+file, keep_default_na=False, header=1) # target file
            if '安徽' in file:
                df.rename(columns={year:f'{year}' for year in [2013,2014,2015,2016,2017,2018,2019]}, inplace=True)
            else:
                df.rename(columns={year:f'{year}' for year in [2013,2014,2015,2016,2017]}, inplace=True)
                df['2018'] = ''
                df['2019'] = ''
            df.rename(columns={"Unnamed: 0":"area"}, inplace = True)
            new_entry = ['贫困率']
            new_year = df.columns[1:]
            if not f'{new_entry[0]}-{new_year[0]}' in Alldata.columns:
                Alldata = create_new_columns(Alldata, new_entry,new_year)  
            
            matched_keys = []
            for i in range(len(df)): ## within line
                target = exact_match(df.iloc[i]['area'].strip(),keys) if exact else fuzzy_match(df.iloc[i]['area'].strip(),keys) ## single line  
                if target is not None:
                    matched_keys.append(target)
                    if target in done:
                        pass
                    else:
                        done.append(target)
                        for year in new_year:
                            Alldata = add_new_data(Alldata,df.iloc[i],'贫困率',year,target)
            keys = list(set(keys).difference(set(matched_keys)))
            


if __name__ == '__main__':
    ## initialize
    Poverty = pd.read_excel('./Poverty_county.xlsx')
    area = []
    for i in range(len(Poverty)):
        area.append(((Poverty.iloc[[i],[0,1]].values)[0])[1]+((Poverty.iloc[[i],[0,1]].values)[0])[0])
    Alldata = pd.DataFrame({'area':area})
    Alldata.set_index('area',inplace=True)
    
    ## matches
    folders = ['./six_index/','./Data_all_county/','./poverty_rate/']
    with open('not_matched.json','w') as g:
        for folder in folders:
            done = []
            keys = Alldata.index.values.tolist()
            if folder != './poverty_rate/':
                match_and_write(exact=True)
                match_and_write(exact=False)
                d = {f"{folder}":f"{keys}"}
                json.dump(d,g,ensure_ascii=False)
                g.write('\n')
            else:
                match_and_write_poverty_rate(exact=True)
                match_and_write_poverty_rate(exact=False)
                d = {f"{folder}":f"{keys}"}
                json.dump(d,g,ensure_ascii=False)
                g.write('\n')
                
    try:
        Alldata.drop(axis=1,columns=['贫困率-Unnamed: 6','贫困率-Unnamed: 7','贫困率-Unnamed: 8'],inplace=True)
    except:
        pass
    finally:
        Alldata.to_excel('Alldata_1008.xlsx')