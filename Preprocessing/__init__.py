#!/usr/bin/env python
# coding: utf-8

import os, json
import pandas as pd
import matplotlib.pyplot as plt
import csv
import re

def remove_uncessary_tokens(value):
        value = re.sub(r"[\n\t<>\(\)]+", "", str(value))
        return str(value);

def getDataType(value):
    types = []
    if ((str(value)).isnumeric()):
        types.append('Numeric')
        return types
    if( (str(value)).lower() in ['true', 'false', 'yes', 'no']):
        types.append('Boolean')
        return types

    types.append('String')
    
    match = re.search(r'\d{4}-\d{2}-\d{2}', str(value))
    if match:
        types.append('DateTime')
        
    ''' Important TODOs
    1. Amount
    2. Dimensions
    3. Weight
    4. Ratio
    5. etc....
    '''    
    return types

rootdir = '../data/data'

data = {}
index = 0

for root, dirs, files in os.walk(rootdir, topdown=False):
    for name in files:
        f = open(os.path.join(root, name), )
        jsondata = json.load(f)
        for key, value in jsondata.items():
            id = os.path.basename(root) + key
            if id not in data:                
                data[id] = {}
                data[id]['Source'] = os.path.basename(root)
                data[id]['File'] = name;
                data[id]['Frequency'] = 1;
                data[id]['Attribute'] = remove_uncessary_tokens(key);
                data[id]['Value'] = []
                data[id]['Value'].append(remove_uncessary_tokens(value))
            else:
                if not any(str(value) in s for s in data[id]['Value']):
                    data[id]['Frequency'] = data[id]['Frequency'] + 1
                    data[id]['Value'].append( remove_uncessary_tokens(value))
            datatypes = getDataType(value)
            for dt in datatypes:
                data[id][dt] = 1
        f.close()

dt_grndtruthraw = pd.read_csv('../data/monitor_schema_matching_labelled_data.csv')
dt_grndtruth = pd.DataFrame(dt_grndtruthraw['source_attribute_id,target_attribute_name'].str.split(',').tolist())

dt_grndtruth[0] = dt_grndtruth[0].apply(lambda x: x.split('//'))
for a in dt_grndtruth.values.tolist():
    id = a[0][0] + a[0][1]
    if id in data:
        data[id]['TargetAttribute'] = a[1]
        
df = pd.DataFrame(data);
dfT = df.T
dfT.to_csv('out.csv',index=False)
print(dfT)
