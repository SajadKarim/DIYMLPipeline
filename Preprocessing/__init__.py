#!/usr/bin/env python
# coding: utf-8

import os, json
import pandas as pd
import matplotlib.pyplot as plt
import csv
import re

# This method eradicates unnecessary tokens and characters for the stream.
def remove_uncessary_tokens(value):
        value = re.sub(r"[\n\t<>\(\)]+", "", str(value))
        return str(value);
allTypes=['Numeric','Boolean','String','DateTime','Currency','Dimensions','Weight','Ratio','FreqParameters','power']
# This method tries to deduce primitive and other datatypes that later can be considered as features.
def getDataType(value):
    types = []
    if ((str(value)).isnumeric()):
        types.append('Numeric')
        return types
    if( (str(value)).lower() in ['true', 'false', 'yes', 'no']):
        types.append('Boolean')
        return types

    types.append('String')
    
    value = str(value).lower();
    
    match = re.search(r'\d{4}-\d{2}-\d{2}', str(value))
    if match:
        types.append('DateTime')
    
    # 12.55$
    match = re.search(r'^(\$|£|€)?[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)(\$|£|€)?$', str(value))
    if match:
        types.append('Currency')

    #'12.2x12.2x12.2 in'
    match = re.search(r'(^[^x\d]+$)|(^.+-)|(\..+$)\s?(cms|in|inch|inches|mms)?', str(value))
    if match:
        types.append('Dimensions')

    #'15kg'
    match = re.search(r'([\d.]+)\s?(lbs|oz|g|kg)', str(value))
    if match:
        types.append('Weight')

    #'10:10'
    match = re.search(r'\d+\s?:\s?\d+', str(value))
    if match:
        types.append('Ratio')
    match = re.search(r'([\d.]+)\s?(KHz|Hz|hz)', str(value))
    if match:
        types.append('FreqParameters')
    match = re.search(r'([\d.]+)\s?(W|w)', str(value))
    if match:
        types.append('power')

    ''' Important TODOs
    Also refine the features (dateTime)
    5. Think about other datatypes as well....
    6. power
    7. frequency
    8. pixel resolution,
    9. Angles 
    '''    
    return types

data = {}
# This method read all the json files and push the extracted information to a dictionary.
'''
    data = {
        'key' : {                            // Key is concatenated value of 'site-name' and 'source-id'
            'Source' : ''
            'File' : ''
            'Frequency' : ''
            'Attribute' : ''
            'Value' : ['','',.....,'']    // Array of values - since any 'site-name' may have numerous values for a 'source-id', therefore, instead of creating-
                                             a separate entry at dictionary level, merging values at this level. 'Frequency' attribute in dictionary tells how-
                                             many times the respective 'source-id' has occured.
            'String' : 0
            'Numeric' : 0
            ..
            ....
            .....
            'Ratio' : 0
        },
        key-2 : { },
        key-3 : { },
        .... N-3 objects ....
        key-N : { }
    }

'''

for root, dirs, files in os.walk('../data/data', topdown=False):
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
#print(data)
# Reading ground-truth file.
dt_grndtruthraw = pd.read_csv('../data/monitor_schema_matching_labelled_data.csv')
dt_grndtruth = pd.DataFrame(dt_grndtruthraw['source_attribute_id,target_attribute_name'].str.split(',').tolist())
# Merging ground-truth with filtered json data.

grnd_data={}

dt_grndtruth[0] = dt_grndtruth[0].apply(lambda x: x.split('//'))
count=0
for row in dt_grndtruth.values.tolist():
    id = row[0][0] + row[0][1]
    if id in data:
        for x in data[id]['Value']:
            grnd_data[count]={}
            grnd_data[count]['Source'] = data[id]['Source']
            grnd_data[count]['File'] = data[id]['File'];
            grnd_data[count]['Frequency'] = data[id]['Frequency'];
            grnd_data[count]['Attribute'] = data[id]['Attribute'];
            grnd_data[count]['Value'] = x
            for dt in allTypes:
                if dt in data[id]:
                    grnd_data[count][dt] = data[id][dt]
                else:
                    grnd_data[count][dt] = 0
            grnd_data[count]['TargetAttribute'] = row[1]
            count+=1
        #print(data[id])
        data[id]['TargetAttribute'] = row[1]
        
df = pd.DataFrame(data);
df_g = pd.DataFrame(grnd_data);
dfT = df.T
dfT_g = df_g.T
dfT.to_csv('out.csv',index=False)
dfT_g.to_csv('out_grnd.csv',index=False)
#print(dfT)
