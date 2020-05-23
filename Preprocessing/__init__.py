#!/usr/bin/env python
# coding: utf-8

import os, json
import pandas as pd
import matplotlib.pyplot as plt
import csv
import re

allTypes=['Numeric','Boolean','String','DateTime','Currency','Dimensions','Weight','Ratio','FreqParameters','power']
# This method eradicates unnecessary tokens and characters for the stream.
def remove_uncessary_tokens(value):
        value = re.sub(r"[\n\t<>\(\)]+", "", str(value))
        return str(value);

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
    match = re.search(r'(^[^x\d]+$)|(^.+-)|(\..+$)\s?(cms|in|inch|inches|mms)', str(value))
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
    5. Think about other datatypes as well....
    '''    
    return types


def entityResolution():
    pairs = {};
    groups = {};
    
    rows = pd.read_csv('../data/monitor_entity_resolution_labelled_data.csv')
    df = pd.DataFrame(rows['left_spec_id,right_spec_id,label'].str.split(',').tolist())
    
    groupCntr = 0
    
    for row in df.values.tolist():
        groupId1 = -1
        groupId2 = -1
        
        if row[2] != '1':
            continue
        
        if row[0] in pairs:
            groupId1 = pairs[row[0]]
        if row[1] in pairs:
            groupId2 = pairs[row[1]]

        if groupId1 == -1 and groupId2 == -1:
            groups[groupCntr] = [ row[0], row[1]]
            pairs[row[0]] = groupCntr;
            pairs[row[1]] = groupCntr;
            groupCntr = groupCntr + 1
        elif groupId1 == -1 and groupId2 != -1:
            pairs[row[0]] = groupId2
            groups[groupId2].append(row[0])
        elif groupId1 != -1 and groupId2 == -1:
            pairs[row[1]] = groupId1
            groups[groupId1].append(row[1])
        elif groupId1 != groupId2:
            groups[groupCntr] = groups[groupId1] + groups[groupId2]
            for val in groups[groupId1]:
                pairs[val] = groupCntr
            for val in groups[groupId2]:
                pairs[val] = groupCntr
            groups.pop(groupId1)
            groups.pop(groupId2)
            groupCntr = groupCntr + 1
            
    return pairs, groups

# This method read all the json files and push the extracted information to a dictionary.
def extractJSONFiles():
    data = {}
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
    return data;

# Reading ground-truth file.
grnd_data={}
def schemaMatching(data):
    count=0 
    dt_grndtruthraw = pd.read_csv('../data/monitor_schema_matching_labelled_data.csv')
    dt_grndtruth = pd.DataFrame(dt_grndtruthraw['source_attribute_id,target_attribute_name'].str.split(',').tolist())
    
    # Merging ground-truth with filtered json data.
    dt_grndtruth[0] = dt_grndtruth[0].apply(lambda x: x.split('//'))
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
            data[id]['TargetAttribute'] = row[1]
            
    return data

def mergeEntityResolutionInfo(data, pairs, groups):
    #TODO
    return data

jsonData = extractJSONFiles()
pairs, groups = entityResolution()
jsonData = schemaMatching(jsonData)
jsonData = mergeEntityResolutionInfo(jsonData, pairs, groups)

df = pd.DataFrame(jsonData).T
df.to_csv('out.csv',index=False)
print(df)

df_g = pd.DataFrame(grnd_data);
dfT_g = df_g.T
dfT_g.to_csv('out_grnd.csv',index=False)
print(dfT_g)
