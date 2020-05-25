#!/usr/bin/env python
# coding: utf-8

import os, json
import pandas as pd
import matplotlib.pyplot as plt
import csv
import re

v = 'ad 12,2x12,2x12,2 in'
match = re.search(r'(^([\d.]+)\s?(lbs|oz|g|kg)$)|\s+([\d.]+)\s?(lbs|oz|g|kg)\s+', str(v))
if match:
    match = re.search(r'(^(\d+(?:,\d+)?)x(\d+(?:,\d+)?)(?:x(\d+(?:,\d+)?))?\s?(cms|in|inch|inches|mms)$)|(\d+(?:,\d+)?)x(\d+(?:,\d+)?)(?:x(\d+(?:,\d+)?))?\s?(cms|in|inch|inches|mms)', v)
    print ('')

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
    
    value = str(value).lower();
    
    match = re.search(r'\d{4}-\d{2}-\d{2}', str(value))
    if match:
        value = re.sub(r'\d{4}-\d{2}-\d{2}', '', str(value))
        types.append('DateTime')
    
    # 12.55$
    match = re.search(r'(^(\$|£|€)?[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)(\$|£|€)?$)|(\s+(\$|£|€)?[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)(\$|£|€)?\s+)', str(value))
    if match:
        value = re.sub(r'(^(\$|£|€)?[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)(\$|£|€)?$)|(\s+(\$|£|€)?[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)(\$|£|€)?\s+)', '', str(value))
        types.append('Currency')

    # TODO: Add regex for this as well: 64.2cm W x 22.7cm D x 44.0cm H - Weight 8.2kg
    #'12.2x12.2x12.2 in'
    match = re.search(r'(^(\d+(?:,\d+)?)x(\d+(?:,\d+)?)(?:x(\d+(?:,\d+)?))?\s?(cms|in|inch|inches|mms)$)|(\d+(?:,\d+)?)x(\d+(?:,\d+)?)(?:x(\d+(?:,\d+)?))?\s?(cms|in|inch|inches|mms)', str(value))
    if match:
        value = re.sub(r'(^(\d+(?:,\d+)?)x(\d+(?:,\d+)?)(?:x(\d+(?:,\d+)?))?\s?(cms|in|inch|inches|mms)$)|(\d+(?:,\d+)?)x(\d+(?:,\d+)?)(?:x(\d+(?:,\d+)?))?\s?(cms|in|inch|inches|mms)', '', str(value))
        types.append('Dimensions')

    #'15kg'
    match = re.search(r'(^([\d.]+)\s?(lbs|oz|g|kg)$)|\s+([\d.]+)\s?(lbs|oz|g|kg)\s+', str(value))
    if match:
        value = re.sub(r'(^([\d.]+)\s?(lbs|oz|g|kg)$)|\s?([\d.]+)\s?(lbs|oz|g|kg)\s+', '', str(value))
        types.append('Weight')

    #'15mm'
    match = re.search(r'(^([\d.]+)\s?(mm|cm)$)|\s+([\d.]+)\s?(mm|cm)\s+', str(value))
    if match:
        value = re.sub(r'(^([\d.]+)\s?(mm|cm)$)|\s?([\d.]+)\s?(mm|cm)\s+', '', str(value))
        types.append('Length')

    #'15mm'
    match = re.search(r'(^([\d,]+)\s?(mm|cm)$)|\s+([\d,]+)\s?(mm|cm)\s+', str(value))
    if match:
        value = re.sub(r'(^([\d,]+)\s?(mm|cm)$)|\s?([\d,]+)\s?(mm|cm)\s+', '', str(value))
        types.append('Length')

    #'10:10'
    match = re.search(r'\d+\s?:\s?\d+', str(value))
    if match:
        value = re.sub(r'\d+\s?:\s?\d+', '', str(value))
        types.append('Ratio')

    match = re.search(r'([\d.]+)\s?(khz|hz)', str(value))
    if match:
        value = re.sub(r'([\d.]+)\s?(khz|hz)', '', str(value))
        types.append('Frequency')

    match = re.search(r'([\d.]+)\s?(W|w)', str(value))
    if match:
        value = re.sub(r'([\d.]+)\s?(W|w)', '', str(value))
        types.append('Power')

    ''' Important TODOs
    5. Think about other datatypes as well....
    '''    

    if len(value) > 0:
        types.append('String')
    
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
    keys = {}
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
    index = 0
    for root, dirs, files in os.walk('../data/data', topdown=False):
        for name in files:
            f = open(os.path.join(root, name), )
            jsondata = json.load(f)
            for key, value in jsondata.items():
                id = os.path.basename(root) + key
                
                if id not in keys:
                    keys[id] = []
                keys[id].append(index)
                
                data[index] = {}
                data[index]['Source'] = os.path.basename(root)
                data[index]['File'] = name
                data[index]['Attribute'] = remove_uncessary_tokens(key)                
                data[index]['Value'] = remove_uncessary_tokens(value)
                data[index]['Total Occurrences'] = 1
                data[index]['TargetAttribute'] = ''

                datatypes = getDataType(value)
                for dt in datatypes:
                    data[index][dt] = 1

                index = index + 1
            f.close()
    return data, keys;

# Reading ground-truth file.
def schemaMatching(data, keys):
    index = 0;
    jsonDataForNN = {}
    
    dt_grndtruthraw = pd.read_csv('../data/monitor_schema_matching_labelled_data.csv')
    dt_grndtruth = pd.DataFrame(dt_grndtruthraw['source_attribute_id,target_attribute_name'].str.split(',').tolist())
    
    # Merging ground-truth with filtered json data.
    dt_grndtruth[0] = dt_grndtruth[0].apply(lambda x: x.split('//'))
    for row in dt_grndtruth.values.tolist():
        id = row[0][0] + row[0][1]
        if id in keys:
            for value in keys[id]:
                data[value]['Total Occurrences'] = len(keys[id])
                data[value]['TargetAttribute'] = row[1]
                
                jsonDataForNN[index] = data[value]
                index = index + 1
            
    return data, jsonDataForNN

def mergeEntityResolutionInfo(data, pairs, groups):
    #TODO
    return data

jsonData, keys = extractJSONFiles()
pairs, groups = entityResolution()
jsonData, jsonDataForNN = schemaMatching(jsonData, keys)
jsonData = mergeEntityResolutionInfo(jsonData, pairs, groups)

df = pd.DataFrame(jsonDataForNN).T
df.to_csv('out.csv',index=False)
print(df)