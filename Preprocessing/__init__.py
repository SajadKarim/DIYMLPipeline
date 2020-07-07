#!/usr/bin/env python
# coding: utf-8

import os, json
import pandas as pd
import matplotlib.pyplot as plt
import csv
import re    
import numpy as np
#import KMeansPlusPlus
from kmeansplusplus import KMeansPlusPlus
from scipy import stats
import random
from tqdm import tqdm

from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer 
 
from sklearn.model_selection import train_test_split
from sklearn.utils import shuffle
 
systemDataTypes =['Numeric','Boolean','String','DateTime','Currency','Dimensions','Weight','Scale','Ratio','Frequency','Power', 'Temperature', 'Colors']

comments = ['why', 'where', 'when', 'what', 'which', 'who', 'whose','whom']


# This method eradicates unnecessary tokens and characters for the stream.
def remove_uncessary_tokens(value):
        value = re.sub(r"[\n\t<>\(\),;]+", "", str(value))
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
    match = re.search(r'(^(\$|£|€)\s?[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)$)|(\s+(\$|£|€)\s?[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)\s+)|(^[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)\s?(\$|£|€)$)|(\s+[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)\s?(\$|£|€)\s+)', str(value))
    if match:
        value = re.sub(r'(^(\$|£|€)\s?[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)$)|(\s+(\$|£|€)\s?[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)\s+)|(^[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)\s?(\$|£|€)$)|(\s+[+-]?[0-9]{1,3}(?:[0-9]*(?:[.,][0-9]{1})?|(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|(?:\.[0-9]{3})*(?:,[0-9]{1,2})?)\s?(\$|£|€)\s+)', '', str(value))
        types.append('Currency')

    # TODO: Add regex for: 27"
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
        types.append('Scale')

    #'15mm'
    match = re.search(r'(^([\d,]+)\s?(mm|cm)$)|\s+([\d,]+)\s?(mm|cm)\s+', str(value))
    if match:
        value = re.sub(r'(^([\d,]+)\s?(mm|cm)$)|\s?([\d,]+)\s?(mm|cm)\s+', '', str(value))
        types.append('Scale')

    #'10:10'
    match = re.search(r'\d+\s?:\s?\d+', str(value))
    if match:
        value = re.sub(r'\d+\s?:\s?\d+', '', str(value))
        types.append('Ratio')

    match = re.search(r'([\d.]+)\s?(khz|hz)', str(value))
    if match:
        value = re.sub(r'([\d.]+)\s?(khz|hz)', '', str(value))
        types.append('Frequency')

    match = re.search(r'^([\d.]+)\s?(W|w)$|\s+([\d.]+)\s?(W|w)\s+', str(value))
    if match:
        value = re.sub(r'^([\d.]+)\s?(W|w)$|\s+([\d.]+)\s?(W|w)\s+', '', str(value))
        types.append('Power')

    ''' Important TODOs
    1. Temperature
    2. Colors
    ...
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
    counter=0
    for root, dirs, files in os.walk('../data/data', topdown=False):
        for name in files:
            f = open(os.path.join(root, name), )
            jsondata = json.load(f)
            for key, value in jsondata.items():
                id = os.path.basename(root) + key
                
                if key == '<page title>' or any(ext in key for ext in comments):
                    continue
                
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
                for dt in systemDataTypes:
                    if dt in datatypes: 
                        data[index][dt] = 1
                    else:
                        data[index][dt] = 0

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
#pairs, groups = entityResolution()
jsonData, jsonDataLabelled = schemaMatching(jsonData, keys)
#jsonData = mergeEntityResolutionInfo(jsonData, pairs, groups)


_groupeddata = []
_groupeddatatrain = []

_groupheader = []
_groupheader.append('Source')
_groupheader.append('Attribute')
_groupheader.append('TargetAttribute')
_groupheader.append('Value')
_groupheader.append('Avg')
for datatype in systemDataTypes:
    _groupheader.append(datatype)
#for datatype in _feature_array:
#    _groupheader.append(datatype)
_groupeddata.append(_groupheader)
_groupeddatatrain.append(_groupheader)

for key in keys:
    
    _groupinstance = []
    _groupinstance.append('')
    _groupinstance.append('')
    _groupinstance.append('')
    _groupinstance.append('')
    _groupinstance.append([])

    for datatype in systemDataTypes:
        _groupinstance.append(0)
    #for datatype in _feature_array:
     #   _groupinstance.append(0)
        
    for instance in keys[key]:
        _groupinstance[0]= jsonData[instance]['Source']  
        _groupinstance[1]= jsonData[instance]['Attribute']
        _groupinstance[2]= jsonData[instance]['TargetAttribute']
        _groupinstance[3]= _groupinstance[3] + ' ' + jsonData[instance]['Value']
        _groupinstance[4].append(len(jsonData[instance]['Value']))
        
        _index = 5
        for datatype in systemDataTypes:
            _groupinstance[_index] = int(_groupinstance[_index]) + int(jsonData[instance][datatype])
            _index = _index + 1
    
        _total = 0
        _index = 5
        for datatype in systemDataTypes:
            _total = _total + _groupinstance[_index]
            _index = _index + 1

        _index = 5
        for datatype in systemDataTypes:
            _groupinstance[_index] = (int(_groupinstance[_index]) / _total ) * 100
            _index = _index + 1
            
        '''for datatype in _feature_array:
            if datatype in jsonData[instance]:
                _groupinstance[_index] =  int(_groupinstance[_index]) + int(jsonData[instance][datatype])
                _index = _index + 1'''
    avg_val=np.mean(_groupinstance[4])
    #print(avg_val)
    _groupinstance[4]=avg_val
    val_arr=_groupinstance[3].split(" ")
    a_set=set(val_arr)
    list_of_strings = [str(s) for s in a_set]
    joined_string = " ".join(list_of_strings)
    _groupinstance[3]=joined_string
    
    
    
    if str(_groupinstance[2]):
        _groupeddatatrain.append(_groupinstance)
    else:
        _groupeddata.append(_groupinstance)
df = pd.DataFrame(_groupeddata)
new_header = df.iloc[0] #grab the first row for the header
df = df[1:] #take the data less the header row
df.columns = new_header
df=df.drop_duplicates(subset=['Source','Attribute'])
dfT = pd.DataFrame(_groupeddatatrain)
new_header = dfT.iloc[0] #grab the first row for the header
dfT = dfT[1:] #take the data less the header row
dfT.columns = new_header
dfT=dfT.drop_duplicates(subset=['Source','Attribute'])
print (df)
df.to_csv('mergedOut.csv',index=False)
dfT.to_csv('mergedOutTrain.csv',index=False)



'''
df = pd.DataFrame(jsonData).T
df.to_csv('complete_dataset.csv',index=False)
print(df)

df = pd.DataFrame(jsonDataLabelled).T
df = df.replace(np.nan, '', regex=True)
df.to_csv('labelled_dataset.csv',index=False)
'''
dfCompleteDataset = pd.DataFrame(jsonData).T
dfCompleteDataset = dfCompleteDataset.replace(np.nan, '', regex=True)
#dfCompleteDataset.to_csv('complete_dataset.csv',index=False)


dfLabelledDataset = pd.DataFrame(jsonDataLabelled).T
dfLabelledDataset = dfLabelledDataset.replace(np.nan, '', regex=True)
#dfLabelledDataset.to_csv('labelled_dataset.csv',index=False)

#for i in range(0,10):
dfLabelledDataset = shuffle(dfLabelledDataset)
trainData, testData = train_test_split(dfLabelledDataset, test_size=0.02)

kmeanspp = KMeansPlusPlus(trainData, systemDataTypes,1.25)
kmeanspp.trainModel(trainData)

#kmeanspp.clustersDataFrame.to_csv('clusters.csv',index=False)
#kmeanspp.sourceAttributesDataFrame.to_csv('tfids_sourceattributes.csv',index=False)
#kmeanspp.valuesDataFrame.to_csv('tfids_values.csv',index=False)

nmatch = 0
for index, row in tqdm(testData.iterrows()):
    attributeDataTypes = []
    for datatype in systemDataTypes:
        if datatype in row.index and row[datatype] == 1:
            attributeDataTypes.append(datatype)

    _matched_clusters = kmeanspp.predict(row['Attribute'], row['Value'], attributeDataTypes)
    if row['TargetAttribute'] in _matched_clusters:
        nmatch = nmatch + 1
    #print('actual: ' + row['TargetAttribute'] + ' --- predicted: ' + dataTypeArray)
    for _targetattribute in _matched_clusters:
        kmeanspp.addPredictedDataToCluster(_targetattribute, row['Attribute'], row['Value'], attributeDataTypes)

    kmeanspp.retrainModel()
    
print('total test records: ' + str(len(testData.index)) + ', successful matches: ' + str(nmatch))
print('success %: ' + str( (nmatch/len(testData.index)) * 100))

#total_recs = len(dfCompleteDataset.index)
f = open('output.csv', 'w', buffering = 1)
f.write("source_attribute_id, target_attribute_name\n")

dfCompleteDataset= pd.read_csv("cleaned2.csv")

#dfCompleteDataset=dfCompleteDataset.dropna(axis=0, how="any")
print(dfCompleteDataset)
for index, row in tqdm(dfCompleteDataset.iterrows()):
    if not row['TargetAttribute']:
        attributeDataTypes = []
        for datatype in systemDataTypes:
            if datatype in row.index and row[datatype] > 0:
                attributeDataTypes.append(datatype)

        _matched_clusters = kmeanspp.predict_(row['Attribute'], row['Value'],row['Avg'], attributeDataTypes)
        
        for _targetattribute in _matched_clusters:
            f.write(row['Source'] + '//' + row['Attribute'] + ',' + _targetattribute + '\n')
            #kmeanspp.addPredictedDataToCluster(_targetattribute, row['Attribute'], row['Value'], attributeDataTypes)
        
        kmeanspp.retrainModel()

        if len(_matched_clusters)>0:
            row['TargetAttribute'] = _matched_clusters[0]
        #print(str(index) + '/' + str(total_recs))

#dfCompleteDataset.to_csv('clusters_with_results.csv',index=False)
f.close()
