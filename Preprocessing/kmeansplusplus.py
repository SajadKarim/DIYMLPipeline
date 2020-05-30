import pandas as pd
import numpy as np
from scipy import stats

from sklearn.feature_extraction.text import TfidfVectorizer 

clusters = {}
tfidfVectorizerSourceAttributes=TfidfVectorizer(use_idf=True)
tfidfVectorizerValues=TfidfVectorizer(use_idf=True)

#tfidfVectorsSourceAttributes = None
#tfidfVectorsValues = None

def trainModel(data, systemDataTypes):
    for key, value in data.items():
        if not value['TargetAttribute'] in clusters:
            clusters[value['TargetAttribute']] = {}
            clusters[value['TargetAttribute']]['SourceAttributes'] = ''
            clusters[value['TargetAttribute']]['Values'] = ''
            clusters[value['TargetAttribute']]['ValuesLength'] = []
        clusters[value['TargetAttribute']]['TargetAttribute'] = value['TargetAttribute']
        clusters[value['TargetAttribute']]['SourceAttributes'] += ' ' + value['Attribute']
        clusters[value['TargetAttribute']]['ValuesLength'].append(len(value['Value']))
        clusters[value['TargetAttribute']]['Values'] += ' ' + value['Value']
        
        for datatype in systemDataTypes:
            if datatype in data[key]:
                clusters[value['TargetAttribute']][datatype] = data[key][datatype]
            else:
                clusters[value['TargetAttribute']][datatype] = 0

    for key, value in clusters.items():
        clusters[key]['Mean'] = np.mean(clusters[key]['ValuesLength'])
        clusters[key]['Mode'] = stats.mode(clusters[key]['ValuesLength'])
        clusters[key]['StD'] = np.std(clusters[key]['ValuesLength'])

    dfClusters = pd.DataFrame(clusters).T
    del dfClusters['ValuesLength']
    dfClusters = dfClusters.replace(np.nan, '', regex=True)
    
    tfidfVectorsSourceAttributes = tfidfVectorizerSourceAttributes.fit_transform(dfClusters['SourceAttributes'])
    
    tfidfVectorsValues = tfidfVectorizerValues.fit_transform(dfClusters['Values'])

    return clusters, dfClusters, tfidfVectorsSourceAttributes, tfidfVectorsValues 

def predict(dfClusters, tfidfVectorsSourceAttributes, tfidfVectorsValues, name, datatype, value):
    nameArray = [] + name.strip().split(' ')
    valueArray = [] + value.strip().split(' ')

    dfSA = pd.DataFrame(tfidfVectorsSourceAttributes.toarray(), columns = tfidfVectorizerSourceAttributes.get_feature_names())    
    dfV = pd.DataFrame(tfidfVectorsValues.toarray(), columns = tfidfVectorizerValues.get_feature_names())
    
    dataTypeArray = [0] * len(dfClusters)
    
    try:
        cntr = 0
        for i, row in dfClusters.iterrows():
            dataTypeArray[cntr] = 0
            
            if row[datatype] == 1:
                dataTypeArray[cntr] = dataTypeArray[cntr] + 1
               
    
            rcdid = 0
            for featureName in tfidfVectorizerSourceAttributes.get_feature_names():
                for _name in nameArray:
                    if str(featureName) == str(_name):
                        #dataTypeArray[cntr] = dataTypeArray[cntr] + 1
                        dataTypeArray[cntr] = dataTypeArray[cntr] + dfSA.loc[cntr, featureName]
                rcdid = rcdid + 1
                #print(df['tfidf'][featureName])
    
            for featureName in tfidfVectorizerValues.get_feature_names():
                for _value in valueArray:
                    if str(featureName) == str(_value):
                        dataTypeArray[cntr] = dataTypeArray[cntr] + dfV.loc[cntr, featureName]
                
                #print(df['tfidf'][featureName])
    
            cntr = cntr + 1
    except (RuntimeError, TypeError, NameError):
        print('Error')
    ii = dataTypeArray.index(max(dataTypeArray))
    return dfClusters.iloc[ii,2]
