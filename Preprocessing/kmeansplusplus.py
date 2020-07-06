import pandas as pd
import numpy as np
from scipy import stats

from sklearn.feature_extraction.text import TfidfVectorizer 
from pandas.core.frame import DataFrame
import pickle
from time import gmtime, strftime
from heapq import heappush
from heapq import heappop
import heapq

class KMeansPlusPlus:
    clusters = {}
    clustersDataFrame = None
    systemDataTypes = None

    tfidfVectorizerValues=TfidfVectorizer(min_df=0, max_features=1000000, token_pattern = r'\S+')
    tfidfVectorizerSourceAttributes=TfidfVectorizer()
    sourceAttributesDataFrame = None
    valuesDataFrame = None

    centroids = None
    fuzziness = 1
    #tfidfVectorsSourceAttributes = None
    #tfidfVectorsValues = None

    def __init__(self, data, systemDataTypes, fuzziness):
        self.systemDataTypes = systemDataTypes
        self.fuzziness = fuzziness
        self.trainModel(data)
    
    def trainModel(self, data):
        for index, row in data.iterrows():
            self.addDataToCluster(row, data)
    
        self.calculateMeanAndStD()
        self.calculateTfIds()
        
        self.calculateCentroid()
        
    def retrainModel(self):
        self.calculateMeanAndStD()
        self.calculateTfIds()

    def addDataToCluster(self, row, data):
        if not row['TargetAttribute'] in self.clusters:
            self.clusters[row['TargetAttribute']] = {}
            self.clusters[row['TargetAttribute']]['TargetAttribute'] = ''

            for datatype in self.systemDataTypes:
                self.clusters[row['TargetAttribute']][datatype] = 0
            
            self.clusters[row['TargetAttribute']]['SourceAttributes'] = ''
            self.clusters[row['TargetAttribute']]['Values'] = ''
            self.clusters[row['TargetAttribute']]['ValuesLength'] = []

        self.clusters[row['TargetAttribute']]['TargetAttribute'] = row['TargetAttribute']
        self.clusters[row['TargetAttribute']]['SourceAttributes'] += ' ' + row['Attribute']
        self.clusters[row['TargetAttribute']]['Values'] += ' ' + row['Value']
        self.clusters[row['TargetAttribute']]['ValuesLength'].append(len(row['Value']))
        
        for datatype in self.systemDataTypes:
            if datatype in data.columns:
                self.clusters[row['TargetAttribute']][datatype] = row[datatype]

    def addPredictedDataToCluster(self, targetAttributeName, sourceAttributeName, sourceAttributeValue, sourceAttributeDatatypes):
        if not targetAttributeName in self.clusters:
            self.clusters[targetAttributeName] = {}
            self.clusters[targetAttributeName]['TargetAttribute'] = ''
            
            for datatype in self.systemDataTypes:
                self.clusters[targetAttributeName][datatype] = 0

            self.clusters[targetAttributeName]['SourceAttributes'] = ''
            self.clusters[targetAttributeName]['Values'] = ''            
            self.clusters[targetAttributeName]['ValuesLength'] = []            
        self.clusters[targetAttributeName]['SourceAttributes'] += ' ' + sourceAttributeName
        self.clusters[targetAttributeName]['Values'] += ' ' + sourceAttributeValue
        self.clusters[targetAttributeName]['TargetAttribute'] = targetAttributeName
        self.clusters[targetAttributeName]['ValuesLength'].append(len(sourceAttributeValue))

        for datatype in self.systemDataTypes:
            if datatype in sourceAttributeDatatypes:
                self.clusters[targetAttributeName][datatype] = 1
    
    def calculateMeanAndStD(self):
        for key, value in self.clusters.items():
            self.clusters[key]['Mean'] = np.mean(self.clusters[key]['ValuesLength'])
            #self.clusters[key]['Mode'] = stats.mode(self.clusters[key]['ValuesLength'])
            self.clusters[key]['StD'] = np.std(self.clusters[key]['ValuesLength'])            
    
    def calculateTfIds(self):
        self.clustersDataFrame = pd.DataFrame(self.clusters).T
        #del self.clustersDataFrame['ValuesLength']
        #self.clustersDataFrame = self.clustersDataFrame.replace(np.nan, '', regex=True)
        
        tfidfVectorsSourceAttributes = self.tfidfVectorizerSourceAttributes.fit_transform(self.clustersDataFrame['SourceAttributes'])
        self.sourceAttributesDataFrame = pd.DataFrame(tfidfVectorsSourceAttributes.todense().tolist(), columns = self.tfidfVectorizerSourceAttributes.get_feature_names())
        
        tfidfVectorsValues = self.tfidfVectorizerValues.fit_transform(self.clustersDataFrame['Values'])    
        self.valuesDataFrame = pd.DataFrame(tfidfVectorsValues.todense().tolist(), columns = self.tfidfVectorizerValues.get_feature_names())
        
        #self.clustersDataFrame.to_csv('clusters.csv',index=False)
        #self.sourceAttributesDataFrame.to_csv('tfids_sourceattributes.csv',index=False)
        #self.valuesDataFrame.to_csv('tfids_values.csv',index=False)

    def getEnabledDataTypes(self, row):
        count = 0
        for datatype in self.systemDataTypes:
            if row[datatype].values[0] == 1:
                count = count + 1
        return count
        
    def calculateCentroid(self):
        '''
        self.centroids = [0] * len(self.clustersDataFrame)

        cntr = 0
        for i, row in self.sourceAttributesDataFrame.iterrows():
            self.centroids[cntr] = self.centroids[cntr] + np.sum(row)
            cntr = cntr + 1

        cntr = 0
        for i, row in self.valuesDataFrame.iterrows():
            self.centroids[cntr] = self.centroids[cntr] + np.sum(row)
            cntr = cntr + 1

        cntr = 0
        for i, row in self.clustersDataFrame.iterrows():
            self.centroids[cntr] = self.centroids[cntr] + np.sum(row[1:13])
            cntr = cntr + 1
            
        dfCentriods = pd.DataFrame(self.centroids)
        dfCentriods.to_csv('centroids.csv',index=False)
        '''
           
    def predict(self, attributeName, attributeValue, attributeDatatypes):
        _name_tokens = [] + attributeName.strip().split(' ')
        _value_tokens = [] + attributeValue.strip().split(' ')    
        
        _heap_clusters_distances = []
        
        _return_clusters_count = 0
                
        cntr = 0
        for i, row in self.clustersDataFrame.iterrows():
            _clusters_distances  = 0
                    
            for featureName in self.tfidfVectorizerSourceAttributes.get_feature_names():
                for _name in _name_tokens:
                    if str(featureName) == str(_name):
                        _clusters_distances +=  self.sourceAttributesDataFrame.loc[cntr, featureName]
    
            for featureName in self.tfidfVectorizerValues.get_feature_names():
                for _value in _value_tokens:
                    if str(featureName) == str(_value):
                        _clusters_distances += self.valuesDataFrame.loc[cntr, featureName]
            
            #if _clusters_distances[cntr] > 0:
            for datatype in attributeDatatypes:
                if datatype in row.index and row[datatype] == 1:
                    _clusters_distances += 1
                    _return_clusters_count = _return_clusters_count + 1

            std = float(row['StD'])
            if std > 0:
                _zvalue = abs((len(attributeValue) - float(row['Mean'])) / std)
                if _zvalue < 1:
                    _clusters_distances += ( 1 - _zvalue)
             
            heappush( _heap_clusters_distances, (_clusters_distances, cntr)) 
                 
            cntr = cntr + 1

        # membership matrix.
        '''
        _clusters_distances_difference = [0] * len(self.clustersDataFrame)
        cntr = 0
        while cntr < len(self.clustersDataFrame):
            _diff = abs(self.centroids[cntr] - _clusters_distances[cntr])
            if _diff > 0:
                _clusters_distances_difference[cntr] = (1/_diff) ** (1/(self.fuzziness - 1))
            else:
                _clusters_distances_difference[cntr] = 0
            cntr = cntr + 1;

        _total = np.sum(_clusters_distances_difference)
        
        _clusters_membership = [0] * len(self.clustersDataFrame)
        cntr = 0
        while cntr < len(self.clustersDataFrame):
            _clusters_membership[cntr] = _clusters_distances_difference[cntr] / _total
            cntr = cntr + 1;

        cntr = 0
        while cntr < len(self.clustersDataFrame):
            _clusters_distances[cntr] = _clusters_distances[cntr] * _clusters_membership[cntr] 
            cntr = cntr + 1 
        '''

        '''
        file = 'outfile' + strftime("%Y%m%d%H%M%S", gmtime()) 
        if dataTypeRow['TargetAttribute'] != self.clustersDataFrame.iloc[dataTypeArray.index(max(dataTypeArray)),2]:
            file = file + '____'
        file = file + '.csv'
        
        with open(file, 'w') as file_handler:
            file_handler.write( self.clustersDataFrame.iloc[dataTypeArray.index(max(dataTypeArray)),2] + "," + dataTypeRow['TargetAttribute'] + "," + dataTypeRow['Attribute'] + "," + dataTypeRow['Value'] + "\n")
            for i in range(1, len(dataTypeArray)):
                file_handler.write( self.clustersDataFrame.iloc[i,2] + "," + str(dataTypeArray[i]) + "\n")
        '''
        _matched_clusters = []

        #_first_matched_row = self.clustersDataFrame.iloc[[_clusters_membership.index(max(_clusters_membership))]]
        #_first_matched_row = self.clustersDataFrame.iloc[[_clusters_distances.index(max(_clusters_distances))]]
        #_clusters_distances.remove(max(_clusters_distances))
        #_datatypes_count = self.getEnabledDataTypes(_first_matched_row)
        
        heapq._heapify_max(_heap_clusters_distances) 
        
        while len(attributeDatatypes) > 0 and len(_heap_clusters_distances) > 0:
            _var = heapq._heappop_max(_heap_clusters_distances)
            _first_matched_row = self.clustersDataFrame.iloc[_var[1]]
            
            for datatype in attributeDatatypes:
                if datatype in _first_matched_row.index and _first_matched_row[datatype] == 1:
                    attributeDatatypes.remove(datatype)
                
                    if _first_matched_row['TargetAttribute'] not in _matched_clusters:
                        _matched_clusters.append(_first_matched_row['TargetAttribute'])                    
        
        
        #_matched_clusters.append(self.clustersDataFrame.iloc[[0]].TargetAttribute.values[0])
        '''
        while len(attributeDatatypes) > _datatypes_count:
            _next_matched_row = self.clustersDataFrame[self.clustersDataFrame.iloc[_clusters_distances.index(max(_clusters_distances)),0]]
            _clusters_distances.remove(max(_clusters_distances))
            
            _datatypes_count = _datatypes_count + self.getEnabledDataTypes(_first_matched_row)        
            _matched_clusters.append(_first_matched_row)
        ''' 
        return _matched_clusters
