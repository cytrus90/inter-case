
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sys
import os
import subprocess
import time as ti
from datetime import datetime

pd.options.display.max_columns = 10000


# In[2]:


dataRawPath = os.path.abspath('Data/Road_MasterData.csv')
dataPathSave = os.path.abspath('Data/Road_Frequency.csv')
pathSave = os.path.abspath('Data/data3Frequency.csv')


# In[3]:


dataRaw = pd.read_csv(dataRawPath, index_col='ID')
dataRaw.head()


# # Encoding:

# In[5]:


events = dataRaw.groupby('Activity').size().sort_values()
for index in range(0,len(events)):
    dataRaw[events.index[index]] = 0


# In[6]:


def Frequency(dataFrame):
    rows = dataFrame.shape[0]
    for index in range(0,rows):
        case = dataFrame.at[index,'ID_Num']
        for i in range(index,rows):
            caseNow = dataFrame.at[i,'ID_Num']
            if (case == caseNow):
                dataFrame.at[i,dataFrame.at[index,'Activity']] = (dataFrame.at[i,dataFrame.at[index,'Activity']]+1)
            else:
                break
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f) + '%')
            sys.stdout.flush()


# In[7]:


Frequency(dataRaw)


# In[10]:


#Bring Order to the Columns:
RAM = pd.DataFrame()
RAM['case'] = dataRaw['CASE_concept_name']
RAM['case_id'] = dataRaw['ID_Num']
RAM['activity'] = dataRaw['Activity']
RAM['activityID'] = dataRaw['Activity_ID']
RAM['sTime'] = dataRaw['timestamp']
RAM['timeInt'] = dataRaw['TimeSinceInt']
RAM['resFac'] = dataRaw['Resource_ID']
RAM['Event_Number'] = dataRaw['Event_Number']

events = dataRaw.groupby('Activity').size().sort_values()
for index in range(0,len(events)):
    RAM[events.index[index]] = dataRaw[events.index[index]]
    
RAM['IsLastPayment'] = dataRaw['IsLastPayment']

RAM['N_act'] = dataRaw['N_act']
RAM['N_a'] = dataRaw['NaNumAct']
RAM['N_ar'] = dataRaw['N_ar']
RAM['Uc_ar'] = dataRaw['Uc']


# In[12]:


RAM.to_csv(dataPathSave)
#dataRaw = pd.read_csv(pathDataSave, sep=',')


# In[14]:


data = pd.DataFrame()
data = RAM


# # Filter for Third Activity:

# In[15]:


num = 4


# In[16]:


# th -> Number of done Activities before prediciton is done
# dataMatrix data.as_Matrix() array
# posCase -> position of the case ID (e.g.: dataMatrix[i][1])
def PreTestSet(dataMatrix, th, posCase):
    rows = len(dataMatrix)
    diff = list()
    n = 1
    case_1 = dataMatrix[0][posCase]
    for index in range(1,rows):
        case = dataMatrix[index][posCase]
        if case_1 == case:
            n = n+1
            if n == th:
                diff.append(index)
        else:
            n = 1
        case_1 = case
        
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f) + '%')
            sys.stdout.flush()
    return diff


# In[17]:


dataMatrix = data.as_matrix()
RAM2 = PreTestSet(dataMatrix, num, 1)
data.iloc[RAM2].to_csv(pathSave)


# In[18]:


print(str('\nDone!'))

