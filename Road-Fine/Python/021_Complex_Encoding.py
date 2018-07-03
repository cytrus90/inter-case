
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
pathDataSave = os.path.abspath('Data/Road_Complex.csv')
pathSave = os.path.abspath('Data/data3Complex.csv')


# In[3]:


dataRaw = pd.read_csv(dataRawPath, index_col='ID')
dataRaw.head()


# # Encoding:

# In[4]:


for i in range(1,(len(dataRaw.groupby('Activity').size().sort_values())+1)):
    event = "Event %s" % (i)
    res = "Res %s" % (i)
    dataRaw[event] = 0
    dataRaw[res] = 0


# In[5]:


def complexEncoding(dataFrame):
    rows = dataFrame.shape[0]
    case_1 = dataFrame.at[0,'ID_Num']
    dataFrame.at[0,'Event 1'] = int(dataFrame.at[0,'Activity_ID'])
    dataFrame.at[0,'Res 1'] = int(dataFrame.at[0,'Resource_ID'])
    n = 1
    #rows = 100
    for index in range(1,rows):
        case = dataFrame.at[index,'ID_Num']
        if case == case_1:
            n = n+1
            event = "Event %s" % (n)
            res = "Res %s" % (n)
            dataFrame.at[index,event] = int(dataFrame.at[index,'Activity_ID'])
            dataFrame.at[index,res] = int(dataFrame.at[index,'Resource_ID'])
            #print(index)
            for i in range(1,(n)):
                event = "Event %s" % (i)
                res = "Res %s" % (i)
                dataFrame.at[index,event] = int(dataFrame.at[(index-1),event])
                dataFrame.at[index,res] = int(dataFrame.at[(index-1),res])
        else:
            n=1
            dataFrame.at[index,'Event 1'] = int(dataFrame.at[index,'Activity_ID'])
            dataFrame.at[index,'Res 1'] = int(dataFrame.at[index,'Resource_ID'])
        case_1 = case
        if (index % 10 == 0):
                f = round(((index/rows)*100),1)
                sys.stdout.write("\r" + str(f) + '%')
                sys.stdout.flush()


# In[6]:


complexEncoding(dataRaw)


# In[8]:


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

for i in range(1,(len(dataRaw.groupby('Activity').size().sort_values())+1)):
    event = "Event %s" % (i)
    res = "Res %s" % (i)
    RAM[event] = dataRaw[event]
    RAM[res] = dataRaw[res]
    
RAM['IsLastPayment'] = dataRaw['IsLastPayment']

RAM['N_act'] = dataRaw['N_act']
RAM['N_a'] = dataRaw['NaNumAct']
RAM['N_ar'] = dataRaw['N_ar']
RAM['Uc_ar'] = dataRaw['Uc']


# In[9]:


RAM.to_csv(pathDataSave)
#dataRaw = pd.read_csv(pathDataSave, sep=',')


# In[10]:


data = pd.DataFrame()
data = RAM


# # Filter for Third Activity:

# In[11]:


num = 4


# In[12]:


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


# In[13]:


dataMatrix = data.as_matrix()
RAM2 = PreTestSet(dataMatrix, num, 1)
data.iloc[RAM2].to_csv(pathSave)


# In[14]:


print(str('\nDone!'))

