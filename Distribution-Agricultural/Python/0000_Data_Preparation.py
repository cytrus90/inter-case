
# coding: utf-8

# # Preparation Data

# In[1]:


import pandas as pd
import statsmodels.api as sm
import numpy as np
import matplotlib.pyplot as plt
import time as ti
from time import gmtime, strftime
from datetime import datetime
from dateutil import relativedelta
import sys
import string
from pandas.plotting import scatter_matrix
import seaborn as sb

import os

pd.options.display.max_columns = 1000


# In[2]:


pathDataSet = os.path.abspath('Data/BPI_Challenge_2018.csv')
pathDataSave = os.path.abspath('Data/BPI_Challenge_2018_prepared.csv')


# In[3]:


#data = pd.read_csv(pathDataSet, index_col='ID')
data = pd.DataFrame.from_csv(path=pathDataSet, sep=',', index_col='case')
data = data.fillna(value=-1)


# In[4]:


data.shape


# In[5]:


#data = data.sort_values(by='startTime')
data.head()


# In[6]:


data['case_original'] = data.index
data['ID'] = range(0,data.shape[0])
data = data.set_index('ID')


# # Combine to activity:

# In[7]:


def combineActivity(dataFrame):
    rows = dataFrame.shape[0]
    diff = list()
    for index in range(0,(rows)):
        s = dataFrame.at[index,'subprocess']
        d = dataFrame.at[index,'doctype']
        a = dataFrame.at[index,'activity']
        diff.append(str(str(s)+'-'+str(d)+'-'+str(a)))
        if (index % 100 == 0):
            f = round(((index/rows)*100),2)
            sys.stdout.write("\r" + str(f))
            sys.stdout.flush()
    return diff


# In[8]:


start = ti.time()
RAM = combineActivity(data)
end = ti.time()
(end-start)


# In[9]:


data['activity_id'] = pd.Series(RAM).values


# In[10]:


data.groupby('activity_id').size().sort_values()


# # Factorize:

# In[11]:


data['Event_ID'] = pd.factorize(data['activity_id'], sort=True)[0]


# In[12]:


data['ID_Num'] = pd.factorize(data['case_original'], sort=True)[0]


# In[13]:


data['resource_id'] = pd.factorize(data['department'], sort=True)[0]
data.drop('department', inplace=True, axis=1)


# In[14]:


dataRaw = data
data = list()


# ### Add Event Number:

# In[15]:


rows = dataRaw.shape[0]
case_1 = dataRaw.at[0,'ID_Num']
diff = list()
n = 0
for index in range(0,rows):
    case = dataRaw.at[index,'ID_Num']
    if case == case_1:
        n = n+1
    else:
        n = 1
    diff.append(n)
    case_1 = case
    if (index % 10 == 0):
        f = round(((index/rows)*100),0)
        sys.stdout.write("\r" + str(f))
        sys.stdout.flush()


# In[16]:


dataRaw['Event_Number'] = pd.Series(diff).values


# In[17]:


#pathDataSave = os.path.abspath('Data/BPM2018_MasterDataRAM.csv')
#dataRaw.to_csv(pathDataSave)
#dataRaw = pd.read_csv(pathDataSave, sep=';')


# In[18]:


def addColumnUndesired(dataMatrix, posCase, posNote, dataFrame):
    rows = len(dataMatrix)
    str1 = 'change by department'
    str2 = 'objection'
    diff = list()
    for index in range(0,rows):
        f1 = str(dataMatrix[index][posNote]).find(str1)
        f2 = str(dataMatrix[index][posNote]).find(str2)
        if ((f1 != -1) or (f2 != -1)):
            diff.append(index)
        if (index % 10 == 0):
            f = round(((index/rows)*100),0)
            sys.stdout.write("\r" + str(f))
            sys.stdout.flush()
    diff2 = list()
    diff = pd.DataFrame(diff)
    sys.stdout.write("\r" + str('                           '))
    sys.stdout.flush()
    for index in range(0,rows):
        if len(diff[diff[0] == dataFrame.at[index,'ID_Num']]) == 0:
            diff2.append(False) #NO undesired Outcome
        else:
            diff2.append(True) #Undesired Outcome
        if (index % 10 == 0):
            f = round(((index/rows)*100),0)
            sys.stdout.write("\r" + str(f))
            sys.stdout.flush()
    return(diff2)


# In[19]:


RAM = pd.DataFrame()
RAM['posCase'] = dataRaw['ID_Num']
RAM['posNote'] = dataRaw['note']
RAM = RAM.as_matrix()
RAM


# In[20]:


RAM[1919][1]#.find('none')


# In[21]:


diff2 = addColumnUndesired(RAM, 0, 1, dataRaw)
dataRaw['UndesiredOutcome'] = pd.Series(diff2).values


# In[22]:


#pathDataSave = os.path.abspath('Data/BPM2018_MasterDataRAM.csv')
#dataRaw.to_csv(pathDataSave)
#dataRaw = pd.read_csv(pathDataSave, sep=';')


# In[23]:


def PreTestSet(dataMatrix, posCase, posAct):
    rows = len(dataMatrix)
    diff = list()
    case_1 = dataMatrix[0][posCase]
    str1 = 'decide'
    none = False
    for index in range(0,rows):
        case = dataMatrix[index][posCase]
        found = str(dataMatrix[index][posAct]).find(str1)
        
        if ((case == case_1) and (found != -1)):
            none = True
        elif ((case == case_1) and (found == -1) and none == False):
            diff.append(index)
        elif ((case != case_1) and (found == -1)):    
            none = False
            diff.append(index)
        if (index % 10 == 0):
            f = round(((index/rows)*100),0)
            sys.stdout.write("\r" + str(f))
            sys.stdout.flush()
    return(diff)


# In[24]:


RAM = pd.DataFrame()
RAM['posCase'] = dataRaw['ID_Num']
RAM['posAct'] = dataRaw['activity_id']
dataMatrix = RAM.as_matrix()
dataMatrix


# In[25]:


RAM2 = PreTestSet(dataMatrix, 0, 1)
RAM2 = pd.DataFrame(RAM2)


# In[26]:


dataRaw = dataRaw.iloc[RAM2[0].values]
dataRaw['ID'] = range(0,dataRaw.shape[0])
dataRaw = dataRaw.set_index('ID')


# In[27]:


dataRaw.shape


# In[28]:


pathDataSave = os.path.abspath('Data/BPM2018_MasterDataRAM.csv')
dataRaw.to_csv(pathDataSave)
#dataRaw = pd.read_csv(pathDataSave, sep=',')

