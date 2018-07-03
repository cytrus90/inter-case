
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


dataRawPath = os.path.abspath('Data/BPM2018_MasterData.csv')
pathSaveThree = os.path.abspath('Data/data3Boolean.csv')


# In[3]:


dataRaw = pd.read_csv(dataRawPath, index_col='ID')
dataRaw.head()


# # Encoding:

# In[4]:


events = dataRaw.groupby('activity_id').size().sort_values()
for index in range(0,len(events)):
    dataRaw[events.index[index]] = 0


# In[5]:


def Boolean(dataFrame):
    rows = dataFrame.shape[0]
    for index in range(0,rows):
        case = dataFrame.at[index,'ID_Num']
        for i in range(index,rows):
            caseNow = dataFrame.at[i,'ID_Num']
            if (case == caseNow):
                dataFrame.at[i,dataFrame.at[index,'activity_id']] = 1
            else:
                break
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f) + '%')
            sys.stdout.flush()


# In[6]:


Boolean(dataRaw)


# In[7]:


#Bring Order to the Columns:
RAM = pd.DataFrame()
RAM['activity'] = dataRaw['activity_id']
RAM['resFac'] = dataRaw['resource_id']
RAM['Event_Number'] = dataRaw['Event_Number']
 
events = dataRaw.groupby('activity_id').size().sort_values()
for index in range(0,len(events)):
    RAM[events.index[index]] = dataRaw[events.index[index]]

RAM['UndesiredOutcome'] = dataRaw['UndesiredOutcome']

RAM['N_act'] = dataRaw['N_act']
RAM['N_a'] = dataRaw['NaNumAct']
RAM['N_ar'] = dataRaw['N_ar']
RAM['Uc_ar'] = dataRaw['Uc']


# In[8]:


RAM.to_csv(pathSaveThree)
#dataRaw = pd.read_csv(pathDataSave, sep=',')


# In[9]:


print(str('\nDone!'))


# In[10]:


dataRaw.groupby('UndesiredOutcome').size().sort_values()

