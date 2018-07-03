
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
dataPathSave = os.path.abspath('Data/BPM2018_LatestPayload.csv')
pathSave = os.path.abspath('Data/data3LatestPayload.csv')


# In[3]:


dataRaw = pd.read_csv(dataRawPath, index_col='ID')
dataRaw.head()


# # Encoding:

# In[4]:


max(dataRaw.groupby('ID_Num').size().sort_values())


# In[5]:


for i in range(1,(max(dataRaw.groupby('ID_Num').size().sort_values())+1)):
    event = "Event %s" % (i)
    dataRaw[event] = 0


# In[6]:


def LatestPayload(dataFrame):
    rows = dataFrame.shape[0]
    case_1 = dataFrame.at[0,'ID_Num']
    dataFrame.at[0,'Event 1'] = int(dataFrame.at[0,'Event_ID'])
    n = 1
    for index in range(1,rows):
        case = dataFrame.at[index,'ID_Num']
        if case == case_1:
            n = n+1
            event = "Event %s" % (n)
            dataFrame.at[index,event] = int(dataFrame.at[index,'Event_ID'])
            for i in range(1,n):
                event = "Event %s" % (i)
                dataFrame.at[index,event] = int(dataFrame.at[(index-1),event])
                #print(str(index) + ' ' + str(i) + ' ' + str(event))
        else:
            n=1
            dataFrame.at[index,'Event 1'] = int(dataFrame.at[index,'Event_ID'])
        case_1 = case
        if (index % 10 == 0):
                f = round(((index/rows)*100),1)
                sys.stdout.write("\r" + str(f) + '%')
                sys.stdout.flush()
    dataFrame = dataFrame.fillna(value=0)


# In[7]:


LatestPayload(dataRaw)


# In[8]:


dataRaw.head()


# In[9]:


#Bring Order to the Columns:
RAM = pd.DataFrame()
RAM['ID_Num'] = dataRaw['ID_Num']
RAM['Event_Number'] = dataRaw['Event_Number']
RAM['activity'] = dataRaw['activity_id']
RAM['resFac'] = dataRaw['resource_id']

for i in range(1,(max(dataRaw.groupby('ID_Num').size().sort_values())+1)):
    event = "Event %s" % (i)
    RAM[event] = dataRaw[event]

RAM['UndesiredOutcome'] = dataRaw['UndesiredOutcome']
    
RAM['N_act'] = dataRaw['N_act']
RAM['N_a'] = dataRaw['NaNumAct']
RAM['N_ar'] = dataRaw['N_ar']
RAM['Uc_ar'] = dataRaw['Uc']


# In[10]:


RAM.head()


# In[11]:


RAM.to_csv(dataPathSave)
RAM.to_csv(pathSave)
#dataRaw = pd.read_csv(pathDataSave, sep=',')


# In[12]:


print(str('\nDone!'))

