
# coding: utf-8

# In[1]:


import pandas as pd

import sys
import os
import subprocess
import time as ti
from datetime import datetime

pd.options.display.max_columns = 10000


# In[2]:


dataRawPath = os.path.abspath('Data/BPM2018_MasterData.csv')
pathDataSave = os.path.abspath('Data/BPM2018_Complex.csv')
pathSave = os.path.abspath('Data/data3Complex.csv')


# In[3]:


data = pd.read_csv(dataRawPath, index_col='ID')
data.head()


# In[4]:


dataRaw = pd.DataFrame()

dataRaw['UndesiredOutcome'] = data['UndesiredOutcome']

dataRaw['N_act'] = data['N_act']
dataRaw['NaNumAct'] = data['NaNumAct']
dataRaw['N_ar'] = data['N_ar']
dataRaw['Uc'] = data['Uc']

dataRaw['ID_Num'] = data['ID_Num']
dataRaw['Event_ID'] = data['Event_ID']
dataRaw['resource_id'] = data['resource_id']
dataRaw['Event_Number'] = data['Event_Number']


# In[5]:


dataRaw = dataRaw.fillna(value=0)


# # Encoding:

# In[6]:


max(dataRaw.groupby('ID_Num').size().sort_values())+1


# In[7]:


for i in range(1,(max(dataRaw.groupby('ID_Num').size().sort_values())+1)):
    event = "Event %s" % (i)
    res = "Res %s" % (i)
    dataRaw[event] = 0
    dataRaw[res] = 0


# In[8]:


def complexEncoding(dataFrame):
    rows = dataFrame.shape[0]
    case_1 = dataFrame.at[0,'ID_Num']
    dataFrame.at[0,'Event 1'] = int(dataFrame.at[0,'Event_ID'])
    dataFrame.at[0,'Res 1'] = int(dataFrame.at[0,'resource_id'])
    n = 1
    #rows = 100
    for index in range(1,rows):
        case = dataFrame.at[index,'ID_Num']
        if case == case_1:
            n = n+1
            event = "Event %s" % (n)
            res = "Res %s" % (n)
            dataFrame.at[index,event] = int(dataFrame.at[index,'Event_ID'])
            dataFrame.at[index,res] = int(dataFrame.at[index,'resource_id'])
            #print(index)
            for i in range(1,(n)):
                event = "Event %s" % (i)
                res = "Res %s" % (i)
                dataFrame.at[index,event] = int(dataFrame.at[(index-1),event])
                dataFrame.at[index,res] = int(dataFrame.at[(index-1),res])
        else:
            n=1
            dataFrame.at[index,'Event 1'] = int(dataFrame.at[index,'Event_ID'])
            dataFrame.at[index,'Res 1'] = int(dataFrame.at[index,'resource_id'])
        case_1 = case
        if (index % 10 == 0):
                f = round(((index/rows)*100),1)
                sys.stdout.write("\r" + str(f) + '%')
                sys.stdout.flush()


# In[9]:


complexEncoding(dataRaw)


# In[10]:


#Bring Order to the Columns:
RAM = pd.DataFrame()
RAM['ID_Num'] = dataRaw['ID_Num']
RAM['Event_Number'] = dataRaw['Event_Number']

for i in range(1,(max(dataRaw.groupby('ID_Num').size().sort_values())+1)):
    event = "Event %s" % (i)
    res = "Res %s" % (i)
    RAM[event] = dataRaw[event]
    RAM[res] = dataRaw[res]
    
RAM['UndesiredOutcome'] = dataRaw['UndesiredOutcome']

RAM['N_act'] = dataRaw['N_act']
RAM['N_a'] = dataRaw['NaNumAct']
RAM['N_ar'] = dataRaw['N_ar']
RAM['Uc_ar'] = dataRaw['Uc']


# In[11]:


RAM.to_csv(pathDataSave)
RAM.to_csv(pathSave)
#RAM = pd.read_csv(pathDataSave, sep=',')


# In[12]:


print(str('\nDone!'))

