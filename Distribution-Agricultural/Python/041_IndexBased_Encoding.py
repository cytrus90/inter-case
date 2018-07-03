
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
pathDataSaveAll = os.path.abspath('Data/BPM2018_IndexBased.csv')
pathDataSaveThree = os.path.abspath('Data/data3IndexBased.csv')


# In[3]:


dataRaw = pd.read_csv(dataRawPath, index_col='ID')
dataRaw.head()


# # Encoding:

# ## 1.Factorize:

# In[4]:


RAMstate = pd.DataFrame(dataRaw.groupby('activity').size().sort_values()) #dynamic                   activity
RAMcaseType = pd.DataFrame(dataRaw.groupby('number_parcels').size().sort_values()) #static           resource_id
RAMdiagnosis = pd.DataFrame(dataRaw.groupby('Event_ID').size().sort_values()) #dynamic


# In[5]:


len(dataRaw.groupby('activity').size().sort_values())


# In[6]:


dataRaw.groupby('number_parcels').size().sort_values()


# In[7]:


dataRaw.groupby('Event_ID').size().sort_values()


# In[8]:


dataUW = pd.DataFrame()
dataUW['case'] = dataRaw['ID_Num']
dataUW['number_parcels'] = dataRaw['number_parcels']
dataUW['activity'] = dataRaw['activity']
dataUW['Event_ID'] = dataRaw['Event_ID']
dataUW['caseTypeFac'] = len(RAMcaseType)+1
dataUW['stateFac'] = len(RAMstate)+1
dataUW['diagnosisFac'] = len(RAMdiagnosis)+1
dataUW.head()


# In[9]:


RAMstate['factor'] = range((len(RAMstate)),0, -1)
RAMcaseType['factor'] = range((len(RAMcaseType)),0,-1)
RAMdiagnosis['factor'] = range((len(RAMdiagnosis)),0,-1)


# In[10]:


length = len(RAMstate)+1
rows = dataUW.shape[0]
for index in range(1,rows):
    state = dataUW.at[index,'activity']
    if state != dataUW.at[index,'activity']:
        dataUW.at[index,'stateFac'] = length
    else:
        dataUW.at[index,'stateFac'] = int(RAMstate.at[state,'factor'])
    if (index % 10 == 0):
        f = round(((index/rows)*100),1)
        sys.stdout.write("\r" + str(f) + '%')
        sys.stdout.flush()


# In[11]:


#length = len(RAMcaseType)+1

rows = dataUW.shape[0]
case_1 = 'END0'
caseT_1 = dataUW.at[0,'number_parcels']
for index in range(1,rows):
    caseT = dataUW.at[index,'number_parcels']
    if caseT != dataUW.at[index,'number_parcels']:
        caseT = caseT_1
    case = dataUW.at[index,'case']
    if case == case_1:
        dataUW.at[index,'caseTypeFac'] = int(RAMcaseType.at[caseT,'factor'])
    elif case != case_1:
        dataUW.at[index,'caseTypeFac'] = int(RAMcaseType.at[caseT,'factor'])
    case_1 = case
    caseT_1 = caseT
    if (index % 10 == 0):
        f = round(((index/rows)*100),1)
        sys.stdout.write("\r" + str(f) + '%')
        sys.stdout.flush()


# In[12]:


dataUW = dataUW.fillna(value='0')


# In[13]:


rows = dataUW.shape[0]
case_1 = 'END0'
diac_1 = 0
for index in range(0,(rows)):
    diac = dataUW.at[index,'Event_ID']
    case = dataUW.at[index,'case']
    if diac != '0':
        diac_1 = diac
        dataUW.at[index,'diagnosisFac'] = int(RAMdiagnosis.at[diac,'factor'])
    elif diac == '0' and case == case_1:
        dataUW.at[index,'diagnosisFac'] = int(RAMdiagnosis.at[diac_1,'factor'])
    elif case != case_1:
        dataUW.at[index,'diagnosisFac'] = 0
    case_1 = case
    if (index % 10 == 0):
        f = round(((index/rows)*100),1)
        sys.stdout.write("\r" + str(f) + '%')
        sys.stdout.flush()


# ## 2.Attach dataUW to the Dataset:

# In[14]:


dataRaw['stateFac']= len(RAMstate)+1
dataRaw['caseTypeFac'] = len(RAMcaseType)+1
dataRaw['diagnosisFac'] = len(RAMdiagnosis)+1


# In[15]:


# Why drop 204??? -> case with NaN
#dataRaw = dataRaw.drop(dataRaw.index[204])
#dataRaw['ID'] = range(0,len(dataRaw))
#dataRaw = dataRaw.set_index('ID')


# In[16]:


dataUW.head()


# In[17]:


rows = dataRaw.shape[0]
index = 0
while index < rows:
    case = dataRaw.at[index,'ID_Num']
    UW = dataUW.loc[dataUW['case']==case]
    UW_l = UW.index[0]
    l = UW.shape[0]
    for i in range(UW_l,(UW_l+l)):
        dataRaw.at[index,'stateFac'] = UW.at[i,'stateFac']
        dataRaw.at[index,'caseTypeFac'] = UW.at[i,'caseTypeFac']
        dataRaw.at[index,'diagnosisFac'] = UW.at[i,'diagnosisFac']
        index = index + 1
    if (index % 10 == 0):
        f = round(((index/rows)*100),1)
        sys.stdout.write("\r" + str(f) + '%')
        sys.stdout.flush()


# In[18]:


for n in range(1,(max(dataRaw.groupby('ID_Num').size().sort_values())+1)):
    ev = "Ev %s" % (n)
    re = "Re %s" % (n)
    st = "St %s" % (n)
    di = "Di %s" % (n)
    dataRaw[ev] = 0
    dataRaw[re] = 0
    dataRaw[st] = 0
    dataRaw[di] = 0


# In[19]:


def indexBasedEncoding(dataRaw):
    rows = dataRaw.shape[0]
    case_1 = dataRaw.at[0,'ID_Num']
    dataRaw.at[0,'Ev 1'] = int(dataRaw.at[0,'Event_ID'])
    dataRaw.at[0,'Re 1'] = int(dataRaw.at[0,'resource_id'])
    dataRaw.at[0,'St 1'] = int(dataRaw.at[0,'stateFac'])
    dataRaw.at[0,'Di 1'] = int(dataRaw.at[0,'diagnosisFac'])
    n = 1
    for index in range(1,(rows-1)):
        case = dataRaw.at[index,'ID_Num']
        if case == case_1:
            n = n+1
            ev = "Ev %s" % (n)
            re = "Re %s" % (n)
            st = "St %s" % (n)
            di = "Di %s" % (n)
            dataRaw.at[index,ev] = int(dataRaw.at[index,'Event_ID'])
            dataRaw.at[index,re] = int(dataRaw.at[index,'resource_id'])
            dataRaw.at[index,st] = int(dataRaw.at[index,'stateFac'])
            dataRaw.at[index,di] = int(dataRaw.at[index,'diagnosisFac'])
            for i in range(1,(n)):
                ev = "Ev %s" % (i)
                re = "Re %s" % (i)
                st = "St %s" % (i)
                di = "Di %s" % (i)
                dataRaw.at[index,ev] = int(dataRaw.at[(index-1),ev])
                dataRaw.at[index,re] = int(dataRaw.at[(index-1),re])
                dataRaw.at[index,st] = int(dataRaw.at[(index-1),st])
                dataRaw.at[index,di] = int(dataRaw.at[(index-1),di])
                #print(str(index) + ' ' + str(i) + ' ' + str(event))
        else:
            n=1
            dataRaw.at[index,'Ev 1'] = int(dataRaw.at[index,'Event_ID'])
            dataRaw.at[index,'Re 1'] = int(dataRaw.at[index,'resource_id'])
            dataRaw.at[index,'St 1'] = int(dataRaw.at[index,'stateFac'])
            dataRaw.at[index,'Di 1'] = int(dataRaw.at[index,'diagnosisFac'])
        case_1 = case
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f) + '%')
            sys.stdout.flush()


# In[20]:


indexBasedEncoding(dataRaw)


# ## Bring Order to the Columns:

# In[21]:


RAM = pd.DataFrame()
RAM['ID_Num'] = dataRaw['ID_Num']
RAM['Event_Number'] = dataRaw['Event_Number']
RAM['activity'] = dataRaw['activity_id']

for n in range(1,(max(dataRaw.groupby('ID_Num').size().sort_values())+1)):
    ev = "Ev %s" % (n)
    re = "Re %s" % (n)
    st = "St %s" % (n)
    di = "Di %s" % (n)
    RAM[ev] = dataRaw[ev]
    RAM[re] = dataRaw[re]
    RAM[st] = dataRaw[st]
    RAM[di] = dataRaw[di]
    
RAM['UndesiredOutcome'] = dataRaw['UndesiredOutcome']

RAM['N_act'] = dataRaw['N_act']
RAM['N_a'] = dataRaw['NaNumAct']
RAM['N_ar'] = dataRaw['N_ar']
RAM['Uc_ar'] = dataRaw['Uc']


# In[22]:


RAM.to_csv(pathDataSaveAll)
RAM.to_csv(pathDataSaveThree)
#RAM = pd.read_csv(pathDataSave, sep=',')


# In[23]:


max(dataRaw.groupby('ID_Num').size().sort_values())


# In[24]:


print(str('\nDone!'))

