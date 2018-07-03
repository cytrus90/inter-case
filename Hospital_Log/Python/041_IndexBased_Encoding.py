
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


dataRawPath = os.path.abspath('Data/Log_MasterData.csv')
pathDataSaveAll = os.path.abspath('Data/Log_IndexBased.csv')
pathDataSaveThree = os.path.abspath('Data/data3IndexBased.csv')


# In[3]:


dataRaw = pd.read_csv(dataRawPath, index_col='ID')
dataRaw.head()


# # Encoding:

# ## 1.Factorize:

# In[5]:


#RAMstate = pd.DataFrame(dataRaw.groupby('state').size().sort_values()) #dynamic
RAMcaseDiagnosis = pd.DataFrame(dataRaw.groupby('CASE_Diagnosis_1').size().sort_values()) #static -> CASE_Diagnosis_1
#RAMres = pd.DataFrame(dataRaw.groupby('Resource_ID').size().sort_values()) #dynamic -> Resource_ID


# In[6]:


dataUW = pd.DataFrame()
dataUW['case'] = dataRaw['CASE_concept_name']
dataUW['CASE_Diagnosis_1'] = dataRaw['CASE_Diagnosis_1']
dataUW['caseTypeFac'] = len(RAMcaseDiagnosis)+1
dataUW.head()


# In[7]:


RAMcaseDiagnosis['factor'] = range((len(RAMcaseDiagnosis)+1),1,-1)


# In[8]:


#length = len(RAMcaseType)+1

rows = dataUW.shape[0]
case_1 = 'END0'
caseT_1 = dataUW.at[0,'CASE_Diagnosis_1']
for index in range(0,rows):
    caseT = dataUW.at[index,'CASE_Diagnosis_1']
    if caseT != dataUW.at[index,'CASE_Diagnosis_1']:
        caseT = caseT_1
    case = dataUW.at[index,'case']
    if case == case_1:
        dataUW.at[index,'caseTypeFac'] = int(RAMcaseDiagnosis.at[caseT,'factor'])
    elif case != case_1:
        dataUW.at[index,'caseTypeFac'] = int(RAMcaseDiagnosis.at[caseT,'factor'])
    case_1 = case
    caseT_1 = caseT
    if (index % 10 == 0):
        f = round(((index/rows)*100),1)
        sys.stdout.write("\r" + str(f) + '%')
        sys.stdout.flush()


# In[9]:


dataUW = dataUW.fillna(value='0')


# ## 2.Attach dataUW to the Dataset:

# In[10]:


dataRaw['caseTypeFac'] = len(RAMcaseDiagnosis)+1


# In[11]:


# Why drop 204??? -> case with NaN
#dataRaw = dataRaw.drop(dataRaw.index[204])
dataRaw['ID'] = range(0,len(dataRaw))
dataRaw = dataRaw.set_index('ID')


# In[12]:


dataUW.head()


# In[13]:


rows = dataRaw.shape[0]
index = 0
while index < rows:
    case = dataRaw.at[index,'CASE_concept_name']
    UW = dataUW.loc[dataUW['case']==case]
    UW_l = UW.index[0]
    l = UW.shape[0]
    for i in range(UW_l,(UW_l+l)):
        dataRaw.at[index,'caseTypeFac'] = UW.at[i,'caseTypeFac']
        index = index + 1
    if (index % 10 == 0):
        f = round(((index/rows)*100),1)
        sys.stdout.write("\r" + str(f) + '%')
        sys.stdout.flush()


# In[14]:


for n in range(1,(max(dataRaw.groupby('CASE_concept_name').size().sort_values())+1)):
    ev = "Ev %s" % (n)
    re = "Re %s" % (n)
    di = "Di %s" % (n)
    dataRaw[ev] = 0
    dataRaw[re] = 0
    dataRaw[di] = 0


# In[15]:


def indexBasedEncoding(dataRaw):
    rows = dataRaw.shape[0]
    case_1 = dataRaw.at[0,'ID_Num']
    dataRaw.at[0,'Ev 1'] = int(dataRaw.at[0,'Activity_ID'])
    dataRaw.at[0,'Re 1'] = int(dataRaw.at[0,'Resource_ID'])
    dataRaw.at[0,'Di 1'] = int(dataRaw.at[0,'caseTypeFac'])
    n = 1
    for index in range(1,(rows-1)):
        case = dataRaw.at[index,'ID_Num']
        if case == case_1:
            n = n+1
            ev = "Ev %s" % (n)
            re = "Re %s" % (n)
            di = "Di %s" % (n)
            dataRaw.at[index,ev] = int(dataRaw.at[index,'Activity_ID'])
            dataRaw.at[index,re] = int(dataRaw.at[index,'Resource_ID'])
            dataRaw.at[index,di] = int(dataRaw.at[index,'caseTypeFac'])
            for i in range(1,(n)):
                ev = "Ev %s" % (i)
                re = "Re %s" % (i)
                di = "Di %s" % (i)
                dataRaw.at[index,ev] = int(dataRaw.at[(index-1),ev])
                dataRaw.at[index,re] = int(dataRaw.at[(index-1),re])
                dataRaw.at[index,di] = int(dataRaw.at[(index-1),di])
                #print(str(index) + ' ' + str(i) + ' ' + str(event))
        else:
            n=1
            dataRaw.at[index,'Ev 1'] = int(dataRaw.at[index,'Activity_ID'])
            dataRaw.at[index,'Re 1'] = int(dataRaw.at[index,'Resource_ID'])
            dataRaw.at[index,'Di 1'] = int(dataRaw.at[index,'caseTypeFac'])
        case_1 = case
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f) + '%')
            sys.stdout.flush()


# In[16]:


indexBasedEncoding(dataRaw)


# ## Bring Order to the Columns:

# In[17]:


RAM = pd.DataFrame()
RAM['case'] = dataRaw['CASE_concept_name']
RAM['case_id'] = dataRaw['ID_Num']
RAM['Event_Number'] = dataRaw['Event_Number']

for n in range(1,(max(dataRaw.groupby('CASE_concept_name').size().sort_values())+1)):
    ev = "Ev %s" % (n)
    re = "Re %s" % (n)
    di = "Di %s" % (n)
    RAM[ev] = dataRaw[ev]
    RAM[re] = dataRaw[re]
    RAM[di] = dataRaw[di]
    
RAM['IsLastOrdertarief'] = dataRaw['IsLastOrdertarief']
    
RAM['N_act'] = dataRaw['N_act']
RAM['N_a'] = dataRaw['NaNumAct']
RAM['N_ar'] = dataRaw['N_ar']
RAM['Uc_ar'] = dataRaw['Uc']


# In[18]:


RAM.to_csv(pathDataSaveAll)
#dataRaw = pd.read_csv(pathDataSave, sep=',')


# In[19]:


data = pd.DataFrame()
data = RAM


# # Filter for Second Activity:

# In[20]:


num = 3


# In[21]:


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


# In[23]:


dataMatrix = data.as_matrix()
RAM2 = PreTestSet(dataMatrix, num, 1)
data.iloc[RAM2].to_csv(pathDataSaveThree)


# In[24]:


print(str('\nDone!'))

