
# coding: utf-8

# # Preparation Data -> Ucoeff(a,r)

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
import math
import os


# In[2]:


pathDataSet = os.path.abspath('Data/BPI_Challenge_2018_prepared.csv')
pathDataSave = os.path.abspath('Data/BPI_Challenge_2018_Uc.csv')
dataRaw = pd.DataFrame.from_csv(path=pathDataSet)
data = dataRaw


# In[3]:


#data = data.fillna(value=-1)
pd.options.display.max_columns = 150
data.head()


# In[4]:


resources = pd.DataFrame(data['resource_id'])
resources = resources.groupby(['resource_id']).size()
resources = pd.DataFrame(resources)
resources['res'] = resources.index
resources['count'] = resources[0]
resources.drop(0, inplace=True, axis=1)
resources = resources.as_matrix()


# In[5]:


resources = resources.astype(dtype='int32')


# In[6]:


data['ID_'] = data.index
RAM = pd.DataFrame()
RAM['id'] = data['ID_']
RAM['event_id'] = data['Event_ID']
RAM['FactorizedResource'] = data['resource_id']
RAM['DurEvent'] = data['durAct']
RAM = RAM.astype(dtype='int32')
dataRAM = RAM.fillna(value=-1)
dataFrame = RAM.as_matrix()
dataFrame


# In[7]:


dataRAM.loc[((dataRAM['FactorizedResource'] == 3) & (dataRAM['event_id'] == 1)),'DurEvent'].median()


# In[8]:


dataRAM.groupby('event_id').size().sort_values() #for for of function below


# In[9]:


# Calculate Average of how long a resource works on one activity:
start = ti.time()
print(strftime("%d-%m-%y %H:%M:%S", gmtime()))
print('\n')

lres = len(resources)
rows = len(dataRAM)
actAV = np.array([[0,0,0]])
for index in range(0,lres):
    res = resources[index][0]
    ################## Change to number of different Activities!!!! #########
    for i in range(0,625):
        dur = dataRAM.loc[((dataRAM['FactorizedResource'] == res) & (dataRAM['event_id'] == i)),'DurEvent'].median()
        if math.isnan(dur):
            dur = 0
        actAV = np.append(actAV, [[int(res),int(i),int(round(dur,0))]], axis=0)  
        #actAV = np.append(actAV, [[int(res),int(i),int(round(dur,0))]], axis=0)
    
    f = round(((index/lres)*100),0)
    sys.stdout.write("\r" + str(f))
    sys.stdout.flush()

actAV = np.delete(actAV, 0, axis=0)
print('\n')
print(strftime("%d-%m-%y %H:%M:%S", gmtime()))
#data['durationSeconds'] = pd.Series(RAM).values
end = ti.time()
(end-start)


# In[10]:


actAV_DF = pd.DataFrame(actAV)
actAV_DF['res'] = actAV_DF[0]
actAV_DF['act'] = actAV_DF[1]
actAV_DF['num'] = actAV_DF[2]
actAV_DF.drop(0, axis=1, inplace=True)
actAV_DF.drop(1, axis=1, inplace=True)
actAV_DF.drop(2, axis=1, inplace=True)


# In[11]:


actAV_DF.loc[(actAV_DF['res'] == res),'num'].sum()


# In[12]:


# Calculate how long a resource works on one activity:
start = ti.time()
print(strftime("%d-%m-%y %H:%M:%S", gmtime()))
print('\n')

lres = len(resources)
allAV = np.array([[0,0]])
for index in range(0,lres):
    res = resources[index][0]
    
    s = actAV_DF.loc[(actAV_DF['res'] == res),'num'].sum()
    allAV = np.append(allAV, [[int(res),int(round(s,0))]], axis=0)  
    #actAV = np.append(actAV, [[int(res),int(i),int(round(dur,0))]], axis=0)
    
    f = round(((index/lres)*100),0)
    sys.stdout.write("\r" + str(f))
    sys.stdout.flush()

allAV = np.delete(allAV, 0, axis=0)
print('\n')
print(strftime("%d-%m-%y %H:%M:%S", gmtime()))
#data['durationSeconds'] = pd.Series(RAM).values
end = ti.time()
(end-start)


# In[13]:


allAV_DF = pd.DataFrame(allAV)
allAV_DF['res'] = allAV_DF[0]
allAV_DF['num'] = allAV_DF[1]
allAV_DF.drop(0, axis=1, inplace=True)
allAV_DF.drop(1, axis=1, inplace=True)


# In[14]:


allAV_DF.head()


# In[15]:


actAV_DF.head()


# In[16]:


# Calculate Average of how long a resource works on each activity:
start = ti.time()
print(strftime("%d-%m-%y %H:%M:%S", gmtime()))
print('\n')
diff = np.array([[0]])
rows = dataRAM.shape[0]

for index in range(0,rows):
    
    act = dataRAM.loc[index,'event_id']
    res = dataRAM.loc[index,'FactorizedResource']
    #print(str(act) + ' res= ' + str(res))
    acAV = actAV_DF.loc[((actAV_DF['res'] == res) & (actAV_DF['act'] == act)),'num'].values[0]
    alAV = allAV_DF.loc[(allAV_DF['res'] == res),'num'].values[0]
    
    if alAV == 0:
        alAv = 1

    diff = np.append(diff, [[round((1-(acAV/alAV)),2)]], axis=0)  

    f = round(((index/rows)*100),1)
    sys.stdout.write("\r" + str(f))
    sys.stdout.flush()
diff = np.delete(diff, 0, axis=0)
print('\n')
print(strftime("%d-%m-%y %H:%M:%S", gmtime()))
#data['durationSeconds'] = pd.Series(RAM).values
end = ti.time()
(end-start)


# In[17]:


len(diff)


# In[18]:


data.shape


# In[19]:


data['Uc'] = diff


# In[20]:


data.groupby('Uc').size().sort_values()


# In[21]:


data.to_csv(pathDataSave)

