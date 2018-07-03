
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sys
import os
import subprocess
import time as ti
from datetime import datetime

import matplotlib.pyplot as plt
import math

pd.options.display.max_columns = 10000


# In[2]:


pathDataSetRaw = os.path.abspath('Data/Road_Traffic_Fine_Management_Process.csv')
dataRaw = pd.read_csv(pathDataSetRaw, sep=';')
dataRaw['ID'] = range(0,dataRaw.shape[0])
dataRaw = dataRaw.set_index('ID')
dataRaw.head()


# In[3]:


dataRaw.tail()


# In[4]:


dataRaw['ID_Num'] = pd.factorize(dataRaw['CASE_concept_name'], sort=True)[0] #Factorize case IDs
dataRaw['Activity_ID'] = (pd.factorize(dataRaw['activity_id'], sort=True)[0] + 1) #Factorize Events
dataRaw.loc[:,'Activity'] = dataRaw.loc[:,'activity_id'].copy()

dataRaw.drop('activity_id', inplace=True, axis=1)


# # Filtering not necessary for Road Traffic

# In[5]:


dataRaw.shape


# # Add TimeSinceInt (for quicker Calculations):

# In[6]:


dataRaw['ID'] = range(0,dataRaw.shape[0])
dataRaw = dataRaw.set_index('ID')


# In[7]:


first = dataRaw.sort_values('timestamp').index[0]
first


# First entry in the log is at index 429367.

# In[8]:


def Time2Int(dataFrame):
    rows = dataFrame.shape[0]
    diff = list()
    date = dataFrame['timestamp'][first].split(' ')[0].split('-')
    time = dataFrame['timestamp'][first].split(' ')[1].split(':')
    start = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2]))
    for index in range(0,(rows)):
        date = dataFrame['timestamp'][index].split(" ")[0].split('-')
        time = dataFrame['timestamp'][index].split(" ")[1].split(':')
        end = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2]))
        RAM = end - start
        RAM = abs(RAM.days*24*60*60 + RAM.seconds)
        diff.append(RAM)
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f))
            sys.stdout.flush()
    return diff


# In[9]:


RAM = Time2Int(dataRaw)
dataRaw['TimeSinceInt'] = pd.Series(RAM).values


# # Factorize Resources:

# pd.factorize does not work because resource_id is only defined at the beginning (assumption: in a case, the resource keeps working on a case until the next resource appears) 

# In[10]:


def completeRes(dataFrame):
    rows = dataFrame.shape[0]
    case_1 = dataFrame.at[0,'CASE_concept_name']
    res_1 = dataFrame.at[0,'resource_id']
    for index in range(1,rows):
        case = dataFrame.at[index,'CASE_concept_name']
        res = dataFrame.at[index,'resource_id']
        if ((case == case_1) and (res != res)): #resource_id is NaN
            dataFrame.at[index,'resource_id'] = res_1
        elif ((case == case_1) and (res == res)):
            res_1 = res
        elif case != case_1:
            res_1 = dataFrame.at[index,'resource_id'] #first resource_id in the case is always defined
        case_1 = case
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f))
            sys.stdout.flush()


# In[11]:


RAM = completeRes(dataRaw)


# In[12]:


dataRaw['Resource_ID'] = (pd.factorize(dataRaw['resource_id'], sort=True)[0] + 1) # +1 because 0 values are not wanted, to fill NaNs
dataRaw.drop('resource_id', inplace=True, axis=1)


# # Create Column with True (if case concludes with 'Payment') or False:

# In[13]:


rows = (dataRaw.shape[0]-1)
case_1 = dataRaw.at[rows,'ID_Num']
diff = list()
act = False
for index in range(rows,-1, -1):
    case = dataRaw.at[index,'ID_Num']
    if ((case != case_1) and (dataRaw.at[index,'Activity'] == 'Payment')):
        diff.append(True)
        act = True
    elif ((case != case_1) and (dataRaw.at[index,'Activity'] != 'Payment')):
        diff.append(False)
        act = False
    else:
        diff.append(act)
    case_1 = case
    if (index % 10 == 0):
        f = round(((index/rows)*100),1)
        sys.stdout.write("\r" + str(f))
        sys.stdout.flush()
diff.reverse()


# In[14]:


dataRaw['IsLastPayment'] = pd.Series(diff).values


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


# # Overview before Saving:

# In[17]:


dataRaw.head()


# In[18]:


dataRaw.tail()


# In[19]:


pathDataSave = os.path.abspath('Data/Road_FirstBackup.csv')
dataRaw.to_csv(pathDataSave)
#dataRaw = pd.read_csv(pathDataSave, sep=';')


# # Calculate Inter-Case Features:

# In[20]:


dataRaw['ID'] = range(0,dataRaw.shape[0])
dataRaw = dataRaw.set_index('ID')
#dataRaw.head(20)


# In[21]:


# First we add a column with a simple True or False if a new case begins:
rows = dataRaw.shape[0]
case_1 = 'Start0'
diff = list()
for index in range(0,rows):
    #if (dataRaw.index == index).any():
    case = dataRaw.at[index,'ID_Num']
    if case != case_1:
        diff.append(True)
    else:
        diff.append(False)
    case_1 = case
    sys.stdout.write("\r" + str(index))
    sys.stdout.flush()
dataRaw['NewActivity'] = pd.Series(diff).values


# In[22]:


dataRaw.head()


# ## Calculate Number of Cases which are active at the same time:

# In[23]:


RAM = pd.DataFrame()
RAM['Activity_ID'] = dataRaw['Activity_ID']
RAM['TimeSinceInt'] = dataRaw['TimeSinceInt']
RAM['NewActivity'] = dataRaw['NewActivity']
RAM.head()


# In[24]:


def Nact(dataFrame):
    diff = list()
    rows = len(dataFrame)
    diff.append(0)
    for index in range(1,rows):
        diff.append(len(dataFrame.loc[(dataFrame['TimeSinceInt'] <= dataFrame.at[(index),'TimeSinceInt']) & (dataFrame['TimeSinceInt'] >= dataFrame.at[(index-1),'TimeSinceInt']) & (dataFrame.at[index, 'NewActivity'] == False)]))
        f = round(((index/rows)*100),1)
        sys.stdout.write("\r" + 'Index: ' + str(index) + ' ' + str(f))
        sys.stdout.flush()
    return(diff)


# In[25]:


start = ti.time()
RAM2 = Nact(RAM)
end = ti.time()
end - start


# In[26]:


dataRaw['N_act'] = pd.Series(RAM2).values


# ## Calculate Number of Same Activities active at the time:

# In[27]:


RAM = pd.DataFrame()
RAM['Activity_ID'] = dataRaw['Activity_ID']
RAM['ID_Num'] = dataRaw['ID_Num']
RAM['TimeSinceInt'] = dataRaw['TimeSinceInt']
RAM['Resource'] = dataRaw['Resource_ID']
RAM['NewActivity'] = dataRaw['NewActivity']


# In[28]:


def Na(dataFrame):
    diff = list()
    rows = len(dataFrame)
    diff.append(0)
    for index in range(1,rows):
        diff.append(len(dataFrame.loc[(dataFrame['TimeSinceInt'] <= dataFrame.at[(index),'TimeSinceInt']) & (dataFrame['TimeSinceInt'] >= dataFrame.at[(index-1),'TimeSinceInt']) & (dataFrame['Activity_ID'] == dataFrame.at[index,'Activity_ID']) & (dataFrame.at[index, 'NewActivity'] == False)]))
        f = round(((index/rows)*100),1)
        sys.stdout.write("\r" + 'Index: ' + str(index) + ' ' + str(f))
        sys.stdout.flush()
    return(diff)


# In[29]:


start = ti.time()
RAM2 = Na(RAM)
end = ti.time()
end - start


# In[30]:


dataRaw['NaNumAct'] = pd.Series(RAM2).values


# In[31]:


# Calculate Number of Activities worked by the Resource at the same time:


# In[32]:


RAM = pd.DataFrame()
RAM['Activity_ID'] = dataRaw['Activity_ID']
RAM['TimeSinceInt'] = dataRaw['TimeSinceInt']
RAM['Resource'] = dataRaw['Resource_ID']
RAM['NewActivity'] = dataRaw['NewActivity']


# In[33]:


def Nar(dataFrame):
    diff = list()
    rows = len(dataFrame)
    diff.append(0)
    for index in range(1,rows):
        diff.append(len(dataFrame.loc[(dataFrame['TimeSinceInt'] <= dataFrame.at[(index),'TimeSinceInt']) & (dataFrame['TimeSinceInt'] >= dataFrame.at[(index-1),'TimeSinceInt']) & (dataFrame.at[index,'Resource'] == dataFrame['Resource']) & (dataFrame.at[index, 'NewActivity'] == False)]))
        f = round(((index/rows)*100),1)
        sys.stdout.write("\r" + 'Index: ' + str(index) + ' ' + str(f))
        sys.stdout.flush()
    end = ti.time()
    return(diff)


# In[34]:


start = ti.time()
RAM2 = Nar(RAM)
end = ti.time()
end - start


# In[35]:


dataRaw['N_ar'] = pd.Series(RAM2).values


# In[36]:


pathDataSave = os.path.abspath('Data/Road_ThirdBackup.csv')
dataRaw.to_csv(pathDataSave)
#dataRaw = pd.read_csv(pathDataSave, sep=';')


# ## Calculate Ucoeff:

# ### Calculate durAct:

# In[37]:


def dfAddDuration(dataFrame):
    diff = list()
    f = 0
    RAM = 0
    diff.append(RAM)
    rows, columns = dataFrame.shape
    case_1 = dataFrame.at[0,'ID_Num']
    FMT = '%Y-%m-%d %H:%M:%S'
    for index in range(0,(rows-1)):
        case = dataFrame.at[(index+1),'ID_Num']
        if (case == case_1):
            date = dataFrame['timestamp'][(index+1)].split(" ")[0].split('-')
            time = dataFrame['timestamp'][(index+1)].split(" ")[1].split(':')
            t2 = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2]))
            date = dataFrame['timestamp'][(index)].split(" ")[0].split('-')
            time = dataFrame['timestamp'][(index)].split(" ")[1].split(':')
            t1 = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2]))
            RAM = t2 - t1
            RAM = abs(RAM.days*24*60*60 + RAM.seconds)
            diff.append(RAM)
        else:
            RAM = 0
            diff.append(RAM)
        case_1 = case
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f) + '%')
            sys.stdout.flush()
    return diff


# In[38]:


start = ti.time()
RAM = dfAddDuration(dataRaw)


# In[39]:


dataRaw['durAct'] = pd.Series(RAM).values


# In[40]:


RAM = pd.DataFrame()
RAM['id'] = dataRaw['ID_Num']
RAM['event_id'] = dataRaw['Activity_ID']
RAM['FactorizedResource'] = dataRaw['Resource_ID']
RAM['DurEvent'] = dataRaw['durAct']
RAM = RAM.astype(dtype='int32')
dataRAM = RAM.fillna(value=0)
dataFrame = RAM.as_matrix()


# In[41]:


resources = pd.DataFrame(dataRaw['Resource_ID'])
resources = resources.groupby(['Resource_ID']).size()
resources = pd.DataFrame(resources)
resources['res'] = resources.index
resources['count'] = resources[0]
resources.drop(0, inplace=True, axis=1)
resources = resources.as_matrix()


# In[42]:


# Calculate Average of how long a resource works on one activity:
lres = len(resources)
rows = len(dataRAM)
actAV = np.array([[0,0,0]])
for index in range(0,lres):
    res = resources[index][0]
    ################## Change to number of different Activities!!!! #########
    for i in range(1,12):
        dur = dataRaw.loc[((dataRaw['Resource_ID'] == res) & (dataRaw['Activity_ID'] == i)),'durAct'].mean()
        if math.isnan(dur):
            dur = 0
        actAV = np.append(actAV, [[int(res),int(i),int(round(dur,0))]], axis=0)  
    
    f = round(((index/lres)*100),1)
    sys.stdout.write("\r" + str(f))
    sys.stdout.flush()

actAV = np.delete(actAV, 0, axis=0)


# In[43]:


actAV_DF = pd.DataFrame(actAV)
actAV_DF['res'] = actAV_DF[0]
actAV_DF['act'] = actAV_DF[1]
actAV_DF['num'] = actAV_DF[2]
actAV_DF.drop(0, axis=1, inplace=True)
actAV_DF.drop(1, axis=1, inplace=True)
actAV_DF.drop(2, axis=1, inplace=True)


# In[44]:


# Calculate how long a resource works on one activity:
lres = len(resources)
allAV = np.array([[0,0]])
for index in range(0,lres):
    res = resources[index][0]
    
    s = actAV_DF.loc[(actAV_DF['res'] == res),'num'].sum()
    allAV = np.append(allAV, [[int(res),int(round((s+1),0))]], axis=0)  
    #actAV = np.append(actAV, [[int(res),int(i),int(round(dur,0))]], axis=0)
    
    f = round(((index/lres)*100),1)
    sys.stdout.write("\r" + str(f))
    sys.stdout.flush()

allAV = np.delete(allAV, 0, axis=0)


# In[45]:


allAV_DF = pd.DataFrame(allAV)
allAV_DF['res'] = allAV_DF[0]
allAV_DF['num'] = allAV_DF[1]
allAV_DF.drop(0, axis=1, inplace=True)
allAV_DF.drop(1, axis=1, inplace=True)


# In[46]:


# Calculate Average of how long a resource works on each activity:
diff = np.array([[0]])
rows = dataRaw.shape[0]

for index in range(0,rows):
    
    act = dataRaw.loc[index,'Activity_ID']
    res = dataRaw.loc[index,'Resource_ID']
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


# In[47]:


end = ti.time()
end - start


# In[48]:


dataRaw['Uc'] = diff


# In[49]:


pathDataSave = os.path.abspath('Data/Road_MasterData.csv')
dataRaw.to_csv(pathDataSave)
#dataRaw = pd.read_csv(pathDataSave, sep=';')

