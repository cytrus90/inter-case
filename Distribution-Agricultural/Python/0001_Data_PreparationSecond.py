
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


pathDataSet = os.path.abspath('Data/BPM2018_MasterDataRAM.csv')
pathDataSave = os.path.abspath('Data/BPI_Challenge_2018_prepared.csv')


# In[3]:


#data = pd.read_csv(pathDataSet, index_col='ID')
dataRaw = pd.DataFrame.from_csv(path=pathDataSet, sep=',')
dataRaw = dataRaw.fillna(value=-1)


# In[4]:


dataRaw.shape


# ### Remove Cases longer than 100 events

# In[5]:


dataRaw['ID'] = range(0,dataRaw.shape[0])
dataRaw = dataRaw.set_index('ID')
dataRaw.head()


# In[6]:


rows = dataRaw.shape[0]-1
case_1 = dataRaw.at[rows,'ID_Num']
diff = list()
n = dataRaw.at[rows,'Event_Number']
diff.append(n)
for index in range(rows,0,-1):
    case = dataRaw.at[index,'ID_Num']
    if case != case_1:
        n = dataRaw.at[index,'Event_Number']
    diff.append(n)
    case_1 = case
    if (index % 10 == 0):
        f = round(((index/rows)*100),0)
        sys.stdout.write("\r" + str(f))
        sys.stdout.flush()


# In[7]:


dataRaw['NumberEventsCase'] = pd.Series(diff).values


# In[8]:


dataRaw.drop(dataRaw[dataRaw.NumberEventsCase > 100].index, inplace=True)


# In[9]:


#pathDataSave = os.path.abspath('Data/BPM2018_MasterDataRAM.csv')
#dataRaw.to_csv(pathDataSave)
#dataRaw = pd.read_csv(pathDataSave, sep=',')


# In[10]:


dataRaw['ID'] = range(0,dataRaw.shape[0])
dataRaw = dataRaw.set_index('ID')
rows = dataRaw.shape[0]
for index in range(0,rows):
    if index in dataRaw.index:
        if dataRaw.at[index, 'Event_Number'] > 100:
            #print(index)
            dataRaw.drop(dataRaw[dataRaw['ID_Num'] == dataRaw.at[index,'ID_Num']].index, inplace=True)
            index = 0
            dataRaw['ID'] = range(0,dataRaw.shape[0])
            dataRaw = dataRaw.set_index('ID')
            rows = dataRaw.shape[0]-1
        if (index % 100 == 0):
            f = round(((index/rows)*100),2)
            sys.stdout.write("\r" + str(f))
            sys.stdout.flush()
    else:
        break


# In[11]:


1245852 in dataRaw.index


# In[12]:


dataRaw.head(2)


# In[13]:


dataRaw.groupby('ID_Num').size().sort_values().count()


# In[14]:


si = pd.DataFrame(dataRaw.groupby('ID_Num').size().sort_values())
len(si[si[0] > 100])


# In[15]:


max(dataRaw.groupby('ID_Num').size().sort_values())


# In[16]:


dataRaw.groupby('UndesiredOutcome').size().sort_values()


# In[17]:


dataRaw.shape


# In[18]:


#pathDataSave = os.path.abspath('Data/BPM2018_MasterDataRAM.csv')
#dataRaw.to_csv(pathDataSave)
#dataRaw = pd.read_csv(pathDataSave, sep=',')


# In[19]:


data = dataRaw
dataRaw = list()


# # Add TimeSinceInt:

# In[20]:


data.groupby('startTime').size().sort_values()
data.sort_values('startTime').head(5)


# In[21]:


#date = data['startTime'][594652].split(' ')[0].split('/')
#time = data['startTime'][594652].split(' ')[1].split(':')


# In[22]:


#datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2].split('.')[0]))


# In[23]:


def Time2Int(dataFrame):
    rows = dataFrame.shape[0]
    diff = list()
    date = dataFrame['startTime'][261709].split(' ')[0].split('/')
    time = dataFrame['startTime'][261709].split(' ')[1].split(':')
    start = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2].split('.')[0]))
    for index in range(0,(rows)):
        date = dataFrame['startTime'][index].split(' ')[0].split('/')
        time = dataFrame['startTime'][index].split(' ')[1].split(':')
        end = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2].split('.')[0]))
        RAM = end - start
        RAM = abs(RAM.days*24*60*60 + RAM.seconds)
        diff.append(RAM)
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f))
            sys.stdout.flush()
    return diff


# In[24]:


start = ti.time()
RAM = Time2Int(data)
end = ti.time()
(end-start)


# In[25]:


data['TimeSinceInt'] = pd.Series(RAM).values
data.head()


# In[26]:


#data.sort_values('TimeSinceInt')


# # Factorize Resources:

# Resources are factorized in this dataset, but are only included in the first row of each event.

# In[27]:


data.at[2,'ID_Num']


# In[28]:


def completeRes(dataFrame):
    rows = dataFrame.shape[0]
    case_1 = dataFrame.at[0,'ID_Num']
    res_1 = dataFrame.at[0,'resource_id']
    for index in range(1,rows):
        case = dataFrame.at[index,'ID_Num']
        if case == case_1:
            dataFrame.at[index,'resource_id'] = res_1
        elif case != case_1:
            res_1 = dataFrame.at[index,'resource_id']
        case_1 = case
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f))
            sys.stdout.flush()


# In[29]:


start = ti.time()
RAM = completeRes(data)
end = ti.time()
(end-start)


# In[30]:


#data['ID_Num'] = pd.factorize(data['CASE_concept_name'], sort=True)[0]
#data['Event_ID'] = pd.factorize(data['event'], sort=True)[0]


# # durAct:

# #Duration of an Activity

# In[31]:


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
            date = data['startTime'][(index+1)].split(' ')[0].split('/')
            time = data['startTime'][(index+1)].split(" ")[1].split(':')
            t2 = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2].split('.')[0]))
            date = data['startTime'][(index)].split(' ')[0].split('/')
            time = data['startTime'][(index)].split(" ")[1].split(':')
            t1 = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2].split('.')[0]))
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


# In[32]:


start = ti.time()
RAM = dfAddDuration(data)
end = ti.time()
(end-start)


# In[33]:


data['durAct'] = pd.Series(RAM).values


# In[34]:


data.head(1)


# # durCase:

# Duration of a Case

# In[35]:


#date = data['startTime'][0].split(" ")[0].split('/')
#time = data['startTime'][0].split(" ")[1].split(':')
#time[2] = time[2].split('.')[1]
#t1 = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2]))
#t1
#time


# In[36]:


#date = data['startTime'][0].split(" ")[0].split('/')
#time = data['startTime'][0].split(" ")[1].split(':')
#time[2] = time[2].split('.')[1]
#t2 = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2]))
#t2


# In[37]:


#RAM = t2 - t1
#RAM = abs(RAM.days*24*60*60 + RAM.seconds)
#RAM


# In[38]:


#date = data['startTime'][261709].split(' ')[0].split('/')
#time = data['startTime'][261709].split(' ')[1].split(':')


# In[39]:


def dfAddRemainingCase(dataFrame):
    diff = list()
    f = 0
    RAM = 0
    FMT = '%Y-%m-%d %H:%M:%S'
    rows, columns = dataFrame.shape
    date = dataFrame['startTime'][0].split(" ")[0].split('/')
    time = dataFrame['startTime'][0].split(" ")[1].split(':')
    t1 = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2].split('.')[0]))
    for index in range(0,(rows)):
        case = data.at[index,'ID_Num']
        if ((index+1) < rows):
            caseI1 = data.at[(index+1),'ID_Num']
        else:
            caseI1 = 'END0'
        if (case != caseI1):
            date = dataFrame['startTime'][index].split(" ")[0].split('/')
            time = dataFrame['startTime'][index].split(" ")[1].split(':')
            t2 = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2].split('.')[0]))
            RAM = t2 - t1
            RAM = abs(RAM.days*24*60*60 + RAM.seconds)
            diff.append(RAM)
            if ((index+1) < rows):
                date = dataFrame['startTime'][(index+1)].split(" ")[0].split('/')
                time = dataFrame['startTime'][(index+1)].split(" ")[1].split(':')
                t1 = datetime(int(date[0]),int(date[1]),int(date[2]),int(time[0]),int(time[1]),int(time[2].split('.')[0]))
            else:
                t1 = 0
        elif (case == caseI1):
                RAM = 0
                diff.append(RAM)
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f) + '%')
            sys.stdout.flush()
    #diff.append(0)
    return diff


# In[40]:


start = ti.time()
RAM = dfAddRemainingCase(data)
end = ti.time()
(end-start)


# In[41]:


data.shape


# In[42]:


data['durCase'] = pd.Series(RAM).values


# In[43]:


data.tail(10)


# In[44]:


def dfAddRemaining(dataFrame):
    diff = list()
    rows, columns = dataFrame.shape
    case_1 = 0
    first = 0
    for index in range(0,rows):
        case = dataFrame.at[index,'ID_Num']
        if index < (rows-1):
            caseP1 = dataFrame.at[(index+1),'ID_Num']
        else:
            caseP1 = 'END0'
        if case != case_1:
            first = index
            case_1 = case
        if case != caseP1:
            value = dataFrame.iloc[index]['durCase']
            for i in range(first,(index+1)):
                #if i == index:
                #    RAM = 0
                #else:
                RAM = value-dataFrame.iloc[i]['durAct']
                value = RAM
                diff.append(RAM)
        case_1 = case
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f) + '%')
            sys.stdout.flush()
    return diff


# In[45]:


start = ti.time()
RAM = dfAddRemaining(data)
end = ti.time()
(end-start)


# In[46]:


data['remTime'] = pd.Series(RAM).values


# In[47]:


data.head()


# In[48]:


def AddNextEvent(dataFrame):
    rows = dataFrame.shape[0]
    diff = list()
    for index in range(0,(rows)):
        case = dataFrame.at[index,'ID_Num']
        if (index < (rows-1)):
            event_1 = dataFrame.iloc[(index+1)]['Event_ID']
            case_1 = dataFrame.at[(index+1),'ID_Num']
        else:
            event_1 = 11
            case_1 = 11
        if (case_1 == case):
            diff.append(event_1)
        elif (case_1 != case):
            diff.append(11)
        
        if (index % 10 == 0):
            f = round(((index/rows)*100),1)
            sys.stdout.write("\r" + str(f) + '%')
            sys.stdout.flush()
    return diff


# In[49]:


start = ti.time()
RAM = AddNextEvent(data)
end = ti.time()
(end-start)


# In[50]:


pathDataSave = os.path.abspath('Data/BPI_Challenge_2018_prepared.csv')
data.to_csv(pathDataSave)


# In[ ]:




