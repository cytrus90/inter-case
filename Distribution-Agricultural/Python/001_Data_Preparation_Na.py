
# coding: utf-8

# # Preparation Data -> N(a)

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


# In[2]:


pathDataSet = os.path.abspath('Data/BPI_Challenge_2018_prepared.csv')
pathDataSave = os.path.abspath('Data/BPI_Challenge_2018_N_a.csv')
dataRaw = pd.DataFrame.from_csv(path=pathDataSet)
data = dataRaw


# In[3]:


#data.head(50)


# # Faster with Pandas Functions:

# In[4]:


RAM = pd.DataFrame()
#RAM['event'] = data['event']
#RAM['startTime'] = data['startTime']
#RAM['ID_Original'] = pd.factorize(data['case_original'])[0]
RAM['Event_ID'] = data['Event_ID']
RAM['ID_Num'] = data['ID_Num']
RAM['TimeSinceInt'] = data['TimeSinceInt']
RAM['Resource'] = data['resource_id']
#RAM['ResourceFactorized'] = data['resource_id']


# In[5]:


RAM.head()


# In[6]:


start = ti.time()
diff = list()
rows = len(RAM)
diff.append(0)
for index in range(1,rows):
    diff.append(len(RAM.loc[(RAM['TimeSinceInt'] <= RAM.at[(index),'TimeSinceInt']) & (RAM['TimeSinceInt'] >= RAM.at[(index-1),'TimeSinceInt']) & (RAM['Event_ID'] == RAM.at[index,'Event_ID'])]))
    f = round(((index/rows)*100),2)
    sys.stdout.write("\r" + str(f) + '% Index: ' + str(index))
    sys.stdout.flush()
end = ti.time()
(end-start)


# In[7]:


dataRaw = pd.DataFrame.from_csv(path=pathDataSet)
data = dataRaw


# In[8]:


dataRaw['NaNumAct'] = pd.Series(diff).values
dataRaw.head()


# In[9]:


dataRaw.to_csv(pathDataSave)

