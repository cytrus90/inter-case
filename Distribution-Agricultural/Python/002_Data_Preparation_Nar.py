
# coding: utf-8

# # Preparation Data -> N(a,r)

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
pathDataSave = os.path.abspath('Data/BPI_Challenge_2018_N_ar.csv')
dataRaw = pd.DataFrame.from_csv(path=pathDataSet)
data = dataRaw


# In[3]:


RAM = pd.DataFrame()
RAM['Event_ID'] = data['Event_ID']
RAM['TimeSinceInt'] = data['TimeSinceInt']
RAM['Resource'] = data['resource_id']


# In[4]:


RAM.head()


# In[5]:


start = ti.time()
diff = list()
rows = len(RAM)
diff.append(0)
for index in range(1,rows):
    diff.append(len(RAM.loc[(RAM['TimeSinceInt'] <= RAM.at[(index),'TimeSinceInt']) & (RAM['TimeSinceInt'] >= RAM.at[(index-1),'TimeSinceInt']) & (RAM.at[index,'Resource'] == RAM['Resource'])]))
    f = round(((index/rows)*100),2)
    sys.stdout.write("\r" + str(f) + '% Index: ' + str(index))
    sys.stdout.flush()
end = ti.time()
(end-start)


# In[6]:


dataRaw['N_ar'] = pd.Series(diff).values
dataRaw.head()


# In[7]:


dataRaw.to_csv(pathDataSave)

