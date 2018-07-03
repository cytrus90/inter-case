
# coding: utf-8

# # Preparation Data -> N_act:

# In[1]:


import pandas as pd
import numpy as np
import time
from time import gmtime, strftime
import sys
import os


# In[2]:


pathDataSet = os.path.abspath('Data/BPI_Challenge_2018_prepared.csv')
pathDataSave = os.path.abspath('Data/BPI_Challenge_2018_N_act.csv')
dataRaw = pd.DataFrame.from_csv(path=pathDataSet)
data = dataRaw


# In[3]:


RAM = pd.DataFrame()
RAM['Event_ID'] = data['Event_ID']
RAM['TimeSinceInt'] = data['TimeSinceInt']


# In[4]:


RAM.head()


# In[5]:


start = time.time()
diff = list()
rows = len(RAM)
diff.append(0)
for index in range(1,rows):
    diff.append(len(RAM.loc[(RAM['TimeSinceInt'] <= RAM.at[(index),'TimeSinceInt']) & (RAM['TimeSinceInt'] >= RAM.at[(index-1),'TimeSinceInt'])]))
    f = round(((index/rows)*100),2)
    sys.stdout.write("\r" + str(f) + '% Index: ' + str(index))
    sys.stdout.flush()
end = time.time()
(end-start)


# In[6]:


data['N_act'] = pd.Series(diff).values
data.head()


# In[7]:


data.to_csv(pathDataSave)

