
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


pathDataSetRaw = os.path.abspath('Data/BPI_Challenge_2018_prepared.csv')
dataRaw = pd.read_csv(pathDataSetRaw, sep=',')
dataRaw['ID'] = range(0,dataRaw.shape[0])
dataRaw = dataRaw.set_index('ID')
#dataRaw.head()


# In[3]:


pathDataSetNACT = os.path.abspath('Data/BPI_Challenge_2018_N_act.csv')
dataNACT = pd.read_csv(pathDataSetNACT, sep=',')
dataNACT['ID'] = range(0,dataNACT.shape[0])
dataNACT = dataNACT.set_index('ID')
#dataNACT.head()


# In[4]:


pathDataSetNA = os.path.abspath('Data/BPI_Challenge_2018_N_a.csv')
dataNA = pd.read_csv(pathDataSetNA, sep=',')
dataNA['ID'] = range(0,dataNA.shape[0])
dataNA = dataNA.set_index('ID')
#dataNA.head()


# In[5]:


pathDataSetNAR = os.path.abspath('Data/BPI_Challenge_2018_N_ar.csv')
dataNAR = pd.read_csv(pathDataSetNAR, sep=',')
dataNAR['ID'] = range(0,dataNAR.shape[0])
dataNAR = dataNAR.set_index('ID')
#dataNAR.head()


# In[6]:


pathDataSetUC = os.path.abspath('Data/BPI_Challenge_2018_Uc.csv')
dataUC = pd.read_csv(pathDataSetUC, sep=',')
dataUC['ID'] = range(0,dataUC.shape[0])
dataUC = dataUC.set_index('ID')
#dataUC.head()


# In[7]:


dataRaw.loc[:,'N_act'] = dataNACT.loc[:,'N_act']
dataRaw.loc[:,'NaNumAct'] = dataNA.loc[:,'NaNumAct']
dataRaw.loc[:,'N_ar'] = dataNAR.loc[:,'N_ar']
dataRaw.loc[:,'Uc'] = dataUC.loc[:,'Uc']


# In[8]:


dataRaw.shape


# In[9]:


pathDataSave = os.path.abspath('Data/BPM2018_MasterData.csv')
dataRaw.to_csv(pathDataSave)

