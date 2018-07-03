
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sys
import os
import subprocess
import time as time
from datetime import datetime

import matplotlib.pyplot as plt
import math

from sklearn.naive_bayes import GaussianNB
from sklearn import tree
from sklearn.ensemble import RandomForestClassifier
import xgboost

from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score

from sklearn.metrics import accuracy_score
from sklearn.metrics import f1_score
from sklearn.metrics import recall_score

pd.options.display.max_columns = 10000


# In[2]:


pathSave = os.path.abspath('Data/data3Frequency.csv')
data = pd.read_csv(pathSave, sep=',')
data.head()


# # Prediction:

# ## Preporation for Prediction:

# In[3]:


Result = pd.DataFrame()
Result['NaiveBayes'] = 0
Result['DecisionTree'] = 0
Result['RandomForest'] = 0
Result['XGBoost'] = 0
Result['Test'] = ['Dim0 AS:','Dim0 F1:','Dim0 RE:','Dim1 AS:','Dim1 F1:','Dim1 RE:',
                      'Dim2 AS:','Dim2 F1:','Dim2 RE:','Dim3 AS:','Dim3 F1:','Dim3 RE:',
                      'Dim4 AS:','Dim4 F1:','Dim4 RE:']
Result = Result.set_index('Test')


# In[4]:


timeDF = pd.DataFrame()
timeDF['NaiveBayes'] = 0
timeDF['DecisionTree'] = 0
timeDF['RandomForest'] = 0
timeDF['XGBoost'] = 0


# In[5]:


# Independant Variables:
events = data.groupby('activity').size().sort_values()
X = pd.DataFrame()
for index in range(0,len(events)):
    X.loc[:,events.index[index]] = data.loc[:,events.index[index]].copy()
X.loc[:,'Event_Number'] = data.loc[:,'Event_Number'].copy()
    
# Dependant Variable:
Y = data[['IsLastOrdertarief']]


# ## Actual Prediction:

# In[6]:


depthTrees = 7
sizeTesting = 0.4


# ### Dimension 0:

# In[7]:


Situation = 'Dim0'
X_train, X_test , Y_train , Y_test = train_test_split(X,Y,test_size=sizeTesting,random_state=25)


# In[8]:


S1 = str(Situation + ' AS:')
S2 = str(Situation + ' F1:')
S3 = str(Situation + ' RE:')

################################
######### Naive Bayes: #########
################################

start = time.time()
NB = GaussianNB()
NB.fit(X_train, Y_train.values.ravel())
timeDF.at[Situation,'NaiveBayes'] = time.time() - start

RAM1 = accuracy_score(Y_test.values.ravel(), NB.predict(X_test))
RAM2 = f1_score(Y_test.values.ravel(), NB.predict(X_test),average='micro')
RAM3 = recall_score(Y_test.values.ravel(), NB.predict(X_test),average='micro')

Result.at[S1,'NaiveBayes'] = round(RAM1,3)
Result.at[S2,'NaiveBayes'] = round(RAM2,3)
Result.at[S3,'NaiveBayes'] = round(RAM3,3)

print(cross_val_score(NB, X_train, Y_train.values.ravel()))


##################################
######### Decision Tree: #########
##################################

start = time.time()
decTree = tree.DecisionTreeClassifier(max_depth = depthTrees)
decTree.fit(X_train, Y_train)
timeDF.at[Situation,'DecisionTree'] = time.time() - start

RAM1 = accuracy_score(Y_test, decTree.predict(X_test))
RAM2 = f1_score(Y_test, decTree.predict(X_test),average='micro')
RAM3 = recall_score(Y_test, decTree.predict(X_test),average='micro')

Result.at[S1,'DecisionTree'] = round(RAM1,3)
Result.at[S2,'DecisionTree'] = round(RAM2,3)
Result.at[S3,'DecisionTree'] = round(RAM3,3)

print(cross_val_score(decTree, X_train, Y_train.values.ravel()))

##################################
######### Random Forest: #########
##################################

start = time.time()
randomForest = RandomForestClassifier(n_estimators=100, oob_score=True, max_depth = depthTrees)
randomForest.fit(X_train, Y_train.values.ravel())
timeDF.at[Situation,'RandomForest'] = time.time() - start

RAM1 = accuracy_score(Y_test.values.ravel(), randomForest.predict(X_test))
RAM2 = f1_score(Y_test.values.ravel(), randomForest.predict(X_test),average='micro')
RAM3 = recall_score(Y_test.values.ravel(), randomForest.predict(X_test),average='micro')

Result.at[S1,'RandomForest'] = round(RAM1,3)
Result.at[S2,'RandomForest'] = round(RAM2,3)
Result.at[S3,'RandomForest'] = round(RAM3,3)
    
print(cross_val_score(randomForest, X_train, Y_train.values.ravel()))
    
############################
######### XGBoost: #########
############################

X_trainM = X_train.as_matrix()
X_testM = X_test.as_matrix()
X_M = X.as_matrix()

start = time.time()
XGBoost = xgboost.XGBClassifier(n_estimators=100, max_depth=depthTrees)
Y_trainM = Y_train.values.ravel()
Y_testM = Y_test.values.ravel()
Y_M = Y
XGBoost.fit(X_trainM, Y_trainM)
timeDF.at[Situation,'XGBoost'] = time.time() - start

RAM1 = accuracy_score(Y_testM, XGBoost.predict(X_testM))
RAM2 = f1_score(Y_testM, XGBoost.predict(X_testM),average='micro')
RAM3 = recall_score(Y_testM, XGBoost.predict(X_testM),average='micro')
    
Result.at[S1,'XGBoost'] = round(RAM1,3)
Result.at[S2,'XGBoost'] = round(RAM2,3)
Result.at[S3,'XGBoost'] = round(RAM3,3)

print(cross_val_score(XGBoost, X_train, Y_train.values.ravel()))


# ### Dimension 1:

# In[9]:


Situation = 'Dim1'
X.loc[:,'N_act'] = data.loc[:,'N_act'].copy()
X_train, X_test , Y_train , Y_test = train_test_split(X,Y,test_size=sizeTesting,random_state=25)


# In[10]:


S1 = str(Situation + ' AS:')
S2 = str(Situation + ' F1:')
S3 = str(Situation + ' RE:')

################################
######### Naive Bayes: #########
################################

start = time.time()
NB = GaussianNB()
NB.fit(X_train, Y_train.values.ravel())
timeDF.at[Situation,'NaiveBayes'] = time.time() - start

RAM1 = accuracy_score(Y_test.values.ravel(), NB.predict(X_test))
RAM2 = f1_score(Y_test.values.ravel(), NB.predict(X_test),average='micro')
RAM3 = recall_score(Y_test.values.ravel(), NB.predict(X_test),average='micro')

Result.at[S1,'NaiveBayes'] = round(RAM1,3)
Result.at[S2,'NaiveBayes'] = round(RAM2,3)
Result.at[S3,'NaiveBayes'] = round(RAM3,3)

print(cross_val_score(NB, X_train, Y_train.values.ravel()))


##################################
######### Decision Tree: #########
##################################

start = time.time()
decTree = tree.DecisionTreeClassifier(max_depth = depthTrees)
decTree.fit(X_train, Y_train)
timeDF.at[Situation,'DecisionTree'] = time.time() - start

RAM1 = accuracy_score(Y_test, decTree.predict(X_test))
RAM2 = f1_score(Y_test, decTree.predict(X_test),average='micro')
RAM3 = recall_score(Y_test, decTree.predict(X_test),average='micro')

Result.at[S1,'DecisionTree'] = round(RAM1,3)
Result.at[S2,'DecisionTree'] = round(RAM2,3)
Result.at[S3,'DecisionTree'] = round(RAM3,3)

print(cross_val_score(decTree, X_train, Y_train.values.ravel()))

##################################
######### Random Forest: #########
##################################

start = time.time()
randomForest = RandomForestClassifier(n_estimators=100, oob_score=True, max_depth = depthTrees)
randomForest.fit(X_train, Y_train.values.ravel())
timeDF.at[Situation,'RandomForest'] = time.time() - start

RAM1 = accuracy_score(Y_test.values.ravel(), randomForest.predict(X_test))
RAM2 = f1_score(Y_test.values.ravel(), randomForest.predict(X_test),average='micro')
RAM3 = recall_score(Y_test.values.ravel(), randomForest.predict(X_test),average='micro')

Result.at[S1,'RandomForest'] = round(RAM1,3)
Result.at[S2,'RandomForest'] = round(RAM2,3)
Result.at[S3,'RandomForest'] = round(RAM3,3)
    
print(cross_val_score(randomForest, X_train, Y_train.values.ravel()))
    
############################
######### XGBoost: #########
############################

X_trainM = X_train.as_matrix()
X_testM = X_test.as_matrix()
X_M = X.as_matrix()

start = time.time()
XGBoost = xgboost.XGBClassifier(n_estimators=100, max_depth=depthTrees)
Y_trainM = Y_train.values.ravel()
Y_testM = Y_test.values.ravel()
Y_M = Y
XGBoost.fit(X_trainM, Y_trainM)
timeDF.at[Situation,'XGBoost'] = time.time() - start

RAM1 = accuracy_score(Y_testM, XGBoost.predict(X_testM))
RAM2 = f1_score(Y_testM, XGBoost.predict(X_testM),average='micro')
RAM3 = recall_score(Y_testM, XGBoost.predict(X_testM),average='micro')
    
Result.at[S1,'XGBoost'] = round(RAM1,3)
Result.at[S2,'XGBoost'] = round(RAM2,3)
Result.at[S3,'XGBoost'] = round(RAM3,3)

print(cross_val_score(XGBoost, X_train, Y_train.values.ravel()))


# ### Dimension 2:

# In[11]:


Situation = 'Dim2'
X.loc[:,'N_a'] = data.loc[:,'N_a'].copy()
X_train, X_test , Y_train , Y_test = train_test_split(X,Y,test_size=sizeTesting,random_state=25)


# In[12]:


S1 = str(Situation + ' AS:')
S2 = str(Situation + ' F1:')
S3 = str(Situation + ' RE:')

################################
######### Naive Bayes: #########
################################

start = time.time()
NB = GaussianNB()
NB.fit(X_train, Y_train.values.ravel())
timeDF.at[Situation,'NaiveBayes'] = time.time() - start

RAM1 = accuracy_score(Y_test.values.ravel(), NB.predict(X_test))
RAM2 = f1_score(Y_test.values.ravel(), NB.predict(X_test),average='micro')
RAM3 = recall_score(Y_test.values.ravel(), NB.predict(X_test),average='micro')

Result.at[S1,'NaiveBayes'] = round(RAM1,3)
Result.at[S2,'NaiveBayes'] = round(RAM2,3)
Result.at[S3,'NaiveBayes'] = round(RAM3,3)

print(cross_val_score(NB, X_train, Y_train.values.ravel()))


##################################
######### Decision Tree: #########
##################################

start = time.time()
decTree = tree.DecisionTreeClassifier(max_depth = depthTrees)
decTree.fit(X_train, Y_train)
timeDF.at[Situation,'DecisionTree'] = time.time() - start

RAM1 = accuracy_score(Y_test, decTree.predict(X_test))
RAM2 = f1_score(Y_test, decTree.predict(X_test),average='micro')
RAM3 = recall_score(Y_test, decTree.predict(X_test),average='micro')

Result.at[S1,'DecisionTree'] = round(RAM1,3)
Result.at[S2,'DecisionTree'] = round(RAM2,3)
Result.at[S3,'DecisionTree'] = round(RAM3,3)

print(cross_val_score(decTree, X_train, Y_train.values.ravel()))

##################################
######### Random Forest: #########
##################################

start = time.time()
randomForest = RandomForestClassifier(n_estimators=100, oob_score=True, max_depth = depthTrees)
randomForest.fit(X_train, Y_train.values.ravel())
timeDF.at[Situation,'RandomForest'] = time.time() - start

RAM1 = accuracy_score(Y_test.values.ravel(), randomForest.predict(X_test))
RAM2 = f1_score(Y_test.values.ravel(), randomForest.predict(X_test),average='micro')
RAM3 = recall_score(Y_test.values.ravel(), randomForest.predict(X_test),average='micro')

Result.at[S1,'RandomForest'] = round(RAM1,3)
Result.at[S2,'RandomForest'] = round(RAM2,3)
Result.at[S3,'RandomForest'] = round(RAM3,3)
    
print(cross_val_score(randomForest, X_train, Y_train.values.ravel()))
    
############################
######### XGBoost: #########
############################

X_trainM = X_train.as_matrix()
X_testM = X_test.as_matrix()
X_M = X.as_matrix()

start = time.time()
XGBoost = xgboost.XGBClassifier(n_estimators=100, max_depth=depthTrees)
Y_trainM = Y_train.values.ravel()
Y_testM = Y_test.values.ravel()
Y_M = Y
XGBoost.fit(X_trainM, Y_trainM)
timeDF.at[Situation,'XGBoost'] = time.time() - start

RAM1 = accuracy_score(Y_testM, XGBoost.predict(X_testM))
RAM2 = f1_score(Y_testM, XGBoost.predict(X_testM),average='micro')
RAM3 = recall_score(Y_testM, XGBoost.predict(X_testM),average='micro')
    
Result.at[S1,'XGBoost'] = round(RAM1,3)
Result.at[S2,'XGBoost'] = round(RAM2,3)
Result.at[S3,'XGBoost'] = round(RAM3,3)

print(cross_val_score(XGBoost, X_train, Y_train.values.ravel()))


# ### Dimension 3:

# In[13]:


Situation = 'Dim3'
X.loc[:,'N_ar'] = data.loc[:,'N_ar'].copy()
X_train, X_test , Y_train , Y_test = train_test_split(X,Y,test_size=sizeTesting,random_state=25)


# In[14]:


S1 = str(Situation + ' AS:')
S2 = str(Situation + ' F1:')
S3 = str(Situation + ' RE:')

################################
######### Naive Bayes: #########
################################

start = time.time()
NB = GaussianNB()
NB.fit(X_train, Y_train.values.ravel())
timeDF.at[Situation,'NaiveBayes'] = time.time() - start

RAM1 = accuracy_score(Y_test.values.ravel(), NB.predict(X_test))
RAM2 = f1_score(Y_test.values.ravel(), NB.predict(X_test),average='micro')
RAM3 = recall_score(Y_test.values.ravel(), NB.predict(X_test),average='micro')

Result.at[S1,'NaiveBayes'] = round(RAM1,3)
Result.at[S2,'NaiveBayes'] = round(RAM2,3)
Result.at[S3,'NaiveBayes'] = round(RAM3,3)

print(cross_val_score(NB, X_train, Y_train.values.ravel()))


##################################
######### Decision Tree: #########
##################################

start = time.time()
decTree = tree.DecisionTreeClassifier(max_depth = depthTrees)
decTree.fit(X_train, Y_train)
timeDF.at[Situation,'DecisionTree'] = time.time() - start

RAM1 = accuracy_score(Y_test, decTree.predict(X_test))
RAM2 = f1_score(Y_test, decTree.predict(X_test),average='micro')
RAM3 = recall_score(Y_test, decTree.predict(X_test),average='micro')

Result.at[S1,'DecisionTree'] = round(RAM1,3)
Result.at[S2,'DecisionTree'] = round(RAM2,3)
Result.at[S3,'DecisionTree'] = round(RAM3,3)

print(cross_val_score(decTree, X_train, Y_train.values.ravel()))

##################################
######### Random Forest: #########
##################################

start = time.time()
randomForest = RandomForestClassifier(n_estimators=100, oob_score=True, max_depth = depthTrees)
randomForest.fit(X_train, Y_train.values.ravel())
timeDF.at[Situation,'RandomForest'] = time.time() - start

RAM1 = accuracy_score(Y_test.values.ravel(), randomForest.predict(X_test))
RAM2 = f1_score(Y_test.values.ravel(), randomForest.predict(X_test),average='micro')
RAM3 = recall_score(Y_test.values.ravel(), randomForest.predict(X_test),average='micro')

Result.at[S1,'RandomForest'] = round(RAM1,3)
Result.at[S2,'RandomForest'] = round(RAM2,3)
Result.at[S3,'RandomForest'] = round(RAM3,3)
    
print(cross_val_score(randomForest, X_train, Y_train.values.ravel()))
    
############################
######### XGBoost: #########
############################

X_trainM = X_train.as_matrix()
X_testM = X_test.as_matrix()
X_M = X.as_matrix()

start = time.time()
XGBoost = xgboost.XGBClassifier(n_estimators=100, max_depth=depthTrees)
Y_trainM = Y_train.values.ravel()
Y_testM = Y_test.values.ravel()
Y_M = Y
XGBoost.fit(X_trainM, Y_trainM)
timeDF.at[Situation,'XGBoost'] = time.time() - start

RAM1 = accuracy_score(Y_testM, XGBoost.predict(X_testM))
RAM2 = f1_score(Y_testM, XGBoost.predict(X_testM),average='micro')
RAM3 = recall_score(Y_testM, XGBoost.predict(X_testM),average='micro')
    
Result.at[S1,'XGBoost'] = round(RAM1,3)
Result.at[S2,'XGBoost'] = round(RAM2,3)
Result.at[S3,'XGBoost'] = round(RAM3,3)

print(cross_val_score(XGBoost, X_train, Y_train.values.ravel()))


# ### Dimension 4:

# In[15]:


Situation = 'Dim4'
X.loc[:,'Uc_ar'] = data.loc[:,'Uc_ar'].copy()
X_train, X_test , Y_train , Y_test = train_test_split(X,Y,test_size=sizeTesting,random_state=25)


# In[16]:


S1 = str(Situation + ' AS:')
S2 = str(Situation + ' F1:')
S3 = str(Situation + ' RE:')

################################
######### Naive Bayes: #########
################################

start = time.time()
NB = GaussianNB()
NB.fit(X_train, Y_train.values.ravel())
timeDF.at[Situation,'NaiveBayes'] = time.time() - start

RAM1 = accuracy_score(Y_test.values.ravel(), NB.predict(X_test))
RAM2 = f1_score(Y_test.values.ravel(), NB.predict(X_test),average='micro')
RAM3 = recall_score(Y_test.values.ravel(), NB.predict(X_test),average='micro')

Result.at[S1,'NaiveBayes'] = round(RAM1,3)
Result.at[S2,'NaiveBayes'] = round(RAM2,3)
Result.at[S3,'NaiveBayes'] = round(RAM3,3)

print(cross_val_score(NB, X_train, Y_train.values.ravel()))


##################################
######### Decision Tree: #########
##################################

start = time.time()
decTree = tree.DecisionTreeClassifier(max_depth = depthTrees)
decTree.fit(X_train, Y_train)
timeDF.at[Situation,'DecisionTree'] = time.time() - start

RAM1 = accuracy_score(Y_test, decTree.predict(X_test))
RAM2 = f1_score(Y_test, decTree.predict(X_test),average='micro')
RAM3 = recall_score(Y_test, decTree.predict(X_test),average='micro')

Result.at[S1,'DecisionTree'] = round(RAM1,3)
Result.at[S2,'DecisionTree'] = round(RAM2,3)
Result.at[S3,'DecisionTree'] = round(RAM3,3)

print(cross_val_score(decTree, X_train, Y_train.values.ravel()))

##################################
######### Random Forest: #########
##################################

start = time.time()
randomForest = RandomForestClassifier(n_estimators=100, oob_score=True, max_depth = depthTrees)
randomForest.fit(X_train, Y_train.values.ravel())
timeDF.at[Situation,'RandomForest'] = time.time() - start

RAM1 = accuracy_score(Y_test.values.ravel(), randomForest.predict(X_test))
RAM2 = f1_score(Y_test.values.ravel(), randomForest.predict(X_test),average='micro')
RAM3 = recall_score(Y_test.values.ravel(), randomForest.predict(X_test),average='micro')

Result.at[S1,'RandomForest'] = round(RAM1,3)
Result.at[S2,'RandomForest'] = round(RAM2,3)
Result.at[S3,'RandomForest'] = round(RAM3,3)
    
print(cross_val_score(randomForest, X_train, Y_train.values.ravel()))
    
############################
######### XGBoost: #########
############################

X_trainM = X_train.as_matrix()
X_testM = X_test.as_matrix()
X_M = X.as_matrix()

start = time.time()
XGBoost = xgboost.XGBClassifier(n_estimators=100, max_depth=depthTrees)
Y_trainM = Y_train.values.ravel()
Y_testM = Y_test.values.ravel()
Y_M = Y
XGBoost.fit(X_trainM, Y_trainM)
timeDF.at[Situation,'XGBoost'] = time.time() - start

RAM1 = accuracy_score(Y_testM, XGBoost.predict(X_testM))
RAM2 = f1_score(Y_testM, XGBoost.predict(X_testM),average='micro')
RAM3 = recall_score(Y_testM, XGBoost.predict(X_testM),average='micro')
    
Result.at[S1,'XGBoost'] = round(RAM1,3)
Result.at[S2,'XGBoost'] = round(RAM2,3)
Result.at[S3,'XGBoost'] = round(RAM3,3)

print(cross_val_score(XGBoost, X_train, Y_train.values.ravel()))


# # Result:

# In[17]:


Result


# In[18]:


timeDF


# In[19]:


Result.to_csv(os.path.abspath('Results/Frequency_Results.csv'))
timeDF.to_csv(os.path.abspath('Results/Frequency_ResultsTiming.csv'))

