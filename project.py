#!/usr/bin/env python
# coding: utf-8

# In[368]:


# Import libraries, set options to display all columns in dataframes
import pandas as pd
import numpy as np
import re
import requests
from bigfloat import *
from lxml import html
from functools import partial, reduce 

pd.set_option('display.max_columns', None)  


# In[ ]:





# In[369]:


# Load financial data
subs = pd.read_csv("data/2019q1//sub.txt", sep='\t')# parsedates=['accepted'])) 
pre = pd.read_csv("data/2019q1/pre.txt", sep='\t') 
num = pd.read_csv("data/2019q1/num.txt", sep='\t')#, parsedates=['ddate']) 
tag = pd.read_csv("data/2019q1/tag.txt", sep='\t') 

subs = subs.append(pd.read_csv("data/2019q2/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2019q2/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2019q2/num.txt", sep='\t'))#, parsedates=['ddate']))

subs = subs.append(pd.read_csv("data/2019q3/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2019q3/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2019q3/num.txt", sep='\t'))

subs = subs.append(pd.read_csv("data/2018q1/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2018q1/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2018q1/num.txt", sep='\t'))#, parsedates=['ddate']))

subs = subs.append(pd.read_csv("data/2018q2/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2018q2/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2018q2/num.txt", sep='\t'))#, parsedates=['ddate']))

subs = subs.append(pd.read_csv("data/2018q3/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2018q3/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2018q3/num.txt", sep='\t'))#, parsedates=['ddate']))

subs = subs.append(pd.read_csv("data/2018q4/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2018q4/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2018q4/num.txt", sep='\t'))#, parsedates=['ddate']))


# In[370]:


# Load and clean ticker and ciks
ciks = pd.read_csv('data/ticker_cik.csv')
ciks.dropna(inplace=True)
ciks['cik'] = ciks['cik'].astype('int64')


# In[371]:


new_sub = pd.merge(left=ciks, right=subs, left_on='cik', right_on='cik')
new_sub = new_sub[['ticker', 'adsh', 'name', 'form', 'period', 'fp', 'fy', 'fye']]


# In[372]:


new_sub.head()


# In[373]:


subs.head()


# In[ ]:





# In[ ]:





# In[374]:


new_pre = pre[['adsh', 'line', 'stmt', 'tag', 'plabel', 'negating']]


# In[375]:


new_pre.head()


# In[376]:


sub_pre = pd.merge(left=new_sub, right=new_pre, on='adsh' )


# In[377]:


#### WORKING HERE !!!!!!!!!!!! ####################
sub_pre.head(20)


# In[378]:


new_num = num[['adsh', 'tag', 'version', 'ddate', 'qtrs', 'uom', 'value', 'footnote']]


# In[379]:


merged_items = pd.merge(left=sub_pre, right=new_num, on=['adsh', 'tag'])


# In[380]:


merged_items.head()


# In[381]:


aapl = merged_items[merged_items['ticker']=='aapl']


# In[382]:


statement_list = []
statment_types = aapl['stmt'].unique()
for statement_type in statment_types:
    statement = aapl[aapl['stmt'] == statement_type]
    statement_list.append(statement)  


# In[383]:


q10_list = []
k10_list = []
states = {}

for statement in statement_list:
    annual = statement[statement['form']=='10-K']
    a_stmt_type = statement['stmt']

    quarterly = statement[statement['form']=='10-Q']
    q_stmt_type = statement['stmt']
    
    for version in quarterly['version'].unique():

        v = quarterly[quarterly['version']==version]
        q10 = v.pivot_table(index=['ticker', 'form', 'fy', 'stmt', 'line', 'plabel'], columns=['fp'], values = 'value')
        q10_list.append(q10)
        
        v2 = annual[annual['version']==version]
        k10 = v2.pivot_table(index=['ticker', 'form', 'stmt', 'line', 'plabel'], columns=['fye'], values = 'value')
        k10_list.append(k10)
        
states['10-Q'] = q10_list
states['10-K'] = k10_list
        


# In[384]:


##### FOR QUARTERLY STATEMENTS
for i in range(0,len(states['10-Q'])):
    m = states['10-Q'][i].unstack(level=2)
    display(m)


# In[359]:


########## FOR ANNUAL STATEMENET
for i in range(0,len(states['10-K'])):
    m = states['10-K'][i]
    display(m)


# In[314]:


for i in states['10-Q']:
    display(i)


# In[323]:


states['10-Q'][0].unstack(level=2)


# In[ ]:




