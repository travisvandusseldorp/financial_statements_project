#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Import libraries, set options to display all columns in dataframes
import pandas as pd
import numpy as np
import re
import requests
from bigfloat import *
from lxml import html
from functools import partial, reduce 

idx = pd.IndexSlice
pd.set_option('display.max_columns', None)  
pd.__version__


# In[ ]:


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


# In[ ]:


# Load and clean ticker and ciks
ciks = pd.read_csv('data/ticker_cik.csv')
ciks.dropna(inplace=True)
ciks['cik'] = ciks['cik'].astype('int64')


# In[ ]:


new_sub = pd.merge(left=ciks, right=subs, left_on='cik', right_on='cik')
new_sub = new_sub[['ticker', 'adsh', 'name', 'form', 'period', 'fp', 'fy', 'fye']]


# In[ ]:


new_pre = pre[['adsh', 'report', 'line', 'stmt', 'tag', 'plabel', 'negating', 'inpth']]


# In[ ]:


sub_pre = pd.merge(left=new_sub, right=new_pre, on='adsh')


# In[ ]:


new_num = num[['adsh', 'tag', 'version', 'ddate', 'qtrs', 'uom', 'value', 'footnote']]


# In[ ]:





# In[ ]:


new_sub = pd.merge(left=ciks, right=subs, left_on='cik', right_on='cik')
sub_pre = pd.merge(left=new_sub, right=new_pre, on='adsh' )


# In[ ]:


merged_items = pd.merge(left=sub_pre, right=new_num, on=['adsh', 'tag'])


# In[ ]:


merged_items.head()


# In[ ]:


aapl = merged_items[merged_items['ticker']=='aapl']


# In[ ]:


print(aapl['report'].unique())
print(aapl['stmt'].unique())
for i in range(1,9):
    print(i)
    q = aapl[aapl['report']==i]
    display(q)


# In[ ]:


mv = aapl.pivot_table(index=['adsh', 'stmt', 'fp', 'report', 'form', 'qtrs', 'version', 'fye', 'line', 'plabel'], columns=['ddate'], values = 'value')
all_bs = mv
all_bs


# In[ ]:


adsh_nums = list(all_bs.index.levels[0])
all_bs2 = all_bs.reset_index(['adsh'])


adsh_group = all_bs2.groupby('adsh')
num_filings = len(all_bs2['adsh'].unique())

w = {}

bs_list = []
is_list = []

for i in range(0,num_filings):
    first_adsh = adsh_group.get_group(adsh_nums[i])
    c = first_adsh.drop('adsh', axis = 1)
    
    drops = []
    levs = list(c.index.levels)
    for i in levs:
        if len(i) <= 1:
            drops.append(i.name)
    
    d = c.reset_index(drops, drop=True)
    
    
    argh = []
    e = d.reset_index('version')
    versions = e['version'].unique()
    f = e.reset_index('qtrs')
    g = f.drop(['version'], axis = 1)
    
    h = g.reset_index('report', drop=True)
    i = h
    j = i[i['qtrs'] != 4]
    k = j.drop('qtrs', axis=1)
    #k = j.unstack(1)
    y = k.dropna(axis=1, thresh=2)
    z = y.reset_index('line', drop=True)
    display(z)

    for i in mq:
        try:
            u = z.loc[i].dropna(axis=1)
            if i == 'BS':
                bs_list.append(u)
            else:
                is_list.append(u)
        except Exception:
            pass


# In[ ]:


for i in bs_list:
    display(i)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


################################################################################################################


# In[ ]:


y.loc['BS']


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


tester.droplevel(1)zz


# In[ ]:


t4 = t3.reset_index(level='version')
t4.reset_index(['ticker','qtrs', 'fye'])


# In[ ]:





# In[ ]:


t5 = t4.groupby(['version'])
t5.get_group('us-gaap/2017')


# In[ ]:


t4['version'].unique()


# In[ ]:





# In[ ]:





# In[ ]:


# Create list containing different states in the data frame
statement_list = []
statment_types = aapl['stmt'].unique()
for statement_type in statment_types:
    statement = aapl[aapl['stmt'] == statement_type]
    statement_list.append(statement)  


# In[ ]:


q10_list = []
k10_list = []
states = {}

for statement in statement_list:
    annual = statement[statement['form']=='10-K']
    
############ INCOME STATEMENT NEEDS 4 QTRS, BALANCE SHEET DOES NOT    
    annual = annual[annual['qtrs']==4]
    a_stmt_type = statement['stmt']

    quarterly = statement[statement['form']=='10-Q']
    q_stmt_type = statement['stmt']
    
    for version in quarterly['version'].unique():

        v = quarterly[quarterly['version']==version]
        q10 = v.pivot_table(index=['ticker', 'form', 'stmt', 'line', 'plabel'], columns=['ddate', 'fp'], values = 'value')
        q10_list.append(q10)
        
        v2 = annual[annual['version']==version]
        k10 = v2.pivot_table(index=['ticker', 'form', 'stmt', 'line', 'plabel'], columns=['ddate', 'fp'], values = 'value')
        k10_list.append(k10)
        
states['10-Q'] = q10_list
states['10-K'] = k10_list


# In[ ]:


#statement_pivots = []
#for i in statement_list:
#    statement_pivots.append(i.pivot_table(index=['ticker', 'ddate', 'form', 'stmt', 'qtrs', 'version', 'fye', 'line', 'plabel'], columns=['fp'], values = 'value'))
mv = aapl.pivot_table(index=['adsh', 'ticker', 'ddate', 'form', 'stmt', 'qtrs', 'version', 'fye', 'line', 'plabel'], columns=['fp'], values = 'value')


# In[ ]:


statement_pivots[0]


# In[ ]:





# In[ ]:


quarter_l = []
for i in range(0,len(states['10-Q'])):
    m = states['10-Q'][i]
    display(m)
    quarter_l.append(m)
    
    

statment_labels = []

for i in annual_l:
    
    stt = i.index.get_level_values(2).unique()
    statment_labels.append(list(stt))

statement_labels = [''.join(x) for x in statment_labels]


# In[ ]:


mamba = dict(zip(statement_labels,quarter_l))


# In[ ]:


########## FOR ANNUAL STATEMENET
annual_l = []
for i in range(0,len(states['10-K'])):
    m = states['10-K'][i]
    display(m)
    annual_l.append(m)


# In[ ]:


year_labels2 = []
statment_labels = []
annual_statments_clean = []
for i in annual_l:
    
    stt = i.index.get_level_values(2).unique()
    statment_labels.append(list(stt))
    
statement_labels = [''.join(x) for x in statment_labels]


# In[ ]:


mamba2 = dict(zip(statement_labels,annual_l))
mamba2['IS']


# In[ ]:


mk = mamba2['IS']
mk


# In[ ]:


mq = mamba['IS']
mq


# In[ ]:


r = pd.merge(left = mk, right = mq, on='plabel')


# In[ ]:


v = r.reindex(sorted(r.columns, reverse=True), axis=1)
v


# In[ ]:


v[v.columns[::-1]]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


##### NEED TO FIGURE THIS OUT.... maybe due to the common numbers like below
#fy_convert = pd.to_numeric(aapl['fy'], downcast='integer')

#### Converts year labels to int but will only keep one of each year
#[int(i) for i in year_labels]

