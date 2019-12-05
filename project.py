#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Load libraries, set options to display all columns in dataframes

import pandas as pd
import numpy as np
from functools import partial, reduce 
#import re
#import requests
#from bigfloat import *
#from lxml import html

idx = pd.IndexSlice
pd.set_option('display.max_columns', None)  
pd.__version__


# In[ ]:


##### Data loading and preparing
###
# Load financial data
sub = pd.read_csv("data/2019q1//sub.txt", sep='\t')# parsedates=['accepted'])) 
pre = pd.read_csv("data/2019q1/pre.txt", sep='\t') 
num = pd.read_csv("data/2019q1/num.txt", sep='\t')#, parsedates=['ddate']) 
tag = pd.read_csv("data/2019q1/tag.txt", sep='\t') 

sub = sub.append(pd.read_csv("data/2019q2/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2019q2/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2019q2/num.txt", sep='\t'))#, parsedates=['ddate']))

sub = sub.append(pd.read_csv("data/2019q3/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2019q3/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2019q3/num.txt", sep='\t'))

sub = sub.append(pd.read_csv("data/2018q1/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2018q1/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2018q1/num.txt", sep='\t'))#, parsedates=['ddate']))

sub = sub.append(pd.read_csv("data/2018q2/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2018q2/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2018q2/num.txt", sep='\t'))#, parsedates=['ddate']))

sub = sub.append(pd.read_csv("data/2018q3/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2018q3/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2018q3/num.txt", sep='\t'))#, parsedates=['ddate']))

sub = sub.append(pd.read_csv("data/2018q4/sub.txt", sep='\t'))
pre = pre.append(pd.read_csv("data/2018q4/pre.txt", sep='\t'))
num = num.append(pd.read_csv("data/2018q4/num.txt", sep='\t'))#, parsedates=['ddate']))


# In[ ]:


# Load and clean ticker and ciks
ciks = pd.read_csv('data/ticker_cik.csv')
ciks.dropna(inplace=True)


# In[ ]:


# Merge ciks with subs, select necessary columns from submission dataframes
new_sub = pd.merge(left=ciks, right=sub, left_on='cik', right_on='cik')
new_sub = new_sub[['ticker', 'adsh', 'name', 'form', 'period', 'fp', 'fy', 'filed']]
new_num = num[['adsh', 'tag', 'version', 'ddate', 'qtrs', 'value', 'footnote']]
new_pre = pre[['adsh', 'line', 'stmt', 'tag', 'plabel', 'negating']]


# In[ ]:


# Merge new submission dataframes into on dataframe -- merged_items
sub_pre = pd.merge(left=new_sub, right=new_pre, on='adsh' )
merged_items = pd.merge(left=sub_pre, right=new_num, on=['adsh', 'tag'])


# In[ ]:


merged_items.head(2)


# In[ ]:


####################################################################################################
# End of loading and preparing


# In[ ]:





# In[ ]:


### Functions


# In[ ]:


# Select company submissions using ticker
def find_company_submissions(ticker):
    comp_sub = merged_items[merged_items['ticker']== ticker]
    return(comp_sub)


# In[ ]:


# Filter dataframe based on statement
def filter_statement(comp_df, stmt):
    comp_statement = comp_df[comp_df['stmt'] == stmt]
    return(comp_statement)


# In[ ]:


def split_stmt_subs(comp_stmt):
    adsh_nums = comp_stmt['adsh'].unique()
    sngl_sub_list = []
    
    for num in adsh_nums:
        sngl_sub = comp_stmt[comp_stmt['adsh'] == num]
        sngl_sub_list.append(sngl_sub)
    return(sngl_sub_list)


# In[ ]:


# Create pivot table from the company statement
#def make_pivot_table(stmt_df):
#    pv_table = stmt_df.pivot_table(index=['adsh', 'stmt', 'fp', 'form', 'qtrs', 'version', 'filed', 'line','negating',
#                                          'plabel', 'tag'], columns=['ddate'], values = 'value')
#    return(pv_table)


# In[ ]:


# Get filings that have values reflection only 1 quarter
def get_quarterly_values(cln_list):
    quarterly_results = []
    for stmt in cln_list:
        quarterly_statement = stmt.xs(1)
        quarterly_results.append(quarterly_statement)
    return(quarterly_results)


# In[ ]:


##### Clean pivot table for combining
# Create pivot table from the company statement
def make_pivot_table(stmt_df):
    pv_table = stmt_df.pivot_table(index=['adsh', 'stmt', 'qtrs', 'ddate', 'form', 'version', 'filed', 'line','negating',
                                          'plabel', 'tag'], columns=['fp'], values = 'value')
    return(pv_table)


# In[ ]:


def make_list_of_pivot_tables(comp_list):
    pivoted_stmt_list = []
    for sub in comp_list:
        pv_stmt = make_pivot_table(sub)
        pivoted_stmt_list.append(pv_stmt)
    pivoted_stmt_list[1]
    pivoted_stmt_list[0].append(pivoted_stmt_list[1])
    return(pivoted_stmt_list)


# In[ ]:


##### Clean pivot table for combining
### REVIST HOW TO HANDLE NEGATING
def clean_pivot_table(pvt_tb):
    clean_pv_tb = pvt_tb.reset_index(['adsh', 'stmt', 'negating'], drop=True)
    return(clean_pv_tb)


# In[ ]:


# Get filings that have values reflection only 1 quarter
def get_quarterly_values(cln_list):
    quarterly_results = []
    for stmt in cln_list:
        quarterly_statement = stmt.xs(1)
        if quarterly_statement.index.get_level_values('form')[0] == '10-Q':
            quarterly_results.append(quarterly_statement)
        else:
            pass
    return(quarterly_results)


# In[ ]:


def combine_tables(tb_list):
    comb_tb = reduce(lambda x, y: x.append(y, sort=False), tb_list[:5])
    return(comb_tb)


# In[ ]:


### MAY BE BETTER TO USE SUBMISSION DATE????
def seperate_by_dd_date(df):
    filings_by_ddate = []
    for i in df.index.get_level_values('filed').unique().to_list():
        filings_by_ddate.append(df.xs(i, level='filed'))
    return(filings_by_ddate)


# In[ ]:


def clean_dfs(seperated_df):
    cleaned_list = []
    for i in seperated_df:
        tmp = i.dropna(axis=1).reset_index(['ddate'])
        tmp2 = tmp.pivot(columns='ddate')
        tmp3 = tmp2.reset_index(['form', 'line', 'plabel'], drop=True)
        cleaned_list.append(tmp3)
    return(cleaned_list)


# In[ ]:


tickers = ['tsla', 'bke', 'wmt', 'f', 'bbby', 'mck']
results = []

for ticker in tickers:
    aapl = find_company_submissions(ticker)

    aapl_is = filter_statement(aapl, 'IS')    
    aapl_subs_list = split_stmt_subs(aapl_is)  
    pv_tb = []
    for i in aapl_subs_list:
        tmp = make_pivot_table(i)
        pv_tb.append(clean_pivot_table(tmp))

    quarterly_results = get_quarterly_values(pv_tb)

        
    g = combine_tables(quarterly_results)
  
    filings_by_ddate = seperate_by_dd_date(g)
    cleaned = clean_dfs(filings_by_ddate)
    
    v_2017 = []
    v_other = []
    for i in cleaned:
        lvls = i.index.get_level_values(0).unique().to_list()
        if 'us-gaap/2017' in lvls:
            tmp = i.xs('us-gaap/2017', level='version')
            v_2017.append(tmp)
        else:
            v_other.append(i)
    for i in v_2017:
        tmp = reduce(lambda x,y: pd.merge(x, y, on='tag'), v_2017)
        results.append(tmp)
    for i in v_other:
        tmp = reduce(lambda x,y: pd.merge(x, y, on='tag'), v_other)
        results.append(tmp)


# In[ ]:


for i in results:
    display(i)


# In[ ]:


########################################################################


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


###
#def create_new_index

lines_2017 = quarterly_results[4].index.get_level_values('line').to_list()[:-1] 
tags_2017 = quarterly_results[4].index.get_level_values('tag').to_list()[:-1]
tup_2017 = tuple(zip(lines_2017,tags_2017))

new_results = []
for i in quarterly_results[:5]:
    display(i.reset_index('filed'))
    #i = i.unstack(1)
    if i.index.values[0][0] == 'us-gaap/2018':
        i.reset_index('version',drop=True, inplace=True)
        i.index = pd.MultiIndex.from_tuples(tup_2017, names=['line', 'tag'])
        new_results.append(i)
    else:
        i.drop(index='CommonStockDividendsPerShareDeclared', level = 1, inplace=True)
        i.reset_index('version',drop=True, inplace=True)
        new_results.append(i)

#pd.concat(quarterly_results, axis=1, sort=False)
g = reduce(lambda x, y: pd.merge(x, y, on=['tag', 'filed'], how='outer'), new_results)

