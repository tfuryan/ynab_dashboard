
# coding: utf-8

# In[1]:

from ynab import YNAB
import pandas as pd
import numpy as np
import re


# In[2]:

def attr_series(attr):
    list_series = []
    for line in attr:
        list_series.append(re.sub(r'(^<[A-Za-z]+: )(.+)(>$)','\g<2>',str(line)))
    return list_series

def sub_trans_list(subTrans):
    list_series = []
    for line in subTrans:
        if line:
            list_series.append(attr_series(line))
        else:
            line = None
            list_series.append(line)
    return list_series


def transactions_to_dataframe(ynab_budget_path,budget_name):

    ynab_budget = YNAB(ynab_budget_path,budget_name)


    # In[4]:

    budget_df = pd.DataFrame(pd.Series(ynab_budget.transactions.date),columns=['Dates'])

    # Getting the account where the transaction lives
    budget_df['Account'] = pd.Series(attr_series(ynab_budget.transactions.account))
    budget_df['Payee'] = pd.Series(attr_series(ynab_budget.transactions.payee))
    budget_df['Category'] = pd.Series(attr_series(ynab_budget.transactions.category))
    budget_df['SubTrans'] = pd.Series(sub_trans_list(ynab_budget.transactions.sub_transactions))
    budget_df['Memo'] = pd.Series(attr_series(ynab_budget.transactions.memo))
    budget_df['Amount'] = pd.Series(attr_series(ynab_budget.transactions.amount))


    # In[5]:

    subtrans_df = budget_df[~budget_df.SubTrans.isnull()]
    exploded_df = pd.DataFrame(np.nan, index=[], columns=['Dates','Account','Payee','ParentCategory',
                                                          'Parent Amount','SubTransCategory','SubTransAmount'])
    for index, row in subtrans_df.iterrows():
        for line in row['SubTrans']:
            temp_df = pd.DataFrame(np.nan, index=[0], columns=exploded_df.columns)
            temp_df['Dates'] = row['Dates']
            temp_df['Account'] = row['Account']
            temp_df['Payee'] = row['Payee']
            temp_df['ParentCategory'] = str(row['Category'])
            temp_df['ParentAmount'] = row['Amount']
            temp_df['SubTransCategory'] = re.sub(r"((\-)?\d+\.\d+) \((.+)(\))","\g<3>",line)
            temp_df['SubTransAmount'] = re.sub(r"((\-)?\d+\.\d+) \((.+)(\))","\g<1>",line)
            exploded_df = exploded_df.append(temp_df, ignore_index=True)


    # In[6]:

    test = pd.merge(subtrans_df,exploded_df,how="outer",left_on=['Dates','Account','Payee','Category','Amount'],
                   right_on=['Dates','Account','Payee','ParentCategory','ParentAmount'])

    test2 = test[['Dates','Account','Payee','Category','Memo','Amount','SubTransCategory','SubTransAmount']]


    # In[7]:

    test3 = pd.merge(budget_df,test2, how="outer",on=['Dates','Account','Payee','Memo','Category','Amount'])


    # In[8]:

    idx = test3[~test3.SubTransAmount.isnull()].index
    test3.iloc[idx,6] = test3.iloc[idx,8]
    test3['MasterCategory'] = pd.DataFrame(test3.Category.str.split("/").tolist()).ix[:,0]
    test3['SubCategory'] = pd.DataFrame(test3.Category.str.split("/").tolist()).ix[:,1]
    test3.iloc[idx,10] = test3.iloc[idx,7]


    # In[9]:

    test4 = pd.DataFrame(pd.Series(attr_series(ynab_budget.categories)),columns=['Cats'])
    test4['MasterCategory'] = pd.DataFrame(test4.Cats.str.split("/").tolist()).ix[:,0]
    test4['SubCategory'] = pd.DataFrame(test4.Cats.str.split("/").tolist()).ix[:,1]
    test4 = test4[~test4.MasterCategory.isin(['Hidden Categories'])]


    # In[10]:

    split_categories_df = test3.copy()
    for index, cat in test4.iterrows():
        list_subcats = []
        list_subcats.append(cat.SubCategory)
        split_categories_df.iloc[test3[test3.SubCategory.isin(list_subcats)].index,9] = cat.MasterCategory


    # In[11]:

    budget_accounts = pd.DataFrame(pd.Series(attr_series(ynab_budget.accounts)),columns=['Account'])
    budget_accounts['onBudgetAcct'] = ynab_budget.accounts.on_budget

    test6 = pd.merge(split_categories_df,budget_accounts, how="left", on=['Account'])

    test6['TransferAcct'] = pd.DataFrame(test6.Payee.str.split(" : ").tolist()).ix[:,1]


    # In[12]:

    test7 = test6.copy()
    test7.iloc[test6[~test6.TransferAcct.isnull()].index,2] = test7.iloc[test6[~test6.TransferAcct.isnull()].index,12]
    test7.Amount = pd.to_numeric(test7.Amount)
    final_budget_df = test7[['Dates','Account','Payee','MasterCategory','SubCategory','Memo','Amount','onBudgetAcct','TransferAcct']].copy()


    # In[13]:

    return final_budget_df


# In[ ]:
