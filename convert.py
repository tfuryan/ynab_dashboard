
# coding: utf-8

from ynab import YNAB
import pandas as pd
import numpy as np
import re

# Strips the attribute format that the pynab library returns into just the main text
def attr_series(attr):
    list_series = []
    for line in attr:
        list_series.append(re.sub(r'(^<[A-Za-z]+: )(.+)(>$)','\g<2>',str(line)))
    return list_series

# Turning sub transaction information returned from pynab into a list
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

    '''
    Main function that will convert ynab budget files into a flat workable pandas dataframe by
    taking in the YNAB path and budget name
    Example:

        mybudget_dataframe = transactions_to_dataframe('~/Dropbox/YNAB','My Budget')
    '''
    # Importing ynab data using pynab libraries
    ynab_budget = YNAB(ynab_budget_path,budget_name)
    # Creating initial dataframe with dates of all transactions from entire budget
    budget_df = pd.DataFrame(pd.Series(ynab_budget.transactions.date),columns=['Dates'])

    # Appending various attributes of the transactions within the budget
    budget_df['Account'] = pd.Series(attr_series(ynab_budget.transactions.account))
    budget_df['Payee'] = pd.Series(attr_series(ynab_budget.transactions.payee))
    budget_df['Category'] = pd.Series(attr_series(ynab_budget.transactions.category))
    budget_df['SubTrans'] = pd.Series(sub_trans_list(ynab_budget.transactions.sub_transactions))
    budget_df['Memo'] = pd.Series(attr_series(ynab_budget.transactions.memo))
    budget_df['Amount'] = pd.Series(attr_series(ynab_budget.transactions.amount))

    # Subsetting budget dataframe where there exists sub transactions
    subtrans_df = budget_df[~budget_df.SubTrans.isnull()]

    # Creating empty dataframe for population of sub transactions
    exploded_df = pd.DataFrame(np.nan, index=[], columns=['Dates','Account','Payee','ParentCategory',
                                                          'ParentAmount','SubTransCategory','SubTransAmount'])

    # Populating sub transactions into dataframe where each sub transaction is its own row
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

    # Merging the 'compacted' sub transaction dataframe with the exploded dataframe
    merged_subtrans_df = pd.merge(subtrans_df,exploded_df,how="outer",
                                    left_on=['Dates','Account','Payee','Category','Amount'],
                                    right_on=['Dates','Account','Payee','ParentCategory','ParentAmount'])

    # Removing extra columns
    merged_subtrans_df = merged_subtrans_df[['Dates','Account','Payee','Category','Memo','Amount','SubTransCategory','SubTransAmount']]

    # Merging exploded sub transactions with correct column names into main budget dataframe
    budget_subtrans_df = pd.merge(budget_df,merged_subtrans_df, how="outer",
                                    on=['Dates','Account','Payee','Memo','Category','Amount'])

    # Overwriting sub transaction's original category of 'None' with real categories
    idx = budget_subtrans_df[~budget_subtrans_df.SubTransAmount.isnull()].index
    budget_subtrans_df.iloc[idx,6] = budget_subtrans_df.iloc[idx,8]
    budget_subtrans_df['MasterCategory'] = pd.DataFrame(budget_subtrans_df.Category.str.split("/").tolist()).ix[:,0]
    budget_subtrans_df['SubCategory'] = pd.DataFrame(budget_subtrans_df.Category.str.split("/").tolist()).ix[:,1]
    budget_subtrans_df.iloc[idx,10] = budget_subtrans_df.iloc[idx,7]

    # Creating a dataframe that maps all available master cateories and sub categories
    category_map = pd.DataFrame(pd.Series(attr_series(ynab_budget.categories)),columns=['Cats'])
    category_map['MasterCategory'] = pd.DataFrame(category_map.Cats.str.split("/").tolist()).ix[:,0]
    category_map['SubCategory'] = pd.DataFrame(category_map.Cats.str.split("/").tolist()).ix[:,1]
    category_map = category_map[~category_map.MasterCategory.isin(['Hidden Categories'])]

    # Populating sub transactions' master and sub categories from the category_map
    split_categories_df = budget_subtrans_df.copy()
    for index, cat in category_map.iterrows():
        list_subcats = []
        list_subcats.append(cat.SubCategory)
        split_categories_df.iloc[budget_subtrans_df[budget_subtrans_df.SubCategory.isin(list_subcats)].index,9] = cat.MasterCategory

    # Adding column that flags whether the transaction's account is off budget or on (account name is populated)
    budget_accounts = pd.DataFrame(pd.Series(attr_series(ynab_budget.accounts)),columns=['Account'])
    budget_accounts['onBudgetAcct'] = ynab_budget.accounts.on_budget

    # Splitting payee information if it is a transfer
    transfers_df = pd.merge(split_categories_df,budget_accounts, how="left", on=['Account'])
    transfers_df['TransferAcct'] = pd.DataFrame(transfers_df.Payee.str.split(" : ").tolist()).ix[:,1]

    # Renaming payee that originally contained the "Transfer :" text to just be the account name
    transfer_acct_df = transfers_df.copy()
    transfer_acct_df.iloc[transfers_df[~transfers_df.TransferAcct.isnull()].index,2] = transfer_acct_df.iloc[transfers_df[~transfers_df.TransferAcct.isnull()].index,12]
    transfer_acct_df.Amount = pd.to_numeric(transfer_acct_df.Amount)

    # Creating final dataframe with the relevant columns
    final_budget_df = transfer_acct_df[['Dates','Account','Payee','MasterCategory','SubCategory','Memo','Amount','onBudgetAcct','TransferAcct']].copy()

    return final_budget_df
