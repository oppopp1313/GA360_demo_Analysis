# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import gspread
import pandas as pd
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn import datasets


# %%
#Google sheet api & key
auth_json_path = '/Users/oppop/Desktop/Google python.json'
gss_scopes = ['https://spreadsheets.google.com/feeds']

credentials = ServiceAccountCredentials.from_json_keyfile_name(auth_json_path,gss_scopes)
gss_client = gspread.authorize(credentials)

spreadsheet_key = '1RL0D_4kTIO6fsclvRI9Dao7ikDlSh1VP-Qe6f_-28EM' 
sheet = gss_client.open_by_key(spreadsheet_key).worksheet('results-20210115-174158')
Unique_Purchases = sheet.col_values(2)[1:]
Cart_To_Detail_Rate = sheet.col_values(3)[1:]
Buy_To_Detail_Rate = sheet.col_values(4)[1:]
Product_Detail_Views = sheet.col_values(5)[1:]
Product_Adds_To_Cart = sheet.col_values(6)[1:]
Product_Removes_From_Cart = sheet.col_values(7)[1:]
Product_Checkouts = sheet.col_values(8)[1:]


# %%
#以 Add to cart & Remove from cart 來做分群
atc_float = []
rfc_float = []
for i in Product_Adds_To_Cart:
    i = int(i)
    atc_float.append(i)
for i in Product_Removes_From_Cart:
    i = int(i)
    rfc_float.append(i)
a = np.array(atc_float)
r = np.array(rfc_float)
ar_cluster = np.dstack((a,r))[0]
KM=KMeans(n_clusters=4,init='random',random_state=10)
KM.fit(ar_cluster)
KM.predict(ar_cluster)
plt.scatter(ar_cluster[:,0],ar_cluster[:,1],c=KM.predict(ar_cluster))


