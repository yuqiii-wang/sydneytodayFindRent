import urllib3
import json
import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from utils import processPrice, processDate


refreshDatabase = False # if set to True, the source data will be re-read from server

http = urllib3.PoolManager()

url = "http://www.sydneytoday.com/house_rent-ha0-fs0-hx0-so0-jg0-cw0-mv0-p"
jsonFile = "jsonResponse.json"
dirName = "responseData/"
numRequests = 500

# request data from the server and write the response down
if refreshDatabase:
    if (not os.path.isdir(dirName)):
        os.makedirs(dirName)
    _jsonFileNameSplit = jsonFile.split('.')
    for i in range(numRequests):
        jsonFileName = _jsonFileNameSplit[0] + '_' + str(i+1) + '.' + _jsonFileNameSplit[1]
        with open(dirName + jsonFileName, "w") as openfile:
            response = http.request('GET', url + str(i+2))
            openfile.write(response.data.decode('UTF-8')) # 'Content-Type' can be found in response.headers 

# read data from .json
dataList = []
fileList = glob.glob(dirName + "*.json")
for eachFile in fileList:
    with open(eachFile) as openfile:
        jsonData = json.load(openfile)
        rowData = jsonData['data']['rows']
        for eachRow in rowData:
            dataList.append(eachRow)

rentDataFrame = {}
rentDataFrame['id'] = []
rentDataFrame['price'] = []
rentDataFrame['inDate'] = []
rentDataFrame['rentArea'] = []
rentDataFrame['houseType'] = [] # e.g. apartment or house
rentDataFrame['rentType'] = [] # e.g. by sharing or individual
for eachRow in dataList:

    # use id to avoid duplicated rent request
    try:
        rentDataFrame['id'].append(eachRow['_id'])
    except:
        continue

    try:
        rentDataFrame['price'].append(processPrice(eachRow['jiage']))
    except:
        rentDataFrame['price'].append(None)

    try:
        rentDataFrame['inDate'].append(processDate(eachRow['indate']))
    except:
        rentDataFrame['inDate'].append(None)

    try:
        rentDataFrame['rentArea'].append(eachRow['rentarea'])
    except:
        rentDataFrame['rentArea'].append(None)

    try:
        rentDataFrame['houseType'].append(eachRow['huxings'])
    except:
        rentDataFrame['houseType'].append(None)

    try:
        rentDataFrame['rentType'].append(eachRow['fangshi'][0]['type'])
    except:
        rentDataFrame['rentType'].append(None)


rentDataFrame = pd.DataFrame(rentDataFrame)
rentDataFrame = rentDataFrame.dropna()
rentDataFrame = rentDataFrame.drop_duplicates()

# drop rows deemed illegal, e.g. price over 10k per week
rentDataFrame = rentDataFrame[rentDataFrame['price'] < 10000]
rentDataFrame = rentDataFrame[rentDataFrame['inDate'] > datetime.today() - timedelta(days=666)]

mean_keyInfoGrpBy = rentDataFrame.groupby(['rentArea', 'rentType', 'houseType'])
mean_keyInfo = pd.DataFrame(mean_keyInfoGrpBy['price'].mean(), columns=['price']).rename(columns={'price': 'price_mean'})
mean_keyInfo['price_median'] = mean_keyInfoGrpBy['price'].median()
mean_keyInfo['price_top'] = mean_keyInfoGrpBy['price'].max()
mean_keyInfo['price_bottom'] = mean_keyInfoGrpBy['price'].min()
mean_keyInfo['count'] = mean_keyInfoGrpBy['price'].count()
#mean_keyInfo['price_variance'] = mean_keyInfoGrpBy['price'].std()
mean_keyInfo = mean_keyInfo.fillna(0) # std_var over one sample 

# tune the dataframe, where illegitimate posts are deleted (e.g. burwood house rent of $200 per week)
# formula: bottom below the average more than 70% 
# and top above the average more than 3 times and surpassing the second highest more than one time
rentDataFrameAdjusted = pd.DataFrame(mean_keyInfoGrpBy['price'].apply(lambda x: x[np.bitwise_and(x > x.mean()*0.3, x < x.mean()*3)].mean()))
rentDataFrameAdjusted = rentDataFrameAdjusted.rename(columns={'price': 'price_mean_adjusted'})
rentDataFrameAdjusted['price_bottom_adjusted'] = pd.DataFrame(mean_keyInfoGrpBy['price'].apply(lambda x: x[np.bitwise_and(x > x.mean()*0.3, x < x.mean()*3)].min()))
rentDataFrameAdjusted['count_adjusted'] = pd.DataFrame(mean_keyInfoGrpBy['price'].apply(lambda x: x[np.bitwise_and(x > x.mean()*0.3, x < x.mean()*3)].count()))
mean_keyInfo = mean_keyInfo.merge(rentDataFrameAdjusted, left_index=True, right_index=True, how='inner')

trend_keyInfoGrpBy = rentDataFrame.groupby([rentDataFrame['inDate'].dt.date, 'rentArea'])
trend_keyInfo = pd.DataFrame(trend_keyInfoGrpBy['price'].mean(), columns=['price']).rename(columns={'price': 'price_mean'})
trend_keyInfo['count'] = trend_keyInfoGrpBy['price'].count()
# find the top 30 districts and plot their price and supply trends
trendTop30_keyInfo = trend_keyInfo.reset_index()
sorted_districts = pd.DataFrame(trendTop30_keyInfo.groupby(['rentArea'])['count'].sum()).sort_values('count', ascending=False)
sorted_districts = sorted_districts[:30]
trendTop30_keyInfo = trendTop30_keyInfo[trendTop30_keyInfo['rentArea'].apply(lambda x: x in sorted_districts['count'])]

# output
mean_keyInfo_output = mean_keyInfo.reset_index()
mean_keyInfo_output.to_excel("sydneyTodayRent.xlsx", index=False)