import urllib3
import json
import os
import glob
import pandas as pd
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

mean_keyInfoGrpBy = rentDataFrame.groupby(['rentArea', 'rentType', 'houseType'])
mean_keyInfo = pd.DataFrame(mean_keyInfoGrpBy['price'].mean(), columns=['price']).rename(columns={'price': 'price_mean'})
mean_keyInfo['price_median'] = mean_keyInfoGrpBy['price'].median()
mean_keyInfo['count'] = mean_keyInfoGrpBy['price'].count()
mean_keyInfo['variance'] = mean_keyInfoGrpBy['price'].std()
mean_keyInfo = mean_keyInfo.fillna(0) # std_var over one sample 

