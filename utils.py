# functions defined to process data of various try format
import pandas as pd


def processPrice(strData):
    if strData[0] == '$':
        return int(strData[1:])
    else:
        return None

def processDate(strData):
    try:
        return pd.to_datetime(strData)
    except:
        return None