'''
Created on 2017年11月26日

@author: Administrator
'''

import tushare as ts
import pandas as pd
import mysql as ms
from queue import Queue
import time
import sqlalchemy as sqlmy
from multiprocessing import Pool
from sqlalchemy import create_engine

def getStockYearData(trunctablename, thisyear, thisquan):
    thismonth=thisquan*3;
    if thismonth<7:
        thisdays=31;
    else:
        thisdays=30;
    print("%d start to get" % thisyear)
#    trunctablename="report_data_tmp";
#    trunctablename="profit_data_tmp";
    
    for num in range(0,10):
        print("%d:" % num, end='')
        if "report_data_tmp" == trunctablename :
            print("get_report_data will be do")
            df = ts.get_report_data(thisyear,thisquan);
        elif "profit_data_tmp" == trunctablename :
            print("get_profit_data will be do")
            df = ts.get_profit_data(thisyear, thisquan);
        else:
            print("nothings to be done!")
    
    return df


def getstock_yeardata(thisyear, thisquanter,engine):
    print("%d start to get" % thisyear)
    trunctablename = 'profit_data_tmp';
    ms.truncateTableData(trunctablename,mcursor,mconnect);
    for num in range(0,10):
        print("%s time(%d)" % (trunctablename,num), end='')
        df = ts.get_profit_data(thisyear, thisquanter);
        df.to_sql(trunctablename, engine, if_exists='append');     

    trunctablename = 'report_data_tmp';
    ms.truncateTableData(trunctablename,mcursor,mconnect);
    for num in range(0,10):
        print("%s time(%d)" % (trunctablename,num), end='')
        df = ts.get_report_data(thisyear,thisquanter);
        df.to_sql(trunctablename, engine, if_exists='append');
    print("get end")
    insertsql=""

if __name__ == '__main__':
    engine = create_engine('mysql://root:root@localhost/finance?charset=utf8')
    
    mconnect=ms.getMysqlConnect()
    mcursor=ms.getMsqlCursor(mconnect)
    datayear=2017
    dataquanter=4
    getstock_yeardata(datayear,dataquanter, engine)
