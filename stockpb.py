'''
Created on 2017年7月9日

@author: Administrator
https://github.com/waditu/tushare
'''


        
## 头文件
import tushare as ts
import pandas as pd
import mysql as ms
import gc
from threading import Thread
from queue import Queue
import time
import sqlalchemy as sqlmy
from multiprocessing import Pool
from sqlalchemy import create_engine
'''
    说明get_h_data的数据格式是date open high close low volume amount
  对应的market的表内容为stockcode,stockdate,openprice,highprice,closeprice,lowprice,volume,amount
'''
# 新增数据获取

def getAllNewData(strstockcode): 
    print("getAllNewData")
    df=ts.get_h_data(strstockcode,start='2017-01-01',end='2017-07-07') # 对应marcket表,读取数据，格式为DataFrame  
#    mconnect=ms.getMysqlConnect()
    ts.get_today_all()
#    mcursor=ms.getMsqlCursor(mconnect)
#    strsql="""insert into stockmarket (stockcode,stockdate,openprice,highprice,closeprice,lowprice,volume,amount) values ('dragon','great',20,'M')"""
#    ms.execNotSelectMysql(mcursor, strsql, mconnect)
    return df

def getStockYearData(trunctablename):
    thisyear=2011;
    thisquan=4;
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

def threadGetBeforeMarketData(engine,trunctablename,stockcode,startdate,enddate):         
    df = ts.get_k_data(stockcode,start=startdate, end=enddate)
    df.to_sql(trunctablename, engine, if_exists='append');

class DownloadBerMarketDataWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            # 从队列中获取任务并扩展tuple
            engine,trunctablename,stockcode,startdate,enddate = self.queue.get()
            threadGetBeforeMarketData(engine,trunctablename,stockcode,startdate,enddate)
            self.queue.task_done()

if __name__ == '__main__':
    
    engine = create_engine('mysql://root:root@localhost/finance?charset=utf8')
    
    mconnect=ms.getMysqlConnect()
    mcursor=ms.getMsqlCursor(mconnect)
    
    trunctablename = "today_all_tmp";
    print(" ")
    ms.truncateTableData(trunctablename,mcursor,mconnect);
    if "profit_data_tmp" == trunctablename :
        df = getStockYearData(trunctablename);
        df.to_sql(trunctablename, engine, if_exists='append'); 
    elif  "report_data_tmp" == trunctablename :
        df = getStockYearData(trunctablename);
        df.to_sql(trunctablename, engine, if_exists='append'); 
    elif "k_data_tmp" == trunctablename :
        qq=ts.get_today_all()
        for i in qq['code']:
            print(i)
            df = ts.get_k_data(i)
            df.to_sql(trunctablename, engine, if_exists='append'); 
    elif "k_data_tmp_one" == trunctablename :
        print("get_k_date one")
        df = ts.get_k_data("000001",start='2014-10-01', end='2015-01-21')
        df.to_sql(trunctablename, engine, if_exists='append'); 
    elif "today_all_tmp" == trunctablename :
        df = ts.get_today_all()
        df.to_sql(trunctablename, engine, if_exists='append')
        tradedate=time.strftime('%Y%m%d',time.localtime(time.time()))
        strsqltmp = ("INSERT INTO stock_daydata ( "
                    "stockcode,pubdate,statdate,roe,closeprice, "
                    "openprice,highprice,lowprice,volume, "
                    "turnoverratio,amount,per, "
                    "pb,mktcap,nmc) "
                    "SELECT a.code,0,todaytradedate,0,a.trade, "
                           "a.open,a.high,a.low,a.volume, "
                           "a.turnoverratio,a.amount,a.per, "
                           "a.pb,a.mktcap,a.nmc "
                      "FROM stock_daydata b "
                      " LEFT JOIN today_all_tmp a "
                              "ON (b.stockcode = a.code "
                                "AND b.statdate = todaytradedate)"
                     "WHERE b.stockcode IS NULL")
        strsql = strsqltmp.replace('todaytradedate',str(tradedate))
        ms.execNotSelectMysql(mcursor,strsql,mconnect)
        del strsql,strsqltmp
        gc.collect()
    elif "stock_basics_tmp" == trunctablename :
        df = ts.get_stock_basics()
        df.to_sql(trunctablename, engine, if_exists='append')
    elif "k_data_tmp_before" == trunctablename :
#         queue = Queue()
#         # Create 8 worker threads
#         # 创建八个工作线程
#         for x in range(4):
#             worker = DownloadBerMarketDataWorker(queue)
#             # Setting daemon to True will let the main thread exit even though the workers are blocking
#             # 将daemon设置为True将会使主线程退出，即使worker都阻塞了
#             worker.daemon = True
#             worker.start()
        # Put the tasks into the queue as a tuple
        # 将任务以tuple的形式放入队列中
        strsql = "SELECT a.stockcode stockcode, b.timetomarket timetomarket, MIN(a.pubdate) mindate\
                FROM stock_basics b, stock_marketdata a\
                WHERE b.stockcode = a.stockcode\
                GROUP BY b.stockcode"
        ms.execSelectMysql(mcursor, strsql)
        res=mcursor.fetchall()
        rows=0;
        for row in res:
            print(row)
            print(rows);
            rows = rows +1;
            print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
            if (row[1] != row[2]) & (row[1] != 0) & (row[2] != 0) :
#                 if 0 == rows%100 :
#                     time.sleep(5)
                stockcode = row[0];
                startdate = str(row[1]);
                startdate = startdate[:4]+"-"+startdate[4:6]+"-"+startdate[6:];
                enddate = str(row[2]);
                enddate = enddate[:4]+"-"+enddate[4:6]+"-"+enddate[6:];
                df = ts.get_k_data(stockcode,start=startdate, end=enddate)
                df.to_sql(trunctablename, engine, if_exists='append');
#            queue.put((engine,trunctablename,stockcode,startdate,enddate))
        # Causes the main thread to wait for the queue to finish processing all the tasks
        # 让主线程等待队列完成所有的任务
#        queue.join()
    ms.closeMysqlCursor(mcursor);
    ms.closeMysqlConnect(mconnect);
    print("get end")
#    getAllNewData('603898')
#    ts.get_k_data('600000')

## 用以下方式可以获得roe


# quanter = 4
# years = [2015]
# yearbase = 2016
# df1 = ts.get_report_data(yearbase,quanter) # get_report_data才能获取到公司公布的roe
# dfbtable=df1[df1['roe']>=15].copy()
# # print(dfbtable)
# write_file('df2016.csv', dfbtable.to_csv(), append=False) #写到文件中
#  
# for year in years:
#     df1 = ts.get_report_data(year,quanter) # get_report_data才能获取到公司公布的roe
#     dfbase=df1[df1['roe']>=15].copy()
#     write_file('df2015.csv', dfbase.to_csv(), append=False) #写到文件中
# #    dfbtable
#     dfbtable = pd.merge(dfbase,dfbtable,on=['code','code'],how='inner')
# #    print(year)
# #    print(dfbtable)
# ##    dfbase=df1[['code','name','roe']].copy() # 可以打印tushares上面获取的所有内容
#     write_file('dfmerger.csv', dfbtable.to_csv(), append=False) #写到文件中
# # print(dfbase)
