'''
Created on 2017年7月9日

@author: Administrator
'''
import  pymysql as msql
#import  pymysql.cursors

'''
    作用:获取本地的数据库连接
'''
def getMysqlConnect():
    try:
        msqlcon=msql.connect(host='localhost',
                             user='root',
                             password='root',
                             db='finance',
                             port=3306,
                             charset='utf8')
    except Exception as e:
        print(e.message);        
    return msqlcon

'''
    作用:关闭数据库连接，与getMysqlConnect成对使用
'''
def closeMysqlConnect(connection):
    connection.close()

def closeMysqlCursor(msqlcursor):
    msqlcursor.close()
    
'''
        获取游标
'''
def getMsqlCursor(msqlcon):
    msqlcursor = msqlcon.cursor()
    return msqlcursor

'''
    作用:sql语句执行
'''
def execNotSelectMysql(msqlcursor,strsql,msqlcon):
    try:
        msqlcursor.execute(strsql)
    except Exception as e:
        msqlcon.rollback()  # 事务回滚
        print('事务处理失败', e)
    else:
        msqlcon.commit()  # 事务提交
        print('事务处理成功', msqlcursor.rowcount)

def truncateTableData(tablename,mcursor,mconnect):
    strsql = "truncate table "+ tablename;
    execNotSelectMysql(mcursor, strsql, mconnect)

'''
    作用:sql语句执行
'''
def execSelectMysql(msqlcursor,strsql):
    msqlcursor.execute(strsql)
    
'''
    作用:sql语句执行
'''
def execInsertMysql(msqlcursor,tablename,dframe):
    strsql = "insert into " + tablename+ "";
    
    msqlcursor.execute(strsql)

def getsqlFromTsql(msqlcursor, tablename):
    strsql = "select a.sqlcommand from tsql a where a.tablename = " + tablename + " order by a.l_order";
    msqlcursor.execute(strsql);
    sqlcmd = msqlcursor.fetchall();
#   循环获取
#    for ()
#   替换
#   执行替换后的语句，这个语句肯定不能是select的语句
    