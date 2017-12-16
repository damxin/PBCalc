#encoding:utf-8
'''
Created on 2017年12月13日

@author: nifx
需要安装的包
python -m pip install cx_Oracle --upgrade
pip install xlrd
pip install xlwt
pip install xlutils
pip list
pip install pymysql
python通过cx_Oracle模块连接Oracle时，一定要注意保证三者（python，cx_Oracle，Oracle客户端）版本

'''

import xlrd
import xlwt
import pymysql as msql
import gc
import cx_Oracle as oracle  # 这边显示红色是正常的


DATABASETYPE = 0
TABLENAME = 1
PRIMARYKEY = 2
TABLEDBTYPE = 3 # 主库，分库，还是唯一库出数据
CONDITIONWHRE = 4

TABLESHEET = "table"
DATABASESHEET = "database"

DBCONNECT = "CONNECT"
DBCURSOR = "CURSOR"

IPADDR = "ipaddr"
DBNAME = "dbname"
DBNAMETYPE = "dbnametype"
DBUSERNAME = "dbusername"
DBPASSWD = "dbpasswd"

dbcmpname1="mysql1"
dbcmpname2="mysql2" # 该条上面记录了要对比的表

dbcntinfo = {}
dbinfo = {}
tableinfo = {}

'''
    作用:获取本地的数据库连接
    {DBCONNECT:sqlcnt,DBCURSOR:sqlcur}
'''
def getMysqlConnect(curdbcntinfo):
    curcntinfo = {}
#     print("getMysqlConnect:(%s,%s,%s,%s)"%(curdbcntinfo[IPADDR],curdbcntinfo[DBUSERNAME],curdbcntinfo[DBPASSWD],curdbcntinfo[DBNAME]))
    try:
        msqlcon=msql.connect(host=curdbcntinfo[IPADDR],
                             user=curdbcntinfo[DBUSERNAME],
                             password=curdbcntinfo[DBPASSWD],
                             db=curdbcntinfo[DBNAME],
                             port=3306,
                             charset='gb2312')
    except Exception as e:
        print(e.message)
    msqlcursor = msqlcon.cursor()
    curcntinfo[DBCONNECT]=msqlcon
    curcntinfo[DBCURSOR]=msqlcursor
    
    return curcntinfo

'''
 {DBCONNECT:sqlcnt,DBCURSOR:sqlcur}
 oracle的IPADDR是net manager上面配置的监听
'''
def getOracleConnect(curdbcntinfo):
    curcntinfo = {}
    cnttmp = curdbcntinfo[DBUSERNAME]+"/"+curdbcntinfo[DBPASSWD]+"@"+curdbcntinfo[IPADDR]
    oraclecon=oracle.connect(cnttmp)
    oraclecur=oraclecon.cursor()
    curcntinfo[DBCONNECT]=oraclecon
    curcntinfo[DBCURSOR]=oraclecur        
    return curcntinfo

def getdbconnect(dbtype, dbcntinfo):
    if dbtype == "mysql" :
        coninfo = getMysqlConnect(dbcntinfo)
    elif dbtype == "oracle" :
        coninfo = getOracleConnect(dbcntinfo)
    return coninfo

'''
    作用:关闭数据库连接，与getMysqlConnect成对使用
'''
def closeDBConnect(curcursor, connection):
    curcursor.close()
    connection.close()

def closedbinfo():
    global dbcntinfo
    
    for dbcmpnametype in dbcntinfo :
        dbcntlist = dbcntinfo[dbcmpnametype]
        for dbcnttmp in dbcntlist :
            closeDBConnect(dbcnttmp[2][DBCURSOR],dbcnttmp[2][DBCONNECT])

'''
    作用:sql语句执行
'''
def execSelectSql(curcursor,strsql):
    curcursor.execute(strsql)
    results=curcursor.fetchall()
    return results

'''
    以主键作为hashkey
 {hashkey1:[(),(),...], hashkey2:[(),(),...]}
'''
def getdbselectresult(selectsql, dbdisttype, cntinfo,primarytblkey):
    tblrltdict= {}
    prykeyhashset = set()
    prykeylen = len(primarytblkey.split(','))
    for cnttmp in cntinfo :
        curdbdisttype = cnttmp[0]
        if dbdisttype != curdbdisttype :
            continue
        
        curcntinfo = cnttmp[2]
        curresultinfo = execSelectSql(curcntinfo[DBCURSOR],selectsql)
#         if curdbtype == "mysql" :
#             curresultinfo = execSelectMysql(curcntinfo[DBCURSOR],selectsql)
#         elif curdbtype == "oracle" :
#             curresultinfo = execSelectOracle(curcntinfo,selectsql)
        ### 根据主键来生成hash的字典
        print("getdbselectresult")
        for rltinfo in curresultinfo :
            print(rltinfo)
            prykeystr = "".join(tuple(rltinfo[:prykeylen]))
            hastprykey = hash(prykeystr)
            prykeyhashset.add(hastprykey)
            if hastprykey in tblrltdict :
                rltinfolist = tblrltdict[hastprykey]
                rltinfolist.append(rltinfo)
            else :
                rltinfolist = []
                rltinfolist.append(rltinfo)
                tblrltdict[hastprykey] = rltinfolist
    return tblrltdict,prykeyhashset        
            

'''
    根据数据库比对类型名称来获取对应的连接
    [[分库主库,数据库类型， 数据库连接],...]
'''
def getdbtmpcntinfo(dbcmpname, dbinfo):
    print("getdbtmpcntinfo:"+dbcmpname)
    dbcnttmplist = []
    dblist = dbinfo[dbcmpname]
    for varlist in dblist[1:] :
        dbtmpcntinfo = getdbconnect(dblist[0],varlist)
        dbcntlist = [varlist[DBNAMETYPE],dblist[0], dbtmpcntinfo]
        dbcnttmplist.append(dbcntlist)
    
    return dbcnttmplist
'''
    dbcntinfo {比对数据库类型名称:[[分库主库,数据库类型(mysql/oracle), 数据库连接],...]}
'''
def getdbcntinfo():
    global dbinfo,tableinfo,dbcntinfo
    global dbcmpname1,dbcmpname2 
    
    dbcntinfotmp = getdbtmpcntinfo(dbcmpname1, dbinfo)
    dbcntinfo[dbcmpname1] = dbcntinfotmp
    dbcntinfotmp = getdbtmpcntinfo(dbcmpname2, dbinfo)
    dbcntinfo[dbcmpname2] = dbcntinfotmp


def getselectsql(tblname,tblvalue):
    ### select 语句的字段
    sltsql = ','.join(tblvalue[4])
    ### from 语句
    fromsql = " from "+tblname
    
    ### where
    wheresql = ""
    if len(tblvalue[3]) != 0 :
        wheresql = " where " + tblvalue[3]
    
    ### order by 
#     ordbysql = "order by " + tblvalue[1]
    
    sltsql = "select " + tblvalue[1] + "," + sltsql + fromsql + wheresql
    print(sltsql)
    return sltsql

'''
把出的值都改为字符串
'''   
def getstrvar(dbtype, varstr):
    chgvarstr = ""
    if varstr[-1:] != ")" :
        if "c_" == varstr[:2] :
            chgvarstr = varstr
        else :
            if dbtype == "mysql" :
                chgvarstr = "concat("+varstr+")"
            elif dbtype == "oracle" :
                chgvarstr = "to_char("+varstr+")"
    else :
        leftkuohaopos = varstr.find('(')
        defaultvalue = varstr[leftkuohaopos+1:-1] # 取默认值
        chgvarstrtmp = varstr[:leftkuohaopos]
        if "c_" == varstr[:2] :
            chgvarstr1 = ("case when vars is null then default"
                         " when vars = '' then default "
                         " when vars = ' ' then default "
                         " else vars end")
        else :
            if dbtype == "mysql" :
                chgvarstr1 = ("case when vars is null then default"
                             " when vars = '' then default "
                             " when vars = ' ' then default "
                             " else concat(vars) end")
            elif dbtype == "oracle" :
                chgvarstr1 = ("case when vars is null then default"
                             " when vars = '' then default "
                             " when vars = ' ' then default "
                             " else to_char(vars) end")

        chgvarstr2 = chgvarstr1.replace("vars", chgvarstrtmp)
        chgvarstr = chgvarstr2.replace("default", defaultvalue)
        
    return chgvarstr

'''
    获取某表的值
'''
def getselectinfo(tblname,tablevalue,cntlist):
    stlsql = getselectsql(tblname,tablevalue)
    (resulttable,prykeyhashkey) = getdbselectresult(stlsql,tablevalue[2],cntlist,tablevalue[1])
    return resulttable,prykeyhashkey
'''
有相同hash的值的数据进行比较
'''
def cmpoldandnewtableinfo(comuseset, oldtblinfo,oldprykey, newtblinfo, newprykey):
    diffset = set()
    oldprykeylen = len(oldprykey.split(','))
    newprykeylen = len(newprykey.split(','))
    for pykhashkey in comuseset :
        oldlist = oldtblinfo[pykhashkey]
        newlist = newtblinfo[pykhashkey]
        ### 暂时认为每个primarykey对应的hash值都是不同的。
        ### 查找主键相同的项，然后去除主键后做比较
        oldtuple = oldlist[0]
        curoldprykey = "".join(tuple(oldtuple[:oldprykeylen]))
        oldtuplestr = "".join(tuple(oldtuple[oldprykeylen:]))
        newtuple = newlist[0]
        curnewprykey = "".join(tuple(newtuple[:newprykeylen]))
        newtuplestr = "".join(tuple(newtuple[newprykeylen:]))
        if oldtuplestr != newtuplestr :
            diffset.add(pykhashkey)
#             diffset.set([pykhashkey,curoldprykey,curnewprykey])
        
#             for oldtuple in oldlist :
#                 curoldprykey = "".join(tuple(oldtuple[:oldprykeylen]))
#                 for newtuple in newlist :
#                     curnewprykey = "".join(tuple(newtuple[:newprykeylen]))
#                     if curoldprykey == curnewprykey :
#                         oldtuplestr = "".join(tuple(oldtuple[oldprykeylen:]))
#                         newtuplestr = "".join(tuple(newtuple[newprykeylen:]))
#                         if oldtuplestr != newtuplestr :
#                             diffset.set([pykhashkey,curoldprykey])
#                         else :
#                             useset.set([pykhashkey,curoldprykey])
    return diffset

'''
比对两张表的条数是否一致
'''
def gettablediff():
    global tableinfo,dbcntinfo
    global dbcmpname1,dbcmpname2
    
    newtabledict = tableinfo[dbcmpname2]
    oldtabledict = tableinfo[dbcmpname1]
    newcntlist = dbcntinfo[dbcmpname2]
    oldcntlist = dbcntinfo[dbcmpname1]
    for tablename in newtabledict :
        oldmoreset = set()
        newmoreset = set()
        useset = set()
        newtablevalue = newtabledict[tablename]
        oldcmptable = newtablevalue[0]
        oldtablevalue = oldtabledict[oldcmptable]
        (newslttableinfo, newprykeyhashset) = getselectinfo(tablename,newtablevalue,newcntlist)
        (oldslttableinfo, oldprykeyhashset) = getselectinfo(oldcmptable,oldtablevalue,oldcntlist)
        useset = newprykeyhashset&oldprykeyhashset ## 交集
        diffset = cmpoldandnewtableinfo(useset,oldslttableinfo,oldtablevalue[1], newslttableinfo,newtablevalue[1])
        oldmoreset = oldprykeyhashset - newprykeyhashset
        newmoreset = newprykeyhashset - oldprykeyhashset
        writetalbeinfo2excel(tablename,dbcmpname1, oldcmptable, oldmoreset, oldslttableinfo)
        writetalbeinfo2excel(tablename,dbcmpname2, tablename, newmoreset, newslttableinfo)
        writedifftalbeinfo2excel(tablename,diffset, dbcmpname1,oldcmptable,oldslttableinfo, dbcmpname2,tablename,newslttableinfo)
        del newslttableinfo,oldslttableinfo
        del oldmoreset,newmoreset,diffset,useset
        gc.collect
    

'''
    获取数据类型信息
 {比对数据库名称(dbcmpname):[数据库类型，[{ip地址:ipaddr,数据库名称:dbname,数据库名称类型(主库/分库):dbnametype,用户名:username,密码:passwd},...]}
 IPADDR = "ipaddr"
 DBNAME = "dbname"
 DBNAMETYPE = "dbnametype"
 DBUSERNAME = "dbusername"
 DBPASSWD = "dbpasswd"
'''
def getdbinfo(dbsheet):
    global dbinfo
    for rownum in range(1, dbsheet.nrows) :
        rowstrings = dbsheet.row_values(rownum)
        dbcmpname = rowstrings[0]
        if len(dbcmpname.strip()) == 0 :
            break
        dbtype = rowstrings[1]
        dbipaddr = rowstrings[2]
        dbname = rowstrings[3]
        dbnametype = int(rowstrings[4])
        dbusername = rowstrings[5]
        dbpasswd = rowstrings[6]
        
        dbdictnew = {IPADDR:dbipaddr,DBNAME:dbname,DBNAMETYPE:dbnametype,DBUSERNAME:dbusername,DBPASSWD:dbpasswd}
        if dbcmpname in dbinfo :
            dblist = dbinfo[dbcmpname]
            dblist.append(dbdictnew)
        else :
            dblist = [dbtype]
            dblist.append(dbdictnew)
            dbinfo[dbcmpname] = dblist
    print("getdbinfo")
    print(dbinfo)   

    
'''
返回的数据格式是
 {数据库比对名称:{表名:[比对表,主键，主库分库,where条件,[字段列表]],...}}
'''
def gettableinfo(tablesheet):
    global tableinfo,dbinfo
    
    for rownum in range(1, tablesheet.nrows) :
        rowstrings = tablesheet.row_values(rownum)
        databasetype=rowstrings[DATABASETYPE]
#         print("The databasetype %s"%(databasetype))
        tablename=rowstrings[TABLENAME]
        if rownum&1 != 0 :
            cmptablename = tablename
#         print("The tablename %s"%(tablename))
        primarykey=rowstrings[PRIMARYKEY]
#         print("The primary key %s"%(primarykey))
        tabledbtype=int(rowstrings[TABLEDBTYPE])
#         print("主分库还是唯一%d"%(tabledbtype))
        conwhere=rowstrings[CONDITIONWHRE]
#         print("The where %s"%(conwhere))
        ### mysql/oracle
        dbtype = dbinfo[databasetype][0]
        varlist = []
        for colnum in range(CONDITIONWHRE+1, tablesheet.ncols) :
            varstr = tablesheet.cell(rownum,colnum).value.strip()
            if len(varstr) == 0 :
                break
            chgvarstr = getstrvar(dbtype,varstr)
            varlist.append(chgvarstr)
        if rownum&1 != 0 :
            tallist = ["",primarykey,tabledbtype,conwhere,varlist]
        else :
            tallist = [cmptablename,primarykey,tabledbtype,conwhere,varlist]
#         print("tallist")
#         print(tallist)
        tavardict = {}
        if databasetype in tableinfo :
#             print("true"+databasetype)
            tavardict = tableinfo[databasetype]
            tavardict[tablename] = tallist
        else :
#             print("false"+databasetype)
            tavardict[tablename] = tallist
            tableinfo[databasetype] = tavardict
    print("dddddtabledict")
    print(tableinfo)

'''
    读取excel里面的数据并进行存储
 
'''
def tablecmp(xlsfilename):
    '''Read tablecmp Excel with xlrd'''
    #file
    TC_workbook=xlrd.open_workbook(xlsfilename)
    #sheet
    all_sheets_list=TC_workbook.sheet_names()
#     print("All sheets name in File:",all_sheets_list)
    for sheetstr in all_sheets_list :
        if TABLESHEET in sheetstr :
            tablesheet = sheetstr
        if DATABASESHEET in sheetstr :
            databasesheet = sheetstr
    dbsheet=TC_workbook.sheet_by_name(databasesheet)
    getdbinfo(dbsheet)
    tablecmp=TC_workbook.sheet_by_name(tablesheet)
    gettableinfo(tablecmp)
    
'''
    以表名为excel的名称，数据库比对类型作为sheet，第一行为主键和要比对的字段
    第二行开始是具体的值
'''
def writetalbeinfo2excel(excelfile, dbcmpname, cmptablename, tablehashset, slttableinfo):
    '''Write content to excelfile.xlsx'''
    global tableinfo
    
    tabledict = tableinfo[dbcmpname]
    tablelist = tabledict[cmptablename]
    primarykey = tablelist[1]
    varlist = tablelist[4]
    new_workbook=xlwt.Workbook(encoding = 'ascii')
    ws= new_workbook.add_sheet(dbcmpname)
    
    ### 写入第一行
    rownum = 0
    primarykeylist = primarykey.split(',')
    primarykeylistlen = len(primarykeylist)
    for prykeycol in range(0,primarykeylistlen) :
        ws.write(rownum,prykeycol, primarykeylist[prykeycol])
    varlistlen = len(varlist)
    for varcol in range(primarykeylistlen,primarykeylistlen+varlistlen) :
        ws.write(rownum,varcol, varlist[varcol-primarykeylistlen])
        
    ### 写入第二行之后的数据,这个是一定要考虑一个hash下面是否对应多个主键了。
    ### {hashkey1:[(),(),...], hashkey2:[(),(),...]}
    tuplelen = primarykeylistlen+varlistlen
    rownum = rownum +1
    for tblhashtmp in tablehashset :
        slttbllist = slttableinfo[tblhashtmp]
        for curtupletmp in slttbllist :
            for varcol in range(0,tuplelen) :
                ws.write(rownum,varcol, curtupletmp[varcol])
            rownum = rownum +1
        
    ### 保存文件
    excelfile = excelfile+"_"+dbcmpname+".xls"
    new_workbook.save(excelfile)
    

'''
    以表名为excel的名称，数据库比对类型作为sheet，第一行为主键和要比对的字段
    第二行开始是具体的值
'''
def writedifftalbeinfo2excel(excelfile,tablediffhashset,olddbcmpname,oldtablename,oldslttableinfo, newdbcmpname,newtablename,newslttableinfo) :
    '''Write content to excelfile.xlsx'''
    global tableinfo
    
    new_workbook=xlwt.Workbook(encoding = 'ascii')
    ws= new_workbook.add_sheet(olddbcmpname+"_"+newdbcmpname)
    
    oldpattern = xlwt.Pattern()
    oldpattern.pattern = xlwt.Pattern.SOLID_PATTERN
    oldpattern.pattern_fore_colour = 2
    oldcell_style = xlwt.XFStyle()
    oldcell_style.pattern = oldpattern
    
    newpattern = xlwt.Pattern()
    newpattern.pattern = xlwt.Pattern.SOLID_PATTERN
    newpattern.pattern_fore_colour = 3
    newcell_style = xlwt.XFStyle()
    newcell_style.pattern = newpattern
    
    newtablelist = tableinfo[newdbcmpname][newtablename]
    primarykey = newtablelist[1]
    varlist = newtablelist[4]
    ### 写入第一行
    rownum = 0
    ws.write(rownum,0, "比对数据库类型")
    primarykeylist = primarykey.split(',')
    primarykeylistlen = len(primarykeylist)
    for prykeycol in range(1,primarykeylistlen+1) :
        ws.write(rownum,prykeycol, primarykeylist[prykeycol-1])
    varlistlen = len(varlist)
    for varcol in range(primarykeylistlen+1,primarykeylistlen+varlistlen+1) :
        ws.write(rownum,varcol, varlist[varcol-primarykeylistlen-1])
        
    ### 写入第二行之后的数据,这个是一定要考虑一个hash下面是否对应多个主键了。
    ### {hashkey1:[(),(),...], hashkey2:[(),(),...]}
    tuplelen = primarykeylistlen+varlistlen
    rownum = rownum +1
    for tblhashtmp in tablediffhashset :
        slttbllist = oldslttableinfo[tblhashtmp]
        for curtupletmp in slttbllist :
            ws.write(rownum,0, olddbcmpname,oldcell_style)
            for varcol in range(1,tuplelen+1) :
                ws.write(rownum,varcol, curtupletmp[varcol-1],oldcell_style)
            rownum = rownum +1
        slttbllist = newslttableinfo[tblhashtmp]
        for curtupletmp in slttbllist :
            ws.write(rownum,0, newdbcmpname,newcell_style)
            for varcol in range(1,tuplelen+1) :
                ws.write(rownum,varcol, curtupletmp[varcol-1],newcell_style)
            rownum = rownum +1
    
    excelfile = excelfile+"_"+olddbcmpname+"_"+newdbcmpname+".xls"
    new_workbook.save(excelfile)


if __name__ == '__main__':
    tablecmp("tablecmp.xlsx")
    getdbcntinfo()
    gettablediff()
    closedbinfo()

    
    
