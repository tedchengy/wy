# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#encoding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import datetime
import time
import json



sdtb = '2015-02-12'
sdte = '2015-02-14'

def get_log_fname(logtype, dt):
    return "/home/mg_dc/lzo_logs/g4/logs/%s/date=%s/%s.log.lzo" % (logtype, dt.strftime('%Y%m%d'), dt.strftime('%Y%m%d'))

def get_dt_arr(dtb, dte):
    if dte < dtb:
        return []

    dt = dtb
    arr = []
    while dt <= dte:
        arr.append(dt)
        dt += datetime.timedelta(days=1)

    return arr


def get_log_rdd(sc, logtype, dtb, dte):
    #获取开始日期~结束日期之间的每一天
    dt_arr = get_dt_arr(dtb, dte)

    #获取每一天的LOG文件名
    path_arr = [get_log_fname(logtype, x) for x in dt_arr]
    if not path_arr:
        print >> sys.stderr, 'no path_arr'
        return

    #获取一天LOG的数据对象
    d = sc.newAPIHadoopFile(path_arr[0], 'com.hadoop.mapreduce.LzoTextInputFormat', 'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.LongWritable')
    
    for path in path_arr[1:]:
        d2 = sc.newAPIHadoopFile(path, 'com.hadoop.mapreduce.LzoTextInputFormat', 'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.LongWritable')
        #注意这里的union，把多个对象并接起来，形成一个并接了内容的一个对象
        d = d.union(d2)

    return d


def parse_log(line, logtype, field_arr):

    pat = '[%s],' % (logtype)

    arr = line.split(pat)
    if not arr:
        return
    d = {}
    try:
        #解析JSON
        obj = json.loads(arr[1])
        
        #只获取需要的字段
        for field in field_arr:
            d[field] = obj[field]
    except:
        return
    return d

def parse_fields(line, field_arr):
    d = {}
    for field in field_arr:
        pat = '''"%s":''' % (field)

        i1 = line.find(pat)

        if i1 == -1:
            continue

        istart = i1 + len(pat)
        iend = line.find(',', istart)
        if iend == -1:
            iend = line.find('}', istart)

        value = line[istart: iend].strip('''" ''')
        d[field] = value

    return d

def submain(sc):
    #开始、结束日期
    dtb = datetime.datetime.strptime(sdtb, '%Y-%m-%d')
    dte = datetime.datetime.strptime(sdte, '%Y-%m-%d')

    #获取日期范围内、该LOG类型的文件对象
    crdd = get_log_rdd(sc, 'createrole', dtb, dte) 

    cfield = ['role_id', 'create_time', 'server']
    
    #Hadoop RDD的一个元素是(行号， 行内容)，将纯文本LOG的一行，转换为Python Dict对象
    r = crdd.map(lambda (_, x): parse_log(x, 'CreateRole', cfield))
    
    #过滤掉空的Dict, filter接受一个函数参数，这个函数的参数是对象元素，返回值是True/False
    r = r.filter(lambda x: not not x)
    
    #将Dict转换成( (server, role_id), create_time )的tuple结构
    r = r.map(lambda x: ((x['server'], x['role_id']), x['create_time']))
    
    #按key (server, role_id) 聚合，获取最小的创建时间
    r = r.reduceByKey(lambda x, y: min(int(x), int(y))) 

    lrdd = get_log_rdd(sc, 'loginrole', dtb, dte)

    lfield = ['role_id', 'role_level', 'server']
    r2 = lrdd.map(lambda (_, x): parse_log(x, 'LoginRole', lfield)) \
        .filter(lambda x: not not x) \
        .map(lambda x: ((x['server'], x['role_id']), x['role_level'])) \
        .reduceByKey(lambda x, y: max(int(x), int(y)))

    jr = r2.leftOuterJoin(r)

    print jr.take(10)
    
def main():

    from pyspark import SparkContext, SparkConf
    conf = SparkConf()

    sc = SparkContext(appName="test_g4_join", conf=conf)
    try:
        submain(sc)
    finally:
        sc.stop()
        
if __name__ == '__main__':
    main()