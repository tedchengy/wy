# -*- coding: gbk -*-
import sys
import re
import cookielib
import urllib
import urllib2
import time
import datetime
import json
import os
urllib.getproxies_registry = lambda:{}

class Hive():
    def __init__(self,game,dirs, username,apikey):
        self.game = game
        self.dirfile = dirs
        self.dirname = '%s/%s/%s' % (username,username,dirs)
        self.apikey = apikey
        

    def submit_sql(self,sql):
        print sql
        print 'submit_sql'
        self.time = str(datetime.datetime.today())[:13]
        url = 'http://cloud.gameyw.netease.com:8889/hive/submit'
        sql = 'insert overwrite directory "hdfs:///user/%s" ' % self.dirname + sql
        query = (
            ('game',self.game),
            ('db','db_%s' % self.game),
            ('apikey',self.apikey),
            ('sql',sql),
            )
        print url

        try:
            en = urllib.urlencode( query )
            req = urllib2.Request(url)
            html = urllib2.urlopen(req, en,timeout=10).read()
            d = json.loads( html )
            SessionId = d['result']['sessionId']
            success = d['success']
            if success == True:
                print SessionId,'success'
                if SessionId == None:
                    print html
            else:
                print d,'error'
                
            while True:
                time.sleep(15)
                status = self.ret_status(SessionId)
                print 'querying...', status
                if status == 'FINISHED_STATE':break
        except Exception,e:
            print e
            return
        return True        
    
    def ret_status(self,SessionId):
        print 'ret_status',SessionId
        url = 'http://cloud.gameyw.netease.com:8889/hive/status'
        query = (
            ('game',self.game),
            ('db','db_%s' % self.game),
            ('apikey',self.apikey),
            ('sessionId',SessionId),
            )
        en = urllib.urlencode( query )
        req = urllib2.Request(url)
        status = 'status'
        try:
            html = urllib2.urlopen(req, en,timeout=10).read()
            d = json.loads(html)
            status = d['result']['stat']
        except Exception,e:
            print html,e
            status = e
        return status
    
    def download_data(self):
        try:
            os.mkdir(self.dirfile)
        except:
            pass
        
        import glob
        history = glob.glob('.\\%s\\*_0' % self.dirfile )
        print 'history',history
        
        print 'download_data'
        url = 'http://cloud.gameyw.netease.com:18080/cluster/neop/httphdfs/user/%s?op=liststatus&negame=%s&db=db_%s&neapikey=%s&nepermission=hive' %(self.dirname,self.game, self.game, self.apikey)       
        html = urllib2.urlopen(url,timeout=10).read()

        d = json.loads( html )
        file_list = []
        for files in d['FileStatuses']['FileStatus']:
            filename = files['pathSuffix']
            file_list.append(filename)
        file_list.sort()

        for filename in file_list:
            outfile = os.path.join('.',self.dirfile,filename)
            print outfile,
            file_out = open(outfile, 'wb')
            try:
                url = 'http://cloud.gameyw.netease.com:18080/cluster/neop/httphdfs/user/%s/%s?op=OPEN&negame=%s&db=db_%s&neapikey=%s&nepermission=hive' %(self.dirname,filename, self.game, self.game, self.apikey)
                content = urllib.urlopen(url)
                for line in content:
                    file_out.write(line.replace('\x01', '\t'))
                file_out.close()
                print 'download'
            except Exception,e:
                print e
                os.remove(outfile)
                time.sleep( 10 )

stime = int(datetime.datetime.now().strftime("%H")) + (int(datetime.datetime.now().strftime("%H"))/30)
##print stime
if __name__ == '__main__':
    
    #--------------------------------------------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------------------------------------------------------------
    game = 'h45' #产品代号
    dirs = 'n2501' # hdfs路径，请用英文  模块参数以一
    username = 'n2501' #自己的工号      模块参数二
    apikey = 'CE2F4A21E3E39E8EDF7F62F3407AD100C58914D48E8662311721E5CFF04CDA6B3607EFC1B40FF3BC' #http://mgdc.gameyw.netease.com/mcs/main.action里面的APIKEY 模块参数3
    #--------------------------------------------------------------------------------------------------------------------------------------------------------
    
    f= open('sql.txt')#
    sql = f.read()
    f.close()
##    ccc=20170108
##    sql ='''
##select role_id,server,
##fn.json(source,'$.gain_items[0][1]'),
##split(time,':')[0],
##count(fn.json(source,'$.gain_items[0][0]')) 
##from g60.itemp2s 
##where date between %s and %s
##and split(time,':')[0]=='2017-01-08 08'
##and fn.json(source,'$.gain_items.size()') > 0 
##group by split(time,':')[0],server,role_id,fn.json(source,'$.gain_items[0][1]')
##'''%(ccc,ccc)
    H = Hive(game,dirs, username,apikey)
    H.submit_sql( sql )
    H.download_data()
    raw_input('ok')
