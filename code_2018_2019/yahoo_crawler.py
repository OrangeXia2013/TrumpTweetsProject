# -*- coding: utf-8 -*-
"""
Created on Tue Aug 28 12:23:34 2018

@author: admin

This is the yahoo_crawler module.
You can download any stock/stock index data which is avalable on yahoo finance
as long as you know the stokc code display on yahoo.
"""

import numpy as np
import pandas as pd
import requests
import time
import re

import urllib
import json

class yahoo_crawler():
    
    def __init__(self,):     
        self.url='https://query1.finance.yahoo.com/v7/finance/download/'
        self.url2='https://sg.finance.yahoo.com/quote/%5EGSPC/history?p=%5EGSPC'
        print('yahooo_crawler')
    
    def my_request(self,symbol='',params={},request_type='get',):
        '''
        Here is an example of the original url and an example of parameters,
        you can see the working procedure of this crawler.
        
        url='https://query1.finance.yahoo.com/v7/finance/download/C31.SI?period1=1532792959&period2=1535471359&interval=1d&events=history&crumb=O4Ek1iFMGcZ'
        params={'period1':1532792959,
                'period2':1535471359,
                'interval':1,
                'events':'history',
                'crumb':'O4Ek1iFMGcZ'}
        '''
        url=self.url+symbol+'?'+urllib.parse.urlencode(params)
        for i in range(200):
            try:
                if request_type=='get':
                    headers = {
                            "Content-type": "application/x-www-form-urlencoded",
                            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
                            }
                    resp=requests.get(url
                                      ,headers=headers
                                      ,timeout=15
                                      ,allow_redirects = True)
                elif request_type=='post':
                    headers = {
                            "Content-type" : "application/x-www-form-urlencoded",
                            }
                    resp=requests.post(url
                                       ,data=json.dumps(params)
                                       ,headers=headers
                                       ,timeout=15,
                                       allow_redirects = True)
         
                print('Connected! It\'s the '+str(i+1)+'/200 try...')
                return resp
                break
            except:
                print('ConnectTimeout! Waiting for '+str(i+2)+'/200 try...')
                print('sleeping...')
                time.sleep(4+np.random.random([1,1]))
                print('working')
            if i==199:
                print('Having failied for 200 times...')
            
    def GetStockPrice(self,symbol=" ",params="",request_type='get'):
        '''
        symbol="YHOO"
        params={'period1':'2017-01-02',
                'period2':'2018-01-02',
                'interval':'1',
                'events':'history',
                'crumb':'O4Ek1iFMGcZ'}
        '''
        params['period1']=int(time.mktime(pd.to_datetime(params['period1']).timetuple()))
        params['period2']=int(time.mktime(pd.to_datetime(params['period2']).timetuple()))
        
        resp=self.my_request(symbol,params,request_type='post')
        String=resp.content.decode('utf-8')
        String=String.replace('null','np.nan')
        
        if 'Bad Request' in String:
            print('Parameter Error')
            return String
        else:
            pass
        List=String.split('\n')
        List.remove(List[-1])
        List2=[value.split(',') for value in List]
        df=pd.DataFrame(List2[1:],columns=List2[0])
        df['Date']=pd.to_datetime(df['Date'])
        def my_eval(df):
            df=df.apply(eval)
            return df
        df.iloc[:,1:]=df.iloc[:,1:].apply(lambda x:my_eval(x),axis=1)
        return df
    
    def get_yahoo_crumb_cookie(self):
        """Get Yahoo crumb cookie value."""
        res = requests.get('https://finance.yahoo.com/quote/SPY/history')
        yahoo_cookie = res.cookies['B']
        yahoo_crumb = None
        pattern = re.compile('.*"CrumbStore":\{"crumb":"(?P<crumb>[^"]+)"\}')
        for line in res.text.splitlines():
            m = pattern.match(line)
            if m is not None:
                yahoo_crumb = m.groupdict()['crumb']
        return yahoo_cookie, yahoo_crumb
    
    def share(self, symbol = '', params = '',from_date = '', to_date = ''):
        cookie, crumb = self.get_yahoo_crumb_cookie()
        default_symbol = "^GSPC"
        default_params = {'period1': '2008-12-01',
                          'period2': '2018-12-01',
                          'interval': '1d',
                          'events': 'history',
                          'crumb': crumb}
        # default sample
        # S&P500 is provided by the default sample
        if len(symbol)==0:
            symbol = default_symbol
        else:
            assert type(symbol) is str, '*symbol* must be string type!'
        # period is from 2008-12-01 to 2018-12-01
        # frequency is daily
        if len(params) == 0:
            params = default_params
        else:
            assert type(params) is dict, '*params* must be dict type!'
            assert len(params.keys()) == 5, 'Expect 5 key-value pairs in params, but got %s!'%()
            assert all([k in default_params.keys() for k in params.keys()]), '*params* contains invalid key(s)!'
        # can simply modify the date
        if len(from_date) != 0:
            params['period1'] = from_date
        if len(to_date) != 0:
            params['period2'] = to_date
        df=self.GetStockPrice(symbol=symbol,params=params,request_type='post')
        return df
if __name__=='__main__':
    '''
    
    self=yahoo_crawler()
    t=time.time()
    cookie,crumb=self.get_yahoo_crumb_cookie()
    symbol="YHOO"#change stock id here
    params={'period1':'2016-08-17',
            'period2':'2016-08-30',
            'interval':'1d',
            'events':'history',
            'crumb':crumb}
    df=self.GetStockPrice(symbol=symbol,params=params,request_type='post')
    t2=time.time()-t
    print(str(round(t2,4))+' seconds elapsed...')
    
    '''