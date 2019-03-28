#coding: utf-8

from gevent import monkey
monkey.patch_all()
from gevent.pool import Pool
import gevent
import requests
import urllib
import os
import time
import re
import ssl

class Downloader:
    def __init__(self, pool_size, retry=3):
        self.pool = Pool(pool_size)
        self.session = self._get_http_session(pool_size, pool_size, retry)
        self.retry = retry
        self.dir = ''
        self.succed = {}
        self.failed = []
        self.ts_total = 0

    def _get_http_session(self, pool_connections, pool_maxsize, max_retries):
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize, max_retries=max_retries)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            return session

    def run(self, m3u8_url, dir='',moreTs=False):
        self.dir = dir
        if self.dir and not os.path.isdir(self.dir):
            os.makedirs(self.dir)
        r = self.session.get(m3u8_url, timeout=10)
        if r.ok:
            body = r.content
            if body:
                ssl._create_default_https_context = ssl._create_unverified_context
                ts_list = [urllib.parse.urljoin(m3u8_url, n.strip()) for n in str(body, encoding = "utf8").split('\n') if n and not n.startswith("#")]
                if moreTs:
                    ts_list = self.getMoreTsList(ts_list)
                ts_list = list(zip(ts_list, [n for n in range(len(list(ts_list)))]))
                if ts_list:
                    self.ts_total = len(ts_list)
                    print(self.ts_total)
                    g1 = gevent.spawn(self._join_file)
                    self._download(ts_list)
                    g1.join()
        else:
            print( r.status_code)

    def _download(self, ts_list):
        self.pool.map(self._worker, ts_list)
        if self.failed:
            ts_list = self.failed
            self.failed = []
            self._download(ts_list)

    def _worker(self, ts_tuple):
        url = ts_tuple[0]
        index = ts_tuple[1]
        retry = self.retry
        while retry:
            try:
                r = self.session.get(url, timeout=20)
                if r.ok:
                    file_name = url.split('/')[-1].split('?')[0]
                    print( file_name)
                    with open(os.path.join(self.dir, file_name), 'wb') as f:
                        f.write(r.content)
                    self.succed[index] = file_name
                    return
            except:
                retry -= 1
        print ('[FAIL]%s' % url)
        self.failed.append((url, index))

    def _join_file(self):
        index = 0
        outfile = ''
        while index < self.ts_total:
            file_name = self.succed.get(index, '')
            if file_name:
                infile = open(os.path.join(self.dir, file_name), 'rb')
                if not outfile:
                    outfile = open(os.path.join(self.dir, file_name.split('.')[0]+'_all.'+file_name.split('.')[-1]), 'wb')
                outfile.write(infile.read())
                infile.close()
                os.remove(os.path.join(self.dir, file_name))
                index += 1
            else:
                time.sleep(1)
        if outfile:
            outfile.close()
    
    def getMoreTsList(self,ts_list):
        headers = {'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'upgrade-insecure-requests':1,
            'scheme':'https'
            }
        retry = self.retry
        isOk = False
        lastTs = ts_list[-1]
        pattern = re.compile(r'(\d+\.?\d)\.ts')
        tsNum = '{:0>3}'.format(int(pattern.findall(lastTs)[0]) + 1 )
        nextTs = re.sub(pattern,str(tsNum),lastTs,1) + ".ts"
        req = urllib.request.Request(url=nextTs,headers=headers,method='GET')
        l = r = int(tsNum) 
        maxTs = 0
        while retry or isOk:
            try:
                isOk = urllib.request.urlopen(req).status==200
                if isOk:
                    retry = 3
                    l = r + 1   
                    r = l + 100 if maxTs < r else maxTs - int((maxTs-l)/2)
                    nextTs = re.sub(pattern,'{:0>3}'.format(r),lastTs,1) + ".ts"
                    req = urllib.request.Request(url=nextTs,headers=headers,method='GET')
                else:
                    r = r - int((r-l)/2)
            except :
                if int((r-l)/2) == 0:
                    for i in range(int(tsNum) , r): 
                        ts_list.append(re.sub(pattern,'{:0>3}'.format(i),lastTs,1) + ".ts")
                    return ts_list
                maxTs = r
                r = r - int((r-l)/2)
                nextTs = re.sub(pattern,'{:0>3}'.format(r),lastTs,1) + ".ts"
                req = urllib.request.Request(url=nextTs,headers=headers,method='GET')
                retry -= 1
                isOk = False
        return ts_list

if __name__ == '__main__':
    downloader = Downloader(5)
    downloader.run('https://www.xiaodianying.com/filets/2069/dp.m3u8', './video',True)
