#!/user/bin/env python
# -*- coding: utf-8 -*-
__author__="YYYY"   

import re
import time
import urllib.request
import urllib.parse

import lxml.html


#下载url
"""
url: 下载链接
user_agent：浏览器的UA信息，又称用户代理
proxy：代理ip
num_retries： 重试次数
timeout： 超时时间
"""
def dowmlpad(url, user_agent='wswp', proxy=None, num_retries=2, timeout=5):
    """
    # 支持500错误重试
    # 设定用户代理 user_agent
    # 支持ip代理
    """
    print('DownloadURL:',url)

    #配置用户代理
    headers = {'User-agent':user_agent}
    request = urllib.request.Request(url, headers=headers)
    #配置
    opener = urllib.request.build_opener()

    #判断是否代理
    if proxy:
        proxy_params = {urllib.parse.urlparse(url).scheme:proxy}
        opener.add_handler(urllib.request.ProxyHandler(proxy_params))
    try:
        html = opener.open(request, timeout=timeout).read()
    except urllib.request.URLError as e:
        print('Download error:',e.reason)
        html = None
        if num_retries > 0:
            if hasattr(e,'code') and 500 <= e.code <600:
                html = dowmlpad(url, user_agent, num_retries-1)
    except Exception as e:
        print('error :',e)
        html = None

    return html

#获得链接
def get_links(html):
    if html:
        webpage_regex = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
        return  webpage_regex.findall(html.decode('utf-8'))
    else:
        return ""

#编写爬取规则，获得数据
def scrape_callback(url,html):
    csslist = ['span[property = "v:itemreviewed"]', 'span.year', 'strong[property="v:average"]']
    try:
        tree = lxml.html.fromstring(html)
        row = [tree.cssselect('{0}'.format(field))[0].text for field in csslist]

        print(url, row)
    except Exception as e:
        print("ScrapeCallback error:",e)

"""
seed_url:种子url
link_regex: 提取链接的正则表达式
max_depath：提取链接的深度，默认为2爬虫到达第二场页面后不再提取链接 ，对于种子页面取出的链接页面，就是第二层，
scrape_callback：回掉函数
"""
def link_crawler(seed_url, link_regex, max_depath=2, scrape_callback=None):
    crawl_queue = [seed_url]  #配置爬取队列，其实就是一个存储url的列表
    #seen = set(crawl_queue)
    seens = {seed_url:1}

    # 循环直到队列为空退出
    while crawl_queue:
        url = crawl_queue.pop()  # 移除队列最后一个元素，并返回值
        html = dowmlpad(url)     # 根据url 下载页面
        depth = seens[url]       # 获得url深度
        print(depth)

        #获取页面中的链接
        for link in get_links(html):
            if depth != max_depath and re.search(link_regex,link):
                link = urllib.parse.urljoin(seed_url, link)     #组装规范链接

                #添加链接到爬取队列中
                if link not in seens:
                    seens[link] = depth+1
                    crawl_queue.append(link)

        #如果处理回调函数存在，则进行回调处理
        if scrape_callback:
            scrape_callback(url, html)


import csv
class ScrapeCallback:
    def __init__(self):
        self.writer = csv.writer(open('countries.csv','w'))
        self.fields = ('name','year','score')
        self.writer.writerow(self.fields)

    def __call__(self, url,html):
        csslist = ['span[property = "v:itemreviewed"]', 'span.year','strong[property="v:average"]']
        try:
            tree = lxml.html.fromstring(html)
            row = [tree.cssselect('{0}'.format(field))[0].text for field in csslist]
            self.writer.writerow(row)
            print(url, row)
        except Exception as e:\
            print("ScrapeCallback error:",e)


if __name__ == '__main__':
    #测试
    send_url = "https://movie.douban.com/"

    link_regex = '(/subject/[\d]+/)' #获取链接的规则

    #使用类的方式来写，下面两个一样结果
    link_crawler(send_url,link_regex,max_depath=2, scrape_callback=ScrapeCallback())
    #link_crawler(send_url, link_regex, max_depath=2, scrape_callback=scrape_callback)
