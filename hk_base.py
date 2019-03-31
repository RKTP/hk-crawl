import scrapy
import json
import logging
import os, sys
import pickle as pkl

from os import path
#import http

#from http import HTTPStatus


class HkSpider(scrapy.Spider):
    name = "hkc"
    base_url = 'https://hacker-news.firebaseio.com/v0/{}.json?print=pretty'
    data_dir = os.path.join('/home/rktp/capstone/hkcrawl/data') 

    def start_requests(self):
        urls = [
            self.base_url.format('newstories'),
            self.base_url.format('topstories'),
            self.base_url.format('beststories')
        ]

        for url in urls:
            #logging.info('Sending to:{}'.format(url))
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        id_list = json.loads(response.body)
        #logging.info('Article Count: {}'.format(len(id_list)))
        #logging.info(id_list)
        for aid in id_list:
            url = self.base_url.format('item/{}'.format(aid))
            yield scrapy.Request(url=url, callback=self.parseArticle)

    def parseArticle(self, response):
        article_info = json.loads(response.body)
        #logging.info(article_info)
        #logging.info(type(article_info))
        if article_info['type'] != 'story' or 'Ask HN' in article_info['title'] or 'Show HN' in article_info['title']:
            pass #logging.info(article_info['title'])
        elif 'url' not in list(article_info.keys()):
            logging.info('URL not exist: '+str(article_info['id']))
            return
        elif path.exists(os.path.join(self.data_dir,'meta' ,str(article_info['id'])+'.pkl')):
            logging.info('Duplicate article info: '+str(article_info['id']))
            return
        else:
            with open(os.path.join(self.data_dir,'meta' ,str(article_info['id'])+'.pkl'),'wb') as f:
                target = {'id': article_info['id'], 'score': article_info['score'], 'time': article_info['time'], 'title': article_info['title'], 'url': article_info['url']}
                pkl.dump(target, f)
