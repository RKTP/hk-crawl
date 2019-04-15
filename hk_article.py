import scrapy
import json
import logging
import os, sys
import pickle as pkl

from os import path


class HkaSpider(scrapy.Spider):
    name = "hka"
    data_dir = os.path.join('/home/rktp/capstone/hkcrawl/data') 

    def start_requests(self):
        metapaths = os.listdir(path.join(self.data_dir, 'meta'))
        metafiles = []
        for p in metapaths:
            with open(os.path.join(self.data_dir, 'meta', p), 'rb') as f:
                metafiles.append(pkl.load(f))

        #urls = [ x['url'] for x in metafiles ]

        #for url in urls:
        for m in metafiles:
            yield scrapy.Request(url=m['url'], callback=self.parse, meta={'id': m['id']})

    def parse(self, response):
        p_body = response.xpath('/html//p/text()').extract()
        article_body = response.xpath('/html//article/text()').extract()

        res = ''.join(p_body+article_body).replace('   ', '').replace('\r', '').replace('\n', '').replace('\t', '')
        #logging.info(len(res))

        if len(res) > 5000 and not path.exists(path.join(self.data_dir,'article' ,str(response.meta.get('id'))+'.pkl')):
            with open(os.path.join(self.data_dir, 'article', str(response.meta.get('id'))+'.pkl'), 'wb') as f:
                pkl.dump(res, f)
