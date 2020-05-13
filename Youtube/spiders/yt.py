# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from scrapy.loader import ItemLoader
from Youtube.items import YoutubeItem,Youtubecomments
import pandas as pd
from googleapiclient.discovery import build
import logging
import time

logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)

class YtSpider(scrapy.Spider):
    name = 'yt'
    start_urls = ['https://www.youtube.com/watch?v=']
    api_key = ['AIzaSyBzQNqpBOWU-LPLueKelT4Iw8aA4RF2yNY']
    cities = pd.read_csv('/home/nero/PycharmProjects/Youtube/Youtube/test.csv')
    max_comments = 20
    max_videos = 1

    def parse(self, response):

        for key in self.api_key:
            yt = build('youtube', 'v3', developerKey=key,cache_discovery=False)
            cities = pd.read_csv('/home/nero/PycharmProjects/Youtube/Youtube/b.csv', header=None)

            for city in cities[0]:

                logging.info('++++++++++++++++++ {} ++++++++++++++++++'.format(city))
                search = yt.search().list(q='{}'.format(city),part='snippet',maxResults=self.max_videos,type='video').execute()
                for i in range(len(search['items'])):
                    # get basic snippet data
                    l = ItemLoader(item=YoutubeItem(), response=response)
                    l.add_value('city',city)
                    l.add_value('videoId',str(search['items'][i]['id']['videoId']))
                    l.add_value('title',str(search['items'][i]['snippet']['title']))
                    l.add_value('datetime',str(search['items'][i]['snippet']['publishedAt']))
                    l.add_value('description',str(search['items'][i]['snippet']['description']))
                    l.add_value('channelId',str(search['items'][i]['snippet']['channelId']))
                    statistic = yt.videos().list(id='{}'.format(search['items'][i]['id']['videoId']),part='statistics').execute()

                    try:
                        l.add_value('like',statistic['items'][0]['statistics']['likeCount'])
                    except :
                        l.add_value('like','0')
                    try:
                        l.add_value('dislike',statistic['items'][0]['statistics']['dislikeCount'])
                    except :
                        l.add_value('dislike','0')
                    yield l.load_item()

                    comments = yt.commentThreads().list(videoId=str(search['items'][i]['id']['videoId']),part='snippet',maxResults=self.max_comments).execute()
                    c = ItemLoader(item=Youtubecomments(),response=response)
                    c.add_value('videoId',str(search['items'][i]['id']['videoId']))
                    for i in range(len(comments['items']))
                        c.add_value('authorDisplayName',str(comments['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName']))
                        c.add_value('authorChannelUrl',str(comments['items'][i]['snippet']['topLevelComment']['snippet']['authorChannelUrl']))
                        c.add_value('publishedAt',str(comments['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']))
                        c.add_value('updatedAt',str(comments['items'][i]['snippet']['topLevelComment']['snippet']['updatedAt']))
                        c.add_value('likeCount',str(comments['items'][i]['snippet']['topLevelComment']['snippet']['likeCount']))
                        c.add_value('totalReplyCount',str(comments['items'][i]['snippet']['topLevelComment']['snippet']['totalReplyCount']))








