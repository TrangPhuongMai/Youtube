# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from scrapy.loader import ItemLoader
from Youtube.items import YoutubeItem, Youtubecomments
import pandas as pd
from googleapiclient.discovery import build
import logging
from datetime import datetime, timezone, timedelta
from Youtube import middlewares
import time

logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)


class YtSpider(scrapy.Spider):
    name = 'yt'
    start_urls = ['https://www.youtube.com/watch?v=']
    api_keys = ['AIzaSyA25T8oebmmPs5HYeDeiyHbWAOYaG6eM5I',
               'AIzaSyDSrbgpw30IFVg9U0dFps_iSzlbS-TTEsg',
               'AIzaSyC1tFmlqlLJyomJrFKxUvXPh3Sad6iCOT0']

    cities = pd.read_csv('/home/nero/PycharmProjects/Youtube/Youtube/test.csv', header=None)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if 'date' in kwargs:
            self.date = self.date
        else:
            d1 = datetime.now(timezone.utc).astimezone()
            d2 = timedelta(days=20)
            self.date = d1 - d2
            self.date = self.date.isoformat()

        if 'max_videos' not in kwargs:
            self.max_videos = 20

        if 'max_comments' not in kwargs:
            self.max_comments = 20

    def parse(self, response):

        # for key in self.api_key:
        yt = build('youtube', 'v3', developerKey=self.api_keys[0], cache_discovery=False)

            # cities = pd.read_csv('/home/nero/PycharmProjects/Youtube/Youtube/b.csv', header=None)

        for city in self.cities[0]:

            logging.info('++++++++++++++++++ {} ++++++++++++++++++'.format(city))
            # publishedBefore
            print('JJJJJJJKSKDSJDKSDJKSDJSDDDDDDDDDDDDD')
            time.sleep(2)
            # if the token in a given api run out the search with raise a error
            search = yt.search().list(q='{}'.format(city), part='snippet', maxResults=self.max_videos, type='video',
                                      publishedBefore=self.date).execute()

            for i in range(len(search['items'])):
                logging.info('++++++++++++++++++ {} ++++++++++++++++++'.format(len(search['items'])))

                # get basic snippet data
                l = ItemLoader(item=YoutubeItem(), response=response)
                l.add_value('city', city)
                l.add_value('videoId', str(search['items'][i]['id']['videoId']))
                l.add_value('title', str(search['items'][i]['snippet']['title']))
                l.add_value('datetime', str(search['items'][i]['snippet']['publishedAt']))
                l.add_value('description', str(search['items'][i]['snippet']['description']))
                l.add_value('channelId', str(search['items'][i]['snippet']['channelId']))
                statistic = yt.videos().list(id='{}'.format(search['items'][i]['id']['videoId']),
                                             part='statistics').execute()

                # filter for null value in case the video have no like or dislike
                try:
                    l.add_value('like', statistic['items'][0]['statistics']['likeCount'])
                except:
                    l.add_value('like', '0')
                try:
                    l.add_value('dislike', statistic['items'][0]['statistics']['dislikeCount'])
                except:
                    l.add_value('dislike', '0')
                yield l.load_item()

                try:  # avoid some video that has comments disable
                    comments = yt.commentThreads().list(videoId=str(search['items'][i]['id']['videoId']),
                                                        part='snippet', maxResults=self.max_comments).execute()
                    c = ItemLoader(item=Youtubecomments(), response=response)
                    c.add_value('videoId', str(search['items'][i]['id']['videoId']))
                    if len(comments['items']) != 0:
                        c.add_value('c_id', str(
                            comments['items'][i]['id']))
                        c.add_value('authorDisplayName', str(
                            comments['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName']))
                        c.add_value('authorChannelUrl', str(
                            comments['items'][i]['snippet']['topLevelComment']['snippet']['authorChannelUrl']))
                        c.add_value('textOriginal', str(
                            comments['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal']))
                        c.add_value('publishedAt', str(
                            comments['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']))
                        c.add_value('updatedAt', str(
                            comments['items'][i]['snippet']['topLevelComment']['snippet']['updatedAt']))
                        c.add_value('likeCount', str(
                            comments['items'][i]['snippet']['topLevelComment']['snippet']['likeCount']))
                        c.add_value('totalReplyCount', str(comments['items'][i]['snippet']['totalReplyCount']))

                        for i in range(1, len(comments['items'])):
                            c.replace_value('c_id', str(
                                comments['items'][i]['id']))
                            c.replace_value('authorDisplayName', str(
                                comments['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName']))
                            c.replace_value('authorChannelUrl', str(
                                comments['items'][i]['snippet']['topLevelComment']['snippet']['authorChannelUrl']))
                            c.replace_value('textOriginal', str(
                                comments['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal']))
                            c.replace_value('publishedAt', str(
                                comments['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']))
                            c.replace_value('updatedAt', str(
                                comments['items'][i]['snippet']['topLevelComment']['snippet']['updatedAt']))
                            c.replace_value('likeCount', str(
                                comments['items'][i]['snippet']['topLevelComment']['snippet']['likeCount']))
                            c.replace_value('totalReplyCount',
                                            str(comments['items'][i]['snippet']['totalReplyCount']))
                            yield c.load_item()
                except:
                    pass