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


# def parse_comments(self, response, comments):
#     c = ItemLoader(item=Youtubecomments(), response=response)
#     print('''
#     5555
#     5555
#     5555
#     5555
#     5555
#     5555
#     5555
#     5555
#     ''')
#     c.add_value('videoId', str(search['items'][i]['id']['videoId']))
#     if len(comments['items']) != 0:
#         c.add_value('c_id', str(
#             comments['items'][i]['id']))
#         c.add_value('authorDisplayName', str(
#             comments['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName']))
#         c.add_value('authorChannelUrl', str(
#             comments['items'][i]['snippet']['topLevelComment']['snippet']['authorChannelUrl']))
#         c.add_value('textOriginal', str(
#             comments['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal']))
#         c.add_value('publishedAt', str(
#             comments['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']))
#         c.add_value('updatedAt', str(
#             comments['items'][i]['snippet']['topLevelComment']['snippet']['updatedAt']))
#         c.add_value('likeCount', str(
#             comments['items'][i]['snippet']['topLevelComment']['snippet']['likeCount']))
#         c.add_value('totalReplyCount', str(comments['items'][i]['snippet']['totalReplyCount']))
#
#         for i in range(1, len(comments['items'])):
#             c.replace_value('c_id', str(
#                 comments['items'][i]['id']))
#             c.replace_value('authorDisplayName', str(
#                 comments['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName']))
#             c.replace_value('authorChannelUrl', str(
#                 comments['items'][i]['snippet']['topLevelComment']['snippet']['authorChannelUrl']))
#             c.replace_value('textOriginal', str(
#                 comments['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal']))
#             c.replace_value('publishedAt', str(
#                 comments['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']))
#             c.replace_value('updatedAt', str(
#                 comments['items'][i]['snippet']['topLevelComment']['snippet']['updatedAt']))
#             c.replace_value('likeCount', str(
#                 comments['items'][i]['snippet']['topLevelComment']['snippet']['likeCount']))
#             c.replace_value('totalReplyCount',
#                             str(comments['items'][i]['snippet']['totalReplyCount']))
#             yield c.load_item()

class YtSpider(scrapy.Spider):
    # google account
    # gmail = 'htgytapi2020@gmail.com'
    # password = 'getapi2020'

    name = 'yt'
    start_urls = ['https://www.youtube.com/watch?v=']
    api_keys = ['AIzaSyA25T8oebmmPs5HYeDeiyHbWAOYaG6eM5I',

               # 'AIzaSyDSrbgpw30IFVg9U0dFps_iSzlbS-TTEsg',
               'AIzaSyC1tFmlqlLJyomJrFKxUvXPh3Sad6iCOT0']
    # api_keys = ['AIzaSyA25T8oebmmPs5HYeDeiyHbWAOYaG6eM5I',
    #             'AIzaSyDSrbgpw30IFVg9U0dFps_iSzlbS-TTEsg']

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
        self.flag = 0

    def parse(self, response):
        api = self.api_keys[self.flag]
        # for key in self.api_key:
        yt = build('youtube', 'v3', developerKey=api, cache_discovery=False)
        # ------- Exceptions -------
        # ------- Exceptions -------

        # cities = pd.read_csv('/home/nero/PycharmProjects/Youtube/Youtube/b.csv', header=None)

        for city in self.cities[0]:

            logging.info('++++++++++++++++++ {} ++++++++++++++++++'.format(city))
            # publishedBefore
            # Exceptions
            # if the token in a given api run out the search with raise a error
            try:
                search = yt.search().list(q='{}'.format(city), part='snippet', maxResults=self.max_videos, type='video',
                                          publishedBefore=self.date).execute()

            # ------- api Exceptions in case of runing out of quota -------
            except:
                self.flag += 1
                yt = build('youtube', 'v3', developerKey=self.api_keys[self.flag], cache_discovery=False)

                print('''
                { ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ }
                |
                |
                |
                |
                { ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ }
                ''')
                search = yt.search().list(q='{}'.format(city), part='snippet', maxResults=self.max_videos, type='video',
                                          publishedBefore=self.date).execute()
            # ------- Exceptions -------

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
                    try:  # see if we runing out of quota

                        comments = yt.commentThreads().list(videoId=str(search['items'][i]['id']['videoId']),
                                                            part='snippet', maxResults=self.max_comments).execute()
                        # yield parse_comments(response, comments) # normal comments
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

                    # ------- api Exceptions in case of runing out of quota -------
                    except Exception as e:
                        if 'parameter has disabled comments' in str(e):
                            print('''
                            | parameter has disabled comments |
                            ''')

                            pass
                        elif 'quotaExceeded' in str(e):
                            print("""
                            | quotaExceeded |
                            """)
                            self.flag += 1
                            yt = build('youtube', 'v3', developerKey=self.api_keys[self.flag],
                                       cache_discovery=False)
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
                                        comments['items'][i]['snippet']['topLevelComment']['snippet'][
                                            'authorDisplayName']))
                                    c.replace_value('authorChannelUrl', str(
                                        comments['items'][i]['snippet']['topLevelComment']['snippet'][
                                            'authorChannelUrl']))
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


                except :
                    pass
