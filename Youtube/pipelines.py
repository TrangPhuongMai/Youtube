# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exporters import CsvItemExporter
from Youtube.items import YoutubeItem
import cx_Oracle
import sys



class YoutubePipeline(object):
    def __init__(self):
        self.ORACLE_DB_NAME = 'nero'
        self.USER = 'nero'
        self.PASSWORD = 'nero'
        self.IP = 'localhost'
        self.PORT = '1521'
        self.SERVICE_NAME = 'xe'
        self.TABLE_NAME = ['COMMENTS', 'YT_VIDEO']

    def open_spider(self, spider):
        self.dsn = cx_Oracle.makedsn(self.IP, self.PORT, self.SERVICE_NAME)
        self.db = cx_Oracle.connect(user=self.USER, password=self.PASSWORD, dsn=self.dsn, encoding="UTF-8",
                                    nencoding="UTF-8")

        self.cursor = self.db.cursor()

    def save_to_oracle(self, item):
        print('+++++++++++++++++++ {} +++++++++++++++++++'.format('save_to_oracle'))
        if isinstance(item, YoutubeItem):
            if self.cursor:
                row = (str(item['city'][-1].encode('utf-8', 'replace').decode('utf-8')), str(item['videoId'][-1]),
                       str(item['title'][-1].encode('utf-8', 'replace').decode('utf-8')), str(item['datetime'][-1]),
                       str(item['description'][-1].encode('utf-8', 'replace').decode('utf-8')), int(item['like'][-1]),
                       int(item['dislike'][-1]), str(item['channelId'][-1]))
                print("row = {}".format(row))
                sql = "insert into {} (CITY, VIDEOID, TITLE, " \
                      "DATETIME, DESCRIPTION, VIDEO_LIKE, VIDEO_DISLIKE, CHANNELID)" \
                      " values (:1, :2, :3, :4, :5, :6, :7, :8)".format(self.TABLE_NAME[1], )
                self.cursor.execute(sql, row)
                self.db.commit()
                print("insert success db YoutubeItem")


        else:
            if self.cursor:
                row = (str(item['c_id'][-1]), str(item['videoId'][-1]),
                       str(item['authorDisplayName'][-1].encode('utf-8', 'replace').decode('utf-8')),
                       str(item['authorChannelUrl'][-1]),
                       str(item['textOriginal'][-1].encode('utf-8', 'replace').decode('utf-8')),
                       str(item['publishedAt'][-1]), str(item['updatedAt'][-1]),
                       int(item['likeCount'][-1]), int(item['totalReplyCount'][-1]))
                print("row = {}".format(row))
                sql = "insert into {} (COMMENTS_ID, VIDEOID, AUTHORDISPLAYNAME, " \
                      "AUTHORCHANNELURL, TEXTORIGINAL, PUBLISHEDATE, UPDATEDATE, LIKECOUNT,TOTALREPLYCOUNT)" \
                      " values (:1, :2, :3, :4, :5, :6,:7, :8, :9)".format(self.TABLE_NAME[0], )
                self.cursor.execute(sql, row)
                self.db.commit()
                print("insert success db Youtubecomments")

    def process_item(self, item, spider):
        self.save_to_oracle(item)

    def close_spider(self, spider):
        self.cursor.close()
        self.db.close()

# class YoutubePipeline(object):
#     pass