# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class YoutubeItem(scrapy.Item):
    city = scrapy.Field()
    videoId = scrapy.Field()
    title = scrapy.Field()
    datetime = scrapy.Field()
    description = scrapy.Field()
    like = scrapy.Field()
    dislike = scrapy.Field()
    channelId = scrapy.Field()

class Youtubecomments(scrapy.Item):
    videoId = scrapy.Field()
    authorDisplayName = scrapy.Field()
    authorChannelUrl = scrapy.Field()
    textOriginal = scrapy.Field()
    publishedAt = scrapy.Field()
    updatedAt = scrapy.Field()
    likeCount = scrapy.Field()
    totalReplyCount = scrapy.Field()