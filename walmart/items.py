# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WalmartItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    product_url = scrapy.Field()
    product_name = scrapy.Field()
    unique_id = scrapy.Field()
    cost = scrapy.Field()
    delivery_time = scrapy.Field()
    pictures_url = scrapy.Field()
    bullet_points = scrapy.Field()
    details = scrapy.Field()
    review_numbers = scrapy.Field()
    category = scrapy.Field()
    inventory = scrapy.Field()
    options = scrapy.Field()
    seller = scrapy.Field()
    product_type = scrapy.Field()
    create_time = scrapy.Field()
    special_offer = scrapy.Field()
    fulfillment = scrapy.Field()
    url = scrapy.Field()
    select_table = scrapy.Field()

    product_id = scrapy.Field()
    usItemId = scrapy.Field()
    note = scrapy.Field()