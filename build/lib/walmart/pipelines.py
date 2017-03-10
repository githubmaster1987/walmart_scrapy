# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import spiders.config_scrapy as config
import MySQLdb as mdb
import numbers
import random
import json
from datetime import datetime
from items import WalmartItem
import logging
from time import sleep

con = mdb.connect(config.db_host, config.db_user, config.db_pwd, config.db_database)

class WalmartPipeline(object):
    save_item_cnt = 0

    def insert_many(self,sql, sqldata):
        with con:
            cur = con.cursor()
            cur.executemany(sql, sqldata)
            con.commit()

    def update_sql(self,sql, sqldata):
        with con:
            cur = con.cursor()
            cur.execute(sql, sqldata)
            con.commit()

    def process_item(self, item, spider):
        if isinstance(item, WalmartItem):
            self.insert_products(item)
        return item

    def retrieve_data(self,sql):
        with con:
            cur = con.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            return rows

    def delete_product():
        sql  = "DELETE FROM product"
        execute_sql(sql)

        sql  = "DELETE FROM product_picture"
        execute_sql(sql)

        sql  = "DELETE FROM product_options"
        execute_sql(sql)

        sql  = "DELETE FROM product_detail"
        execute_sql(sql)


    def insert_products(self, item):
        table = item["select_table"]
        if table == "upload":
            return

        # product_url = scrapy.Field()
        # product_name = scrapy.Field()
        # unique_id = scrapy.Field()
        # cost = scrapy.Field()
        # delivery_time = scrapy.Field()
        # pictures_url = scrapy.Field()
        # bullet_points = scrapy.Field()
        # details = scrapy.Field()
        # review_numbers = scrapy.Field()
        # category = scrapy.Field()
        # inventory = scrapy.Field()
        # options = scrapy.Field()
        # seller = scrapy.Field()
        # product_type = scrapy.Field()
        
        product_data_list = []

        #Product product Part
        product_data = (
            item["product_id"],        #product_id
            item["product_type"],                             #product_type
            item["delivery_time"],                       #delivery_time
            item["product_name"],              #product_name
            item["product_url"],                            #product_url
            item["review_numbers"],                            #review_numbers
            item["cost"],                            #cost
            item["bullet_points"],                            #bullet_points
            item["seller"],                            #seller
            "",                  #options
            item["category"],                            #category
            item["inventory"],                          #inventory
            item["special_offer"],                      # speciall offer
            str(item["fulfillment"]),            #shipping info
            item["create_time"],                           #scraping date
            str(item["details"]),                                #details
            item["usItemId"],                            #USItemId
            item["unique_id"],                               #SKU
            "")               
        
        product_data_list.append(product_data)
        
        product_sql = """INSERT INTO """ + table + """ (product_id, product_type, delivery_time, product_name, product_url, \
            review_numbers, cost, bullet_points, seller, options, category, inventory, create_date, special_offer, fulfillment, scraping_date, details, unique_id, usItemId, picture_url) VALUES (\
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                Now(),
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s)"""

        picture_sql = """INSERT INTO """ + table + "_pictures " + """ (product_id, picture_url) Values(%s, %s)"""
        picture_data_list = []

        for picture in item["pictures_url"]:
            picture_data = (item["product_id"], picture)
            picture_data_list.append(picture_data)

        
        detail_sql = """INSERT INTO """ + table + "_details " + """ (product_id, detail_content) Values(%s, %s)"""
        detail_data_list = []

        for detail in item["options"]:
            detail_data = (item["product_id"], str(detail))
            detail_data_list.append(detail_data)
        
        if self.save_item_cnt > random.randrange(0, 100):
            logging.info(str(self.save_item_cnt) + " items saved")
            self.save_item_cnt = 0

        try:        
            sleep(0.02)
            self.insert_many(product_sql, product_data_list)
            sleep(0.02)
            self.insert_many(picture_sql, picture_data_list)
            sleep(0.02)
            self.insert_many(detail_sql, detail_data_list)
            self.save_item_cnt += 1

        except mdb.IntegrityError as err:
            #logging.info("Error: {}".format(err))
            return
            #logging.info(item)
        except mdb.OperationalError as err:
            logging.info("Error: {}".format(err))
            # logging.info(detail_data_list)
            # logging.info(picture_data_list)
            # logging.info(product_data_list)
            con = mdb.connect(config.db_host, config.db_user, config.db_pwd, config.db_database)
            return
        except mdb.DataError as err:
            logging.info("Error: {}".format(err))
            return