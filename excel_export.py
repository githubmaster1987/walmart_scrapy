import MySQLdb as mdb
import numbers
import json
import walmart.spiders.config_scrapy as config
import csv
import sys
import random
import logging
from time import sleep
#con = mdb.connect('91.208.99.2', 'guidetom3_scrapy', 'Upwork1!', 'guidetom3_db')
#con = mdb.connect(host='91.208.99.2', user='guidetom3_scrapy', passwd='Upwork1!', db='guidetom3_db', port=1144)
#con = mdb.connect(host=config.db_host, user=config.db_user, passwd=config.db_pwd, db=config.db_database)
con = mdb.connect(host='localhost', user="root", passwd="root", db=config.db_database)

def retrieve_data(sql):
    with con:
        cur = con.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return rows

def update_db_data(sql, sqldata):
    with con:
        cur = con.cursor()
        cur.execute(sql, sqldata)
        con.commit()

def insert_many(sql, sqldata):
    with con:
        cur = con.cursor()
        cur.executemany(sql, sqldata)
        con.commit()

def update_db():
    tables = []
    for key in config.match_tables.keys():
        tables.append(config.match_tables[key])

    for table in tables:
        sql = "Select product_id, picture_url, options FROM " + table

        rows =  retrieve_data(sql)
        
        i = 0
        for row in rows:
            picture_sql = """INSERT INTO """ + table + "_pictures " + """ (product_id, picture_url) Values(%s, %s)"""
            pictures = row[1].replace("[", "").replace("]","").split(",")
            picture_data_list = []

            for picture  in pictures:
                if picture != "":
                    picture = picture.replace("'", "")
                    if picture != "":
                        picture_data = (row[0], picture)
                        picture_data_list.append(picture_data)
            
            insert_many(picture_sql, picture_data_list)

            detail_sql = """INSERT INTO """ + table + "_details " + """ (product_id, detail_content) Values(%s, %s)"""
            detail_data_list = []

            details = row[2].replace("[", "").replace("]","").split("}")

            for detail in details:
                if detail != "":
                    detail = detail.replace(", {", "{") + "}"
                    if detail != "":
                        detail_data = (row[0], detail)
                        detail_data_list.append(detail_data)
            
            insert_many(detail_sql, detail_data_list)

            update_sql = "UPDATE " + table + " SET picture_url=%s, options=%s WHERE product_id=%s"
            update_db_data(update_sql, ('', '',row[0]))
            print table, "-> Total :", len(rows), ", Progress :" , i, ", ID:", row[0]
            i += 1

def export_db():
    table_name = ""
    save_cnt = 0
    with open("db.csv") as csvfile:
        reader = csv.reader(csvfile)
        print ( "-----------------CSV Read------------------" )
        i = 0

        item_list = []
        for item in reader:
            if i > 0:
                table_name = item[16]
                item_list.append(item)
            i += 1
        
        sql = "SELECT product_id FROM " + table_name
        print sql
        print "Total:", len(item_list)
        product_list =  retrieve_data(sql)
        
        real_list = []
        for item in item_list:
            exists = 0

            for product in product_list:
                if product[0] == item[11]:
                    exists = 1
            
            if exists == 0:
                real_list.append(item)
        
        for item in real_list:
            product_data_list = []

            product_data = (
                item[11],        #product_id
                item[13],                             #product_type
                item[5],                       #delivery_time
                item[12],              #product_name
                item[14],                            #product_url
                item[15],                            #review_numbers
                item[3],                            #cost
                item[1],                            #bullet_points
                item[17],                            #seller
                "",                                 #options
                item[2],                            #category
                item[8],                          #inventory
                item[18],                      # speciall offer
                str(item[7]),            #shipping info
                item[4],                           #scraping date
                str(item[6]),                                #details
                item[20],                            #USItemId
                item[19],                           #SKU
                "")                            
            
            product_data_list.append(product_data)
            
            product_sql = """INSERT INTO """ + table_name + """ (product_id, product_type, delivery_time, product_name, product_url, \
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
            
            picture_sql = """INSERT INTO """ + table_name + "_pictures " + """ (product_id, picture_url) Values(%s, %s)"""
            picture_data_list = []

            for picture in item[10].split(","):
                picture_data = (item[11], picture)
                picture_data_list.append(picture_data)

            
            detail_sql = """INSERT INTO """ + table_name + "_details " + """ (product_id, detail_content) Values(%s, %s)"""
            detail_data_list = []

            for detail in item[9].split(","):
                
                detail_data = (item[11], detail)
                detail_data_list.append(detail_data)
            
            try:        
                sleep(0.02)
                insert_many(product_sql, product_data_list)
                sleep(0.02)
                insert_many(picture_sql, picture_data_list)
                sleep(0.02)
                insert_many(detail_sql, detail_data_list)

            except mdb.IntegrityError as err:
                # print product_data_list
                print("Error: {}".format(err))
                continue
            except mdb.OperationalError as err:
                # print picture_data_list
                print("Error: {}".format(err))
                con = mdb.connect(config.db_host, config.db_user, config.db_pwd, config.db_database)
                continue
            except mdb.DataError as err:
                print("Error: {}".format(err))
                return

            save_cnt += 1
            if (save_cnt % 10) > random.randrange(0,10):
                print "Unsaved :", len(real_list), " -----  ", save_cnt, "items saved"

def export_excel():
    with open("product.csv", 'w') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([
                                "Unique Identifier (SKU)", 
                                "Title", 
                                "Cost",
                                "Product Type",
                                "Delivery time",
                                "Pictures",
                                "Bullet points",
                                "Details",
                                "Options",
                                "URL",
                                "Reviews",
                                "Seller",
                                "Category",
                                "Inventory",
                                "Special Offer",
                                "Fulfillment",
                                "Date",
                            ])

        product_list =  retrieve_data("""SELECT 
                        p.product_id, 
                        p.product_type, 
                        p.delivery_time, 
                        p.product_name, 
                        p.product_url, 
                        p.review_numbers, 
                        p.cost, 
                        p.bullet_points, 
                        p.seller, 
                        p.options, 
                        p.category, 
                        p.inventory, 
                        p.create_date, 
                        p.special_offer, 
                        p.fulfillment, 
                        p.scraping_date,
                        p.details,
                        p.picture_url
                    FROM product_health p""")
        
        for i in range(0, len(product_list)):
        #for i in range(0, 1):        
            product_item = product_list[i]
            # if product_item[1] != "REGULAR":
            # if product_item[1] != "VARIANT":
            # if product_item[1] != "PACKS OF":
            #     continue
                
            csv_item = {}
            
            csv_item['product_id']= product_item[0]
            csv_item['product_type']= product_item[1]
            csv_item['delivery_time']= product_item[2]
            csv_item['product_name']= product_item[3]
            csv_item['product_url']= product_item[4]
            csv_item['review_numbers']= product_item[5]
            csv_item['cost']= product_item[6]
            csv_item['bullet_points']= product_item[7]
            csv_item['seller']= product_item[8]
            csv_item['options']= product_item[9]
            csv_item['category']= product_item[10]
            csv_item['inventory']= product_item[11]
            csv_item['create_date']= product_item[12]
            csv_item['special_offer']= product_item[13]
            csv_item['fulfillment']= product_item[14]
            csv_item['scraping_date']= product_item[15]
            csv_item['details']= product_item[16]
            csv_item['picture_url']= product_item[17]
            
            csv_writer.writerow([
                csv_item['product_id'],
                csv_item['product_name'],
                csv_item['cost'],
                csv_item['product_type'],
                csv_item['delivery_time'],
                csv_item['picture_url'],
                csv_item['bullet_points'],
                csv_item['details'],
                csv_item['options'],
                csv_item['product_url'],
                csv_item['review_numbers'],
                csv_item['seller'],
                csv_item['category'],
                csv_item['inventory'],
                csv_item['special_offer'],
                csv_item['fulfillment'],
                csv_item['scraping_date']
            ])

            print i

if __name__ == '__main__':
    #export_excel()
    export_db()
    #update_db()