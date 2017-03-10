# -*- coding: utf-8 -*-
import scrapy
import proxylist
import logging
import useragent
from scrapy.http import Request, FormRequest
import time, re, random, base64
import logging
from walmart.items import WalmartItem
from time import sleep
import csv
import os
import json
import os.path
from io import StringIO
import config_scrapy as config
from datetime import datetime
from datetime import date
import mysql_manage as db

def calc_min_max(min_value):
    max_value = 0

    max_value = min_value + 50

    if min_value < 100:
        max_value = min_value + 1
    elif min_value < 500:
        max_value = min_value + 3
    elif min_value < 2000:
        max_value = min_value + 5
    elif min_value < 5000:
        max_value = min_value + 10
    elif min_value < 10000:
        max_value = min_value + 20

    return max_value

class WalmartspiderSpider(scrapy.Spider):
    name = "walmartspider"
    # allowed_domains = ["walmart.com"]

    parent_url = 'https://www.walmart.com/browse'

    proxy_lists = proxylist.proxys
    useragent_lists = useragent.user_agent_list

    category_urls = []
    category_pages = []
    method = ""
    category_param = ""
    category_table = ""

    product_list = []
    item_list = []

    news_item_cnt = 0
    save_item_cnt = 0

    def __init__(self,  method ='', category ='', *args, **kwargs):
        super(WalmartspiderSpider, self).__init__(*args, **kwargs)
        self.method = method
        self.category_param = category

    def set_proxies(self, url, callback):
        req = Request(url=url, callback=callback,dont_filter=True)
        proxy_url = self.proxy_lists[random.randrange(0,len(self.proxy_lists))]

        user_pass=base64.encodestring(b'silicons:1pRnQcg87F').strip().decode('utf-8')
        req.meta['proxy'] = "http://" + proxy_url
        req.headers['Proxy-Authorization'] = 'Basic ' + user_pass

        user_agent = self.useragent_lists[random.randrange(0, len(self.useragent_lists))]
        req.headers['User-Agent'] = user_agent
        return req
    
    # def parse_root_url(self, response):
    #     department = response.meta["department"]

    #     category_divs = response.xpath("//li[@class='department-single-level']/a")
        
    #     with open(config.output_category_csv_name, 'a') as csvfile:
    #         csv_writer = csv.writer(csvfile)

    #         for category in category_divs:
    #             url = category.xpath("@href").extract_first()
    #             name = category.xpath("text()").extract_first()
    #             csv_writer.writerow([department, name, response.urljoin(url)])
            

    def parse_method(self, response):
        #bundle product
        #req = self.set_proxies("https://www.walmart.com/nco/Purely-Inspired-100-Pure-Garcinia-Cambogia-Dietary-Supplement-Tablets-100-count-Value-Bundle-Pack-of-2/52589521", self.parse_item_detail_page)
        #req = self.set_proxies("https://www.walmart.com/ip/STCKNG-COMPRSN-KNEE-SM/42123288", self.parse_item_detail_page)
        #req = self.set_proxies("https://www.walmart.com/ip/Fruit-of-the-Loom-Men-s-Fleece-Crew-Sweatshirt/25033121", self.parse_item_detail_page)
        # req = self.set_proxies("https://www.walmart.com/ip/Clubmaster-Browline-Classic-Retro-Style-Clear-Lens-Glasses-Half-Frame-Black-NEW-Black/543608520", self.parse_item_detail_page)
        # req = self.set_proxies("https://www.walmart.com/ip/Designer-Protein-Lite/51222397", self.parse_item_detail_page)
        # yield req
        # return
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if self.method == "":
            key_exists = 0
            for key in config.match_tables.keys():
                if key == self.category_param:
                    self.category_table = config.match_tables[key]
                    key_exists = 1

            if key_exists == 0:
                logging.info("-------------------Category is invalid.--------------------")
                return
            
            category_list =  db.retrieve_data("Select * From category")
            logging.info ( "-----------------Category Read------------------" )
            logging.info( self.category_param)
            for category in category_list:
                if category[1] == self.category_param:
                    self.category_urls.append(category[3])
                    logging.info( category[3])

            self.product_list =  db.retrieve_data("Select * From " + self.category_table)
            # category_divs = response.xpath("//li[@class='department-single-level']/a")

            # for category in category_divs:
            #     url = category.xpath("@href").extract_first()
            #     name = category.xpath("text()").extract_first()
            #     self.category_urls.append(response.urljoin(url))

            #self.category_urls = []
            #self.category_urls.append('?cat_id=1085666')
            #self.category_urls.append('?cat_id=1085666_1005862_1007221')
            #self.category_urls.append('?cat_id=5438')

            for url in self.category_urls:
                #min_value = random.randint(1, 5000)
                min_value = 1
                max_value = calc_min_max(min_value)
                price_str = "&min_price=" + str(min_value) + "&max_price=" + str(max_value)

                #url = url

                req = self.set_proxies(response.urljoin(url) + "&sort=new&page=1" + price_str , self.parse_category_items)
                req.meta["min_value"] = min_value
                req.meta["url"] = url
                req.meta["current_time"] = current_time
                yield req
        
        elif self.method == "upload":
           with open(config.upload_csv_name) as csvfile:
                reader = csv.reader(csvfile)
                logging.info ( "-----------------CSV Read------------------" )
                logging.info( config.upload_csv_name)

                i = 0
            
                upload_ids = []
                for input_item in reader:
                    if i>0:
                        upload_ids.append(input_item[0])
                        logging.info(input_item[0])
                    i += 1
                
                url = "https://www.walmart.com/search/?query=" + " ".join(upload_ids)
                req = self.set_proxies(url, self.parse_category_items_by_upload)
                req.meta["ids"] = upload_ids
                req.meta["url"] = url
                req.meta["current_time"] = current_time
                yield req
                

        # elif self.method == "csv_category_by_tag":
        #     with open(config.output_category_csv_name, 'w') as csvfile:
        #         csv_writer = csv.writer(csvfile)
        #         csv_writer.writerow(["Department", "Sub Department", "Sub Department URL"])

        #         category_divs = response.xpath("//li[@class='department-single-level']/a")

        #         for category in category_divs:
        #             root_url = category.xpath("@href").extract_first()
        #             root_name = category.xpath("text()").extract_first()

        #             req = self.set_proxies(response.urljoin(root_url), self.parse_root_url)
        #             req.meta["department"] = root_name
        #             yield req
                

        # elif self.method == "csv_category":
        #     with open(config.output_category_csv_name, 'w') as csvfile:
        #         csv_writer = csv.writer(csvfile)
        #         #CSV File Header
        #         csv_writer.writerow(["Department", "Sub Department","Sub Department URL", "Category", "Category URL"])

        #     logging.info.info ("********************************")
        #     logging.info.info ( response.url)

        #     script_lists = response.xpath("//script[contains(text(), 'window.__WML_REDUX_INITIAL_STATE__ = ')]")
        #     for script in script_lists:
        #         content = script.xpath("text()").extract_first()
        #         if content != "":
        #             json_obj = json.loads(content.split("window.__WML_REDUX_INITIAL_STATE__ = ")[1][0:-1])

        #             with open(config.output_category_csv_name, 'a') as csvfile:
        #                 csv_writer = csv.writer(csvfile)

        #                 for key in json_obj["header"]["quimbyData"]["global_header"]["headerZone3"]["configs"].keys():
        #                     if key == "departments":
        #                         departments = json_obj["header"]["quimbyData"]["global_header"]["headerZone3"]["configs"][key]

        #                         for department in departments:
        #                             logging.info.info (  "*****************DEPARTMENT*****************")
        #                             department_name =  department["name"]
        #                             logging.info( department_name)
        #                             csv_writer.writerow([department_name, "","", "", ""])

        #                             sub_departments = department["departments"]
        #                             logging.info( "-----------------SUB DEPARTMENT-----------------")


        #                             for sub_department in sub_departments:
        #                                 sub_department_name = sub_department["department"]["title"]
        #                                 try:
        #                                     sub_department_url = response.urljoin(sub_department["department"]["clickThrough"]["value"])
        #                                 except:
        #                                     logging.info(sub_department["department"])
        #                                     sub_department_url = ""

        #                                 csv_writer.writerow(["", sub_department_name, sub_department_url , "", ""])
        #                                 try :
        #                                     sub_categories = sub_department["categories"]
        #                                     logging.info( "-----------------SUB CATEGORY-----------------")
        #                                     if len(sub_categories) > 0:
        #                                         for sub_category in sub_categories:
        #                                             sub_category_name = sub_category["category"]["title"]
        #                                             sub_category_link = response.urljoin(sub_category["category"]["clickThrough"]["value"])
        #                                             csv_writer.writerow(["", "", "", sub_category_name, sub_category_link])
        #                                 except :
        #                                     error = 0

    def start_requests(self):
        #db.delete_products()
        #return
        
    	req = self.set_proxies(self.parent_url, self.parse_method)
        yield req

    def parse_item_detail_page(self, response):
        # logging.info( "**********Detail************")
        # logging.info( response.url)
        product = response.meta["product"]
       
        #product = WalmartItem()

        #product["product_type"] ="VARIANT" 
        #product["delivery_time"] = ""

        product["pictures_url"] = ""
        product["category"] = ""
        product["options"] = ""
        product["details"] = ""

        size_list = []
        color_list = []
        size_color_item_list = []
        offer_list = []
        sub_item_list = []
        specification_list = []

        script_lists = response.xpath("//script[contains(text(), 'window.__WML_REDUX_INITIAL_STATE__ = ')]")
        for script in script_lists:
            content = script.xpath("text()").extract_first()
            if content != "":
                json_obj = json.loads(content.split("window.__WML_REDUX_INITIAL_STATE__ = ")[1][0:-1])
                #logging.info(json_obj)

                json_prod = json_obj["product"]

                try:
                    product_id =  json_prod["primaryProduct"]
                except:
                    logging.info("*** Primary Product Not Found **** " + response.url)
                    return

                product["product_id"] = product_id
                
                try:
                    product_obj = json_prod["products"][product_id]
                except:
                    logging.info("*** Project Not Found **** " + str(product_id) + "," + response.url)
                    return

                #Select ID to get Production information
                try:
                    product["unique_id"] = product_obj["productAttributes"]["walmartItemNumber"]
                except:
                    product["unique_id"] = ""

                # Quick Scraping ID Exists Check                
                if response.meta["type"] == "upload":
                    ids_exists = 0
                    for upload_id in response.meta["ids"]:
                        if upload_id == product["unique_id"] or upload_id == product["project_id"]:
                            ids_exists = 1
                    
                    if ids_exists == 0:
                        return

                #Product Category Path
                try:
                    product["category"] = product_obj["productAttributes"]["productCategory"]["categoryPath"]
                except:
                    logging.info("*** Category Not Found **** " + response.url)
                    return
                
                image_list = []

                try:
                    for key in json_prod['images'].keys():
                        image_url = json_prod['images'][key]["assetSizeUrls"]["main"].encode('utf-8').strip()
                        image_list.append(image_url)

                    product["pictures_url"] = image_list
                except:
                    error_flag = 1
                    product["pictures_url"] = ""

                try:
                    specifications =  json_prod["idmlMap"][product_id]["modules"]["Specifications"]["specifications"]["values"][0]
                    for specification in specifications:
                        for key in specification.keys():
                            sp_item = {}
                            sp_name = specification[key]["displayName"]
                            sp_value = ",".join(specification[key]["values"])
                            sp_item["value"] = sp_value.encode('utf-8').strip()
                            sp_item["name"] = sp_name.encode('utf-8').strip()
                            specification_list.append(sp_item)

                    product["details"] = specification_list
                except:
                    logging.info("***Specification Does Not Exist **** " + response.url)
                    return

                #There are offers information in JSON product items
                for key in json_prod["offers"].keys():
                    offer_list.append(json_prod["offers"][key])

                #There are products information in JSON product items
                for key in json_prod["products"].keys():
                    sub_item_list.append(json_prod["products"][key])
                
                if product["product_type"] == "VARIANT":
                    option_keys = []
                    
                    #add all variant keys  for ex: size, actual_color
                    try:
                        for key in json_prod["variantCategoriesMap"][product_id].keys():
                            option_keys.append(key)
                    except:
                        logging.info("------Variant Categories Map Error 1 -----" + response.url)
                        return

                    # Variant Information Example

                    # "variantCategoriesMap":
                    # {
                    #     "6GHTU2RLBBXJ":
                    #     {
                    #         "actual_color":
                    #         {
                    #             "id":"actual_color",
                    #             "name":"Actual Color",
                    #             "type":"DROPDOWN",
                    #             "variants":
                    #             {
                    #                 "actual_color-brown":{
                    #                     "id":"actual_color-brown",
                    #                     "name":"Brown",
                    #                     "categoryId":"actual_color",
                    #                     "products":["6GHTU2RLBBXJ"],
                    #                     "availabilityStatus":"AVAILABLE",
                    #                     "rank":10000000
                    #                 },
                    #                 "actual_color-grey":{
                    #                     "id":"actual_color-grey",
                    #                     "name":"Grey",
                    #                     "categoryId":"actual_color",
                    #                     "products":["2YGHPTDOHUGX"],
                    #                     "availabilityStatus":"AVAILABLE",
                    #                     "rank":10000001
                    #                     }
                    #             }
                    #         }
                    #     }
                    # }

                    option_product_list = {}

                    for option_key in option_keys:
                        option_value = json_prod["variantCategoriesMap"][product_id][option_key]["variants"]

                        for key in option_value.keys():
                            option_product_ids = option_value[key]["products"]

                            for item_id in option_product_ids:
                                try:
                                    opt_str = option_product_list[item_id]
                                except:
                                    option_product_list[item_id] = {}

                                try:
                                    opt_str = option_product_list[item_id]["options"]
                                except KeyError:
                                    option_product_list[item_id]["options"] = []

                                option_item = {}
                                key_str = option_key.encode('utf-8').strip()
                                option_item[key_str] = option_value[key]["name"].encode('utf-8').strip()
                                option_product_list[item_id]["options"].append(option_item)
                    
                    #Get Product id to get offer. There is price in offer list

                    #**********************Example Product********************
                    #2V4EPCIFF89H": {"primaryProductId": "6P0QMLAAZVM1",
                    #  "status": "NOT_FETCHED",
                    #  "cellCoverageLookupEnabled": False,
                    #  "wupc": "0880950516820",
                    #  "usItemId": "54882745",
                    #  "offers": ["089B8844902045B29372753202FEC2ED"],
                    #  "productType": "Shorts",
                    #  "variants": {"actual_color": "actual_color-pinksizzle",
                    #  "size": "size-4/5"}
                    # ,
                    #  "isRestrictedCategory": False,
                    #  "productSegment": "Clothing,
                    #  Shoes & Accessories",
                    #  "productId": "2V4EPCIFF89H"}

                    try:
                        for sub_item in sub_item_list:
                            for key in option_product_list.keys():
                                if key == sub_item["productId"]:
                                    if len(sub_item["offers"]) > 1:
                                        logging.info("Offer Size is more than 1 **** " + response.url)
                                        #logging.info(sub_item)

                                    option_product_list[key]["offer_id"] = sub_item["offers"][0]
                    except:
                        logging.info("--------- Offer Index Error --------" + response.url)
                        return                    

                    #Get Offer id to get Price

                    #**********************Example Offer********************
                    # {"089B8844902045B29372753202FEC2ED": {"status": "NOT_FETCHED",
                    #     "containsHolidayMessaging": False,
                    #     "pickupTodayEligible": False,
                    #     "quantity": 1,
                    #     "isBelowShippingThreshold": False,
                    #     "offerInfo": {"offerType": "ONLINE_AND_STORE",
                    #     "shippingPassEligible": False,
                    #     "offerId": "089B8844902045B29372753202FEC2ED"}
                    #     ,
                    #     "pricesInfo": {"priceMap": {"CURRENT": {"price": 2.86,
                    #     "currencyUnitSymbol": "$",
                    #     "currencyUnit": "USD"}
                    #     ,
                    #     "LIST": {"price": 4.88,
                    #     "currencyUnitSymbol": "$",
                    #     "currencyUnit": "USD"}
                    #     }
                    #     }
                    #     ,
                    #     "addToCart": {"status": "CTA_INITIALIZED"}
                    #     ,
                    #     "productAvailability": {"availabilityStatus": "OUT_OF_STOCK"}
                    #     ,
                    #     "fulfillment": {}
                    #     ,
                    #     "removeATC": False,
                    #     "priceFlagsList": [],
                    #     "hasFreightShipping": False,
                    #     "id": "089B8844902045B29372753202FEC2ED",
                    #     "isEDeliveryItem": False}

                    try:
                        for key in option_product_list.keys():
                            offer_id = option_product_list[key]["offer_id"]
                            for offer in offer_list:
                                if offer_id == offer["id"]:
                                    current_obj = offer["pricesInfo"]["priceMap"]["CURRENT"]
                                    price_str = u"%s" % (current_obj["price"])
                                    options = option_product_list[key]["options"]
                                    price_item = {"price" : (current_obj["currencyUnitSymbol"] + price_str).encode('utf-8').strip()}
                                    option_product_list[key]["options"].append(price_item)
                    except:
                        logging.info("-----------Price Map Error --------" + response.url)
                        return
                    
                    #logging.info(option_product_list)
                    options_str = []
                    for key in option_product_list.keys():
                        options = option_product_list[key]["options"]
                        
                        item = {}
                        for option in options:
                            for k in option.keys():
                                item[k] = option[k]
                                options_str.append(item)
                    
                    product["options"] = options_str

        if product["delivery_time"] == "":
            delivery_divs =  response.xpath("//div[@class='prod-fulfillment-messaging-text']/div")

            for delivery in delivery_divs:
                delivery_str = delivery.xpath("..//div[@class='font-semibold']/text()").extract_first().encode('utf-8').strip()
                if delivery_str is not None:
                    product["delivery_time"] = delivery_str

        if response.meta["type"] == "browse":
            product["select_table"] = self.category_table
        else:
            product["select_table"] = "upload"

        if product["pictures_url"] != "":
            yield product
    
    
    #Get All Products by Category
    def parse_category_items(self, response):
        #logging.info( "++++++++++++++++++++++++++++")
        #logging.info(response.url)

        next_page_url = ""
        min_value = response.meta["min_value"]

        result_condition =  response.xpath("//span[contains(text(), 'find any results with the Price Filter you selected')]")
        if len(result_condition) > 0:
            if min_value > 500000:
                logging.info("-------------Items Not Found--------" + response.url)
                logging.info(min_value)
                return
        else:
            script_lists = response.xpath("//script[contains(text(), 'window.__WML_REDUX_INITIAL_STATE__ = ')]")
            for script in script_lists:
                content = script.xpath("text()").extract_first()
                if content != "":
                    json_obj = json.loads(content.split("window.__WML_REDUX_INITIAL_STATE__ = ")[1][0:-1])

                    pagination = json_obj["preso"]["pagination"]
                    try:
                        next_page_url = response.urljoin("browse?" + pagination["next"]["url"])
                    except Exception as e:
                        next_page_url = ""

                    items = json_obj["preso"]["items"]

                    for item in items:

                        # "productId": item["productId"],
                        # "usItemId": item["usItemId"],
                        # "productType": item["productType"],
                        # "title": item["title"],
                        # "description":item["description"],
                        # "imageUrl": item["imageUrl"],
                        # "productPageUrl": item["productPageUrl"],
                        # "upc":item["upc"],
                        # "department": item["department"],
                        # "customerRating": item["customerRating"],
                        # "numReviews": item["numReviews"],
                        # "specialOfferBadge":item["specialOfferBadge"],
                        # "specialOfferText": item["specialOfferText"],
                        # "specialOfferLink": item["specialOfferLink"],
                        # "sellerId": item["sellerId"],
                        # "sellerName":item["sellerName"],
                        # "preOrderAvailableDate": item["preOrderAvailableDate"],
                        # "launchDate": item["launchDate"],
                        # "enableAddToCart": item["enableAddToCart"],
                        # "canAddToCart": item["canAddToCart"],
                        # "showPriceAsAvailable": item["showPriceAsAvailable"],
                        # "seeAllName": item["seeAllName"],
                        # "seeAllLink": item["seeAllLink"],
                        # "itemClassId": item["itemClassId"],
                        # "variantSwatches": item["variantSwatches"],
                        # "ppu": item["ppu"],
                        # "primaryOffer":item["primaryOffer"],
                        # "fulfillment":item["fulfillment"],
                        # "inventory":item["inventory"],
                        # "quantity":item["quantity"],
                        # "marketPlaceItem":item["marketPlaceItem"],
                        # "preOrderAvailable":item["preOrderAvailable"],
                        # "blitzItem":item["blitzItem"],
                        # "twoDayShippingEligible":item["twoDayShippingEligible"],
                        # "shippingPassEligible":item["shippingPassEligible"],

                        # logging.info item

                        product = WalmartItem()
                        
                        product["usItemId"] = item["usItemId"]
                        product["product_id"] = item["productId"]

                        for db_item in self.product_list:
                            if db_item[0] ==  product["product_id"]:
                                logging.info("*****Product ID Exists*******" + product["product_id"] +  response.url)    
                                return
                        
                        try:
                            product["product_type"] = item["productType"].encode('utf-8').strip()
                        except:
                            logging.info("*********Product Type Error**********" + response.url)
                            #logging.info(item)
                            continue

                        if product["product_type"]  == "BUNDLE":
                            logging.info("------- Product is Bundle --------" + response.url)
                            continue

                        product["product_name"] = item["title"].encode('utf-8').strip()
                        
                        product["create_time"] =  response.meta["current_time"]
                        
                        try:
                            product["product_url"] = response.urljoin(item["productPageUrl"])
                        except:
                            logging.info("********Product URL Error********" + response.url)
                            continue
                        
                        if "(Pack of" in product["product_name"]:
                            product["product_type"] = "PACKS OF"

                        try:
                            product["special_offer"] = item["specialOfferText"]
                        except:
                            product["special_offer"] = ""

                        if item["twoDayShippingEligible"] == "false":
                            product["delivery_time"] = ""
                        else:
                            product["delivery_time"] = "2-Day Shipping"
                        
                        # "fulfillment":{"isS2H":true,
                        # "isS2S":false,
                        # "isSOI":false,
                        # "isPUT":false,
                        # "s2HDisplayFlags":["MARKETPLACE",
                        # "FREE_SHIPPING"]}

                        #Shipping & Pickup Information
                        try:
                            product["fulfillment"] = item["fulfillment"]
                        except:
                            product["fulfillment"] = ""
                        
                        #logging.info(product["fulfillment"])

                        try:
                            product["bullet_points"] = item["description"].encode('utf-8').strip()
                        except:
                            product["bullet_points"] = ""

                        try:
                            product["review_numbers"] = item["numReviews"]
                        except:
                            product["review_numbers"] = 0

                        product["inventory"] = item["quantity"]

                        try:
                            product["seller"] = item["sellerName"].encode('utf-8').strip()
                        except:
                            logging.info("*******Seller Error*******" + response.url)
                            continue

                        try:
                            showMinMaxPrice = item["primaryOffer"]["showMinMaxPrice"]
                        except:
                            showMinMaxPrice = False

                        try:
                            if showMinMaxPrice == False:
                                offerPrice = item["primaryOffer"]["offerPrice"]
                                product["cost"] = str(offerPrice)
                            else:
                                minPrice = item["primaryOffer"]["minPrice"]
                                maxPrice = item["primaryOffer"]["maxPrice"]
                                product["cost"] = str(minPrice) + " ~ " + str(maxPrice)
                        except:
                            logging.info("*********Offer Price Error*******" + response.url)
                            continue


                        exist =  False

                        for p in self.item_list:
                            if p["product_id"] == product["product_id"]:
                                logging.info("******Product Exist*****" + response.url)
                                exists = True

                        if exist == False:
                            self.item_list.append(product)

                            req = self.set_proxies(product["product_url"], self.parse_item_detail_page)
                            req.meta["product"] = product
                            req.meta["type"] = "browse"
                            yield req

        if next_page_url != "":
            min_value = response.meta["min_value"]
            max_value = calc_min_max(min_value)

            #logging.info( "*******************Next Page*********************")
            #logging.info(next_page_url)
            #logging.info( len(self.item_list))

            req = self.set_proxies(next_page_url, self.parse_category_items)
            req.meta["min_value"] = min_value
            req.meta["url"] = response.meta["url"]
            req.meta["current_time"] = response.meta["current_time"]
            
            yield req
        else:
            min_value = calc_min_max(response.meta["min_value"]) + 1
            max_value = calc_min_max(min_value)
            price_str = "&min_price=" + str(min_value) + "&max_price=" + str(max_value)

            #logging.info( " -----------Next Price -----------" + response.urljoin(response.meta["url"]) + "&page=1" + price_str)

            req = self.set_proxies(response.urljoin(response.meta["url"]) + "&sort=new&page=1" + price_str , self.parse_category_items)
            req.meta["url"] = response.meta["url"]
            req.meta["min_value"] = min_value
            req.meta["current_time"] = response.meta["current_time"]
            
            yield req

    #Quick Scraping Function
    def parse_category_items_by_upload(self, response):
        #logging.info( "++++++++++++++++++++++++++++")
        #logging.info(response.url)

        next_page_url = ""

        result_condition =  response.xpath("//span[contains(text(), 'Sorry, no products matched')]")
        if len(result_condition) > 0:
            logging.info("-------------Items Not Found--------" + response.url)
            return
        else:
            script_lists = response.xpath("//script[contains(text(), 'window.__WML_REDUX_INITIAL_STATE__ = ')]")
            for script in script_lists:
                content = script.xpath("text()").extract_first()
                if content != "":
                    json_obj = json.loads(content.split("window.__WML_REDUX_INITIAL_STATE__ = ")[1][0:-1])

                    pagination = json_obj["preso"]["pagination"]
                    
                    try:
                        next_page_url = response.urljoin("?" + pagination["next"]["url"])
                    except Exception as e:
                        next_page_url = ""

                    items = json_obj["preso"]["items"]

                    for item in items:

                        upload_ids = response.meta["ids"]
                        
                        # "productId": item["productId"],
                        # "usItemId": item["usItemId"],
                        # "productType": item["productType"],
                        # "title": item["title"],
                        # "description":item["description"],
                        # "imageUrl": item["imageUrl"],
                        # "productPageUrl": item["productPageUrl"],
                        # "upc":item["upc"],
                        # "department": item["department"],
                        # "customerRating": item["customerRating"],
                        # "numReviews": item["numReviews"],
                        # "specialOfferBadge":item["specialOfferBadge"],
                        # "specialOfferText": item["specialOfferText"],
                        # "specialOfferLink": item["specialOfferLink"],
                        # "sellerId": item["sellerId"],
                        # "sellerName":item["sellerName"],
                        # "preOrderAvailableDate": item["preOrderAvailableDate"],
                        # "launchDate": item["launchDate"],
                        # "enableAddToCart": item["enableAddToCart"],
                        # "canAddToCart": item["canAddToCart"],
                        # "showPriceAsAvailable": item["showPriceAsAvailable"],
                        # "seeAllName": item["seeAllName"],
                        # "seeAllLink": item["seeAllLink"],
                        # "itemClassId": item["itemClassId"],
                        # "variantSwatches": item["variantSwatches"],
                        # "ppu": item["ppu"],
                        # "primaryOffer":item["primaryOffer"],
                        # "fulfillment":item["fulfillment"],
                        # "inventory":item["inventory"],
                        # "quantity":item["quantity"],
                        # "marketPlaceItem":item["marketPlaceItem"],
                        # "preOrderAvailable":item["preOrderAvailable"],
                        # "blitzItem":item["blitzItem"],
                        # "twoDayShippingEligible":item["twoDayShippingEligible"],
                        # "shippingPassEligible":item["shippingPassEligible"],

                        # logging.info item

                        product = WalmartItem()
                        product["usItemId"] = item["usItemId"]
                        product["product_id"] = item["productId"]

                        try:
                            product["product_type"] = item["productType"].encode('utf-8').strip()
                        except:
                            logging.info("*********Product Type Error**********" + response.url)
                            #logging.info(item)
                            continue

                        if product["product_type"]  == "BUNDLE":
                            continue

                        product["product_name"] = item["title"].encode('utf-8').strip()
                        
                        product["create_time"] =  response.meta["current_time"]
                        
                        try:
                            product["product_url"] = response.urljoin(item["productPageUrl"])
                        except:
                            logging.info("********Product URL Error********" + response.url)
                            continue
                        
                        if "(Pack of" in product["product_name"]:
                            product["product_type"] = "PACKS OF"

                        try:
                            product["special_offer"] = item["specialOfferText"]
                        except:
                            product["special_offer"] = ""

                        if item["twoDayShippingEligible"] == "false":
                            product["delivery_time"] = ""
                        else:
                            product["delivery_time"] = "2-Day Shipping"
                        
                        # "fulfillment":{"isS2H":true,
                        # "isS2S":false,
                        # "isSOI":false,
                        # "isPUT":false,
                        # "s2HDisplayFlags":["MARKETPLACE",
                        # "FREE_SHIPPING"]}

                        #Shipping & Pickup Information
                        try:
                            product["fulfillment"] = item["fulfillment"]
                        except:
                            product["fulfillment"] = ""
                        
                        #logging.info(product["fulfillment"])

                        try:
                            product["bullet_points"] = item["description"].encode('utf-8').strip()
                        except:
                            product["bullet_points"] = ""

                        try:
                            product["review_numbers"] = item["numReviews"]
                        except:
                            product["review_numbers"] = 0

                        product["inventory"] = item["quantity"]

                        try:
                            product["seller"] = item["sellerName"].encode('utf-8').strip()
                        except:
                            logging.info("*******Seller Error*******" + response.url)
                            product["seller"] = ""
                            continue

                        try:
                            showMinMaxPrice = item["primaryOffer"]["showMinMaxPrice"]
                        except:
                            showMinMaxPrice = False

                        try:
                            if showMinMaxPrice == False:
                                offerPrice = item["primaryOffer"]["offerPrice"]
                                product["cost"] = str(offerPrice)
                            else:
                                minPrice = item["primaryOffer"]["minPrice"]
                                maxPrice = item["primaryOffer"]["maxPrice"]
                                product["cost"] = str(minPrice) + " ~ " + str(maxPrice)
                        except:
                            logging.info("*********Offer Price Error*******" + response.url)
                            continue


                        exist =  False

                        for p in self.item_list:
                            if p["product_id"] == product["product_id"]:
                                exists = True

                        if exist == False:
                            self.item_list.append(product)

                            req = self.set_proxies(product["product_url"], self.parse_item_detail_page)
                            req.meta["product"] = product
                            req.meta["type"] = "upload"
                            req.meta["ids"] = response.meta["ids"]
                            yield req

        if next_page_url != "":
            logging.info( "*******************Next Page*********************")
            logging.info(next_page_url)
            logging.info( len(self.item_list))

            req = self.set_proxies(next_page_url, self.parse_category_items_by_upload)
            req.meta["url"] = response.meta["url"]
            req.meta["current_time"] = response.meta["current_time"]
            req.meta["ids"] = response.meta["ids"]
            yield req