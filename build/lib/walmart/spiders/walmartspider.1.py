# -*- coding: utf-8 -*-
import scrapy
import proxylist
import logging
import useragent
from scrapy.http import Request, FormRequest
import time, re, random, base64

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

    max_value = min_value + 100
    if min_value < 100:
        max_value = min_value + 10
    elif min_value < 500:
        max_value = min_value + 30
    elif min_value < 2000:
        max_value = min_value + 50
    elif min_value < 5000:
        max_value = min_value + 100
    elif min_value < 10000:
        max_value = min_value + 500

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

    product_list = []
    item_list = []

    news_item_cnt = 0
    save_item_cnt = 0

    def __init__(self,  method ='', *args, **kwargs):
        super(WalmartspiderSpider, self).__init__(*args, **kwargs)
        self.method = method

    def set_proxies(self, url, callback):
        req = Request(url=url, callback=callback,dont_filter=True)
        proxy_url = self.proxy_lists[random.randrange(0,len(self.proxy_lists))]

        user_pass=base64.encodestring(b'silicons:1pRnQcg87F').strip().decode('utf-8')
        req.meta['proxy'] = "http://" + proxy_url
        req.headers['Proxy-Authorization'] = 'Basic ' + user_pass

        user_agent = self.useragent_lists[random.randrange(0, len(self.useragent_lists))]
        req.headers['User-Agent'] = user_agent
        return req
    
    def parse_root_url(self, response):
        department = response.meta["department"]

        category_divs = response.xpath("//li[@class='department-single-level']/a")
        
        with open(config.output_category_csv_name, 'a') as csvfile:
            csv_writer = csv.writer(csvfile)

            for category in category_divs:
                url = category.xpath("@href").extract_first()
                name = category.xpath("text()").extract_first()
                csv_writer.writerow([department, name, response.urljoin(url)])
            

    def parse_method(self, response):
        #bundle product
        #req = self.set_proxies("https://www.walmart.com/nco/Purely-Inspired-100-Pure-Garcinia-Cambogia-Dietary-Supplement-Tablets-100-count-Value-Bundle-Pack-of-2/52589521", self.parse_item_detail_page)
        #req = self.set_proxies("https://www.walmart.com/ip/STCKNG-COMPRSN-KNEE-SM/42123288", self.parse_item_detail_page)
        #req = self.set_proxies("https://www.walmart.com/ip/Fruit-of-the-Loom-Men-s-Fleece-Crew-Sweatshirt/25033121", self.parse_item_detail_page)
        #yield req
        #return
        
        if self.method == "":
            with open(config.output_category_csv_name) as csvfile:
                reader = csv.reader(csvfile)
                self.logger.info ( "-----------------CSV Read------------------" )
                self.logger.info( config.output_category_csv_name)
                i = 0
            
                for input_item in reader:
                    if i>0:
                        if input_item[0] == "Health":
                            self.category_urls.append(input_item[2])
                            self.logger.info( input_item[2])
                    i += 1
            
            self.product_list =  db.retrieve_data("Select * From product")

            # category_divs = response.xpath("//li[@class='department-single-level']/a")

            # for category in category_divs:
            #     url = category.xpath("@href").extract_first()
            #     name = category.xpath("text()").extract_first()
            #     self.category_urls.append(response.urljoin(url))

            #self.category_urls = []
            #self.category_urls.append('?cat_id=1085666')
            #self.category_urls.append('?cat_id=1085666_1005862_1007221')
            #self.category_urls.append('?cat_id=5438')

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for url in self.category_urls:
                #min_value = random.randint(1, 5000)
                min_value = 1
                max_value = calc_min_max(min_value)
                price_str = "&min_price=" + str(min_value) + "&max_price=" + str(max_value)

                #url = url

                req = self.set_proxies(response.urljoin(url) + "&page=1" + price_str , self.parse_category_items)
                req.meta["min_value"] = min_value
                req.meta["url"] = url
                req.meta["current_time"] = current_time
                yield req

        elif self.method == "csv_category_by_tag":
            with open(config.output_category_csv_name, 'w') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow(["Department", "Sub Department", "Sub Department URL"])

                category_divs = response.xpath("//li[@class='department-single-level']/a")

                for category in category_divs:
                    root_url = category.xpath("@href").extract_first()
                    root_name = category.xpath("text()").extract_first()

                    req = self.set_proxies(response.urljoin(root_url), self.parse_root_url)
                    req.meta["department"] = root_name
                    yield req
                

        # elif self.method == "csv_category":
        #     with open(config.output_category_csv_name, 'w') as csvfile:
        #         csv_writer = csv.writer(csvfile)
        #         #CSV File Header
        #         csv_writer.writerow(["Department", "Sub Department","Sub Department URL", "Category", "Category URL"])

        #     self.logger.info.info ("********************************")
        #     self.logger.info.info ( response.url)

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
        #                             self.logger.info.info (  "*****************DEPARTMENT*****************")
        #                             department_name =  department["name"]
        #                             self.logger.info( department_name)
        #                             csv_writer.writerow([department_name, "","", "", ""])

        #                             sub_departments = department["departments"]
        #                             self.logger.info( "-----------------SUB DEPARTMENT-----------------")


        #                             for sub_department in sub_departments:
        #                                 sub_department_name = sub_department["department"]["title"]
        #                                 try:
        #                                     sub_department_url = response.urljoin(sub_department["department"]["clickThrough"]["value"])
        #                                 except:
        #                                     self.logger.info(sub_department["department"])
        #                                     sub_department_url = ""

        #                                 csv_writer.writerow(["", sub_department_name, sub_department_url , "", ""])
        #                                 try :
        #                                     sub_categories = sub_department["categories"]
        #                                     self.logger.info( "-----------------SUB CATEGORY-----------------")
        #                                     if len(sub_categories) > 0:
        #                                         for sub_category in sub_categories:
        #                                             sub_category_name = sub_category["category"]["title"]
        #                                             sub_category_link = response.urljoin(sub_category["category"]["clickThrough"]["value"])
        #                                             csv_writer.writerow(["", "", "", sub_category_name, sub_category_link])
        #                                 except :
        #                                     error = 0

    def start_requests(self):
        # db.delete_products()
        # return
        
    	req = self.set_proxies(self.parent_url, self.parse_method)
        yield req

    def parse_item_detail_page(self, response):
        # self.logger.info( "**********Detail************")
        # self.logger.info( response.url)
        product = response.meta["product"]
        
        #product["product_type"] ="VARIANT" 
        # product = WalmartItem()
        # product["delivery_time"] = ""

        product["pictures_url"] = ""
        product["category"] = ""
        product["option_size"] = ""
        product["option_color"] = ""
        product["cost_option"] = ""
        product["details"] = ""

        size_list = []
        color_list = []
        size_color_item_list = []
        offer_list = []
        sub_item_list = []
        specification_list = []

        error_flag = 0

        script_lists = response.xpath("//script[contains(text(), 'window.__WML_REDUX_INITIAL_STATE__ = ')]")
        for script in script_lists:
            content = script.xpath("text()").extract_first()
            if content != "":
                json_obj = json.loads(content.split("window.__WML_REDUX_INITIAL_STATE__ = ")[1][0:-1])
                #self.logger.info(json_obj)

                json_prod = json_obj["product"]
                # for key in json_prod:
                #     self.logger.info(key)
                
                #return

                image_list = []

                try:
                    for key in json_prod['images'].keys():
                        image_list.append(json_prod['images'][key]["assetSizeUrls"]["main"])

                    product["pictures_url"] = image_list
                except:
                    error_flag = 1
                    product["pictures_url"] = ""

                #Select ID to get Production information
                product_id = json_prod["selected"]["product"]

                #Product Category Path
                product["category"] = json_prod["products"][product_id]["productAttributes"]["productCategory"]["categoryPath"]

                try:
                    specifications =  json_prod["idmlMap"][product_id]["modules"]["Specifications"]["specifications"]["values"][0]
                    for specification in specifications:
                        for key in specification.keys():
                            sp_item = {}
                            sp_name = specification[key]["displayName"]
                            sp_value = ",".join(specification[key]["values"])
                            sp_item["value"] = sp_value
                            sp_item["name"] = sp_name
                            specification_list.append(sp_item)

                    product["details"] = specification_list
                except:
                    self.logger.info("*************************")
                    self.logger.info("***********Specification Does Not Exist**************")
                    product["details"] = ""
                    error_flag = 1
                    self.logger.info("*************************")

                #There are offers information in JSON product items
                for key in json_prod["offers"].keys():
                    offer_list.append(json_prod["offers"][key])

                #There are products information in JSON product items
                for key in json_prod["products"].keys():
                    sub_item_list.append(json_prod["products"][key])

                if product["product_type"] == "VARIANT":

                    color_exists_flag = 1
                    actual_colors = {}
                    sizes = {}
                    # Actual Colors & Size
                    try:
                        actual_colors = json_prod["variantCategoriesMap"][product_id]["actual_color"]["variants"]
                    except:
                        color_exists_flag = 0
                    try:
                        sizes = json_prod["variantCategoriesMap"][product_id]["size"]["variants"]
                    except:
                        self.logger.info ("*********************Variant Error********************")
                        self.logger.info (response.url)
                        error_flag = 1
                        #self.logger.info (json_prod["variantCategoriesMap"][product_id])
                        continue

                    for key in sizes.keys():
                        size_list.append(sizes[key]["name"])

                    if color_exists_flag == 1:
                        for key in actual_colors.keys():
                            color_list.append(actual_colors[key]["name"])
                    
                    
                        for size_key in sizes.keys():
                            size_product_list = sizes[size_key]["products"]
                            for color_key in actual_colors.keys():
                                color_product_list = actual_colors[color_key]["products"]

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

                                for size_product in size_product_list:
                                    for color_product in color_product_list:
                                        if size_product == color_product:
                                            size_color_item = {}
                                            size_color_item["size"] = sizes[size_key]["name"]
                                            size_color_item["color"] = actual_colors[color_key]["name"]
                                            size_color_item["product_id"] = size_product
                                            size_color_item["offer_id"] = ""
                                            size_color_item["price"] = ""

                                            size_color_item_list.append(size_color_item)
                    else:
                        for size_key in sizes.keys():
                            size_product_list = sizes[size_key]["products"]
                                #Get Product id to get offer. There is price in offer list

                                #**********************Example Product********************
                                #"52CRGN2EKSY1":{"usItemId":"42123292",
                                # "productId":"52CRGN2EKSY1",
                                # "upc":"631896020204",
                                # "wupc":"0763189602020",
                                # "primaryProductId":"4Y179A1VQZKZ",
                                # "status":"NOT_FETCHED",
                                # "offers":["6F73A0F040854475840E78A96B598DA8"],
                                # "variants":{"size":"size-small"}
                                # ,
                                # "productSegment":"Health & Beauty",
                                # "productType":"Compression Gloves,
                                # Sleeves & Socks",
                                # "cellCoverageLookupEnabled":false,
                                # "isRestrictedCategory":false}

                            for size_product in size_product_list:
                                size_color_item = {}
                                size_color_item["size"] = sizes[size_key]["name"]
                                size_color_item["color"] = ""
                                size_color_item["product_id"] = size_product
                                size_color_item["offer_id"] = ""
                                size_color_item["price"] = ""

                                size_color_item_list.append(size_color_item)
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

                    for size_color_item in size_color_item_list:
                        for sub_item in sub_item_list:
                            if size_color_item["product_id"] == sub_item["productId"]:

                                if len(sub_item["offers"]) > 1:
                                    self.logger.info("*************************")
                                    self.logger.info("***********Offer Size is more than 1**************")
                                    self.logger.info(response.url)
                                    self.logger.info("*************************")

                                size_color_item["offer_id"] = sub_item["offers"][0]

                    #Get Price by Size & Actual Color
                    for size_color_item in size_color_item_list:
                        for offer in offer_list:
                            if size_color_item["offer_id"] == offer["id"]:
                                current_obj = offer["pricesInfo"]["priceMap"]["CURRENT"]
                                price_str = u"%s" % (current_obj["price"])
                                size_color_item["price"] = current_obj["currencyUnitSymbol"] + price_str

                    # self.logger.info(size_color_item_list)

                    output_detail_list = []
                    for size_color_item in size_color_item_list:
                        output_detail_item = {}
                        output_detail_item["size"] = size_color_item["size"]
                        output_detail_item["color"] = size_color_item["color"]
                        output_detail_item["price"] = size_color_item["price"]
                        output_detail_list.append(output_detail_item)

                    #write option & details
                    product["option_size"] = size_list
                    product["option_color"] = color_list
                    product["cost_option"] = output_detail_list

        if product["delivery_time"] == "":
            delivery_divs =  response.xpath("//div[@class='prod-fulfillment-messaging-text']/div")

            for delivery in delivery_divs:
                str = delivery.xpath("..//div[@class='font-semibold']/text()").extract_first()
                if str is not None:
                    product["delivery_time"] = str

        #self.logger.info(product)

        if error_flag == 0:
            yield product

    def parse_category_items(self, response):
        #self.logger.info( "++++++++++++++++++++++++++++")
        #self.logger.info(response.url)

        next_page_url = ""
        min_value = response.meta["min_value"]

        result_condition =  response.xpath("//span[contains(text(), 'find any results with the Price Filter you selected')]")
        if len(result_condition) > 0:
            #self.logger.info( "------------------------Items Not Found-----------------------")
            #self.logger.info(response.url)
            if min_value > 500000:
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

                        # print item

                        product = WalmartItem()
                        
                        product["unique_id"] = item["usItemId"]
                        for db_item in self.product_list:
                            if db_item[0] ==  product["unique_id"]:
                                continue

                        try:
                            product["product_type"] = item["productType"].encode('utf-8').strip()
                        except:
                            self.logger.info("*****************Product Type Error*****************")
                            self.logger.info(response.url)
                            self.logger.info(item)
                            continue
                                                        

                        product["product_name"] = item["title"].encode('utf-8').strip()
                        
                        product["create_time"] =  response.meta["current_time"]
                        
                        try:
                            product["product_url"] = response.urljoin(item["productPageUrl"])
                        except:
                            self.logger.info("*****************Product URL Error*****************")
                            continue
                        
                        if "(Pack of" in product["product_name"]:
                            product["product_type"] = "Packs of"

                        try:
                            product["special_offer"] = item["specialOfferText"]
                        except:
                            product["special_offer"] = ""
                        
                        # if product["product_type"] == "Packs of":
                        #     self.logger.info("*****************Product Type*****************")
                        #     self.logger.info(product["product_url"])

                        # if product["special_offer"] != "" and product["special_offer"] != "BEST Seller":
                        #     self.logger.info("*****************SPECIAL OFFER*****************")
                        #     self.logger.info(product["special_offer"])

                        # if product["product_type"] != "VARIANT" and product["product_type"] != "REGULAR":
                        #     self.logger.info("*****************Product Type*****************")
                        #     self.logger.info(product["product_type"])
                        #     self.logger.info(product["product_url"])

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
                        
                        #self.logger.info(product["fulfillment"])

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
                            self.logger.info("*****************Seller Error*****************")
                            self.logger.info(item)
                            product["seller"] = ""

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
                            self.logger.info("*****************Offer Price Error*****************")
                            self.logger.info(item)

                        exist =  False

                        for p in self.item_list:
                            if p["unique_id"] == product["unique_id"]:
                                exists = True

                        if exist == False:
                            self.item_list.append(product)

                            req = self.set_proxies(product["product_url"], self.parse_item_detail_page)
                            req.meta["product"] = product
                            yield req

        if next_page_url != "":
            min_value = response.meta["min_value"]
            max_value = calc_min_max(min_value)

            #self.logger.info( "*******************Next Page*********************")
            #self.logger.info(next_page_url)
            #self.logger.info( len(self.item_list))

            req = self.set_proxies(next_page_url, self.parse_category_items)
            req.meta["min_value"] = min_value
            req.meta["url"] = response.meta["url"]
            req.meta["current_time"] = response.meta["current_time"]
            yield req
        else:
            min_value = calc_min_max(response.meta["min_value"]) + 1
            max_value = calc_min_max(min_value)
            price_str = "&min_price=" + str(min_value) + "&max_price=" + str(max_value)

            #self.logger.info( "*******************Next Price*********************")
            #self.logger.info( response.urljoin(response.meta["url"]) + "&page=1" + price_str)

            req = self.set_proxies(response.urljoin(response.meta["url"]) + "&page=1" + price_str , self.parse_category_items)
            req.meta["url"] = response.meta["url"]
            req.meta["min_value"] = min_value
            req.meta["current_time"] = response.meta["current_time"]
            yield req
