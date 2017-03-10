#!/bin/sh

cd /home/ftpuser/
PATH=$PATH:/usr/local/bin
export PATH
scrapy crawl walmartspider -a category="Food" >> log_food.txt 2>&1
