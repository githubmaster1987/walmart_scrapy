#!/bin/sh

cd /home/ftpuser/
PATH=$PATH:/usr/local/bin
export PATH
scrapy crawl walmartspider -a category="Sports & Outdoors" >> log_sports_outdoors.txt 2>&1
