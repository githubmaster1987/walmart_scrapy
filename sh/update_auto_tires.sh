#!/bin/sh

#cd /home/ftpuser/
PATH=$PATH:/usr/local/bin
export PATH
scrapy crawl walmartspider -a category="Auto & Tires" >> log_auto_tires.txt 2>&1
