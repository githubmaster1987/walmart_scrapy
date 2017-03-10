#!/bin/sh

cd /home/ftpuser/
PATH=$PATH:/usr/local/bin
export PATH
scrapy crawl walmartspider -a category="Beauty"  >> log_beauty.txt 2>&1
