#!/bin/sh

cd /home/ftpuser/
PATH=$PATH:/usr/local/bin
export PATH
scrapy crawl walmartspider -a category="Household Essentials" >> log_household_essentials.txt 2>&1
