#!/bin/sh

cd /home/ftpuser/
PATH=$PATH:/usr/local/bin
export PATH
scrapy crawl walmartspider -a category="Cell Phones"  >> log_cell_phones.txt 2>&1
