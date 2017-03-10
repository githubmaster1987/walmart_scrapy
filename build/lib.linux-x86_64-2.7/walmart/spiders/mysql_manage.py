import MySQLdb as mdb
import numbers
import json
import config_scrapy as config

con = mdb.connect(host=config.db_host, user=config.db_user, passwd=config.db_pwd, db=config.db_database)
def retrieve_data(sql):
    with con:
        cur = con.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return rows

def execute_sql(sql):
    with con:
        cur = con.cursor()
        cur.execute(sql)
        con.commit()

# def delete_products():
#     sql  = "DELETE FROM product"
#     execute_sql(sql)