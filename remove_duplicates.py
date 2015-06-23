#!/usr/bin/python

#import time

__author__ = 'Christin'
import glob
import sqlite3 as lite
import os.path
import sys

curr_path = os.getcwd()


merged_db = curr_path+'/new1.db'
#merged_db = curr_path+'/all_data_3G.db'

print merged_db
main_con = lite.connect(merged_db)

cur1 = main_con.cursor()

cur1.execute("PRAGMA temp_store = 2")
cur1 = main_con.cursor()
cur1.execute("""delete from data 
                where id not in 
                (select id
                 from data 
                 group by probe,timestamp,imei);""")

cur1.execute("PRAGMA temp_store = 2")
cur1.execute("VACUUM data;")
main_con.commit()
#main_con.close()
