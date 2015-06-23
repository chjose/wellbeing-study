#!/usr/bin/python

#import time

__author__ = 'Christin'
from collections import defaultdict
import utils
import glob
from time import localtime, strftime
import sqlite3 as lite
import string
import time
import os.path
import csv
import sys

#f = glob.glob("Z:/test/*.db")
#f = glob.glob("/net/garms/users/cj279/alldata/db_files/*.db")
#f = glob.glob("/net/garms/users/cj279/alldata/New version app/*.db")
# dongho begin
curr_path = os.getcwd()
input_path = curr_path + "/*.db"
f = glob.glob(input_path)
# dongho end
print(f)


if os.path.isfile('./new1.db'):
  print "Files already exists please delete it and try!!!!!!"
  sys.exit()

else:
  main_con = lite.connect('./new1.db')

  cur1 = main_con.cursor()
  main_con.execute('''CREATE TABLE data
       (ID TEXT,
        DEVICE TEXT,
        PROBE TEXT,
        TIMESTAMP LONG,
        VALUE TEXT);''')

print "Table created successfully"

cur1.execute("attach ? as toMerge",('./new1.db',))

for db_file in f:
    print("Current DB file is",db_file)
    cur1.execute("attach ? as toMerge1",(db_file,))
    cur1.execute("insert into toMerge.data select * from toMerge1.data")
    cur1.execute("detach database toMerge1")

