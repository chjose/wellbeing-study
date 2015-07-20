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

device_imei = {}
device_id_list = []
row_id_list = []

cur1 = main_con.cursor()

cur1.execute("PRAGMA temp_store = 2")
cur1 = main_con.cursor()
data_imei = cur1.execute("""select device, value from data where probe like '%Hardware%'""")

for x in data_imei:
  duration_index = x[1].find('"deviceId"')
  duration_index = x[1].find(':', duration_index)
  last_index = x[1].find(',', duration_index)
  imei_num = x[1][duration_index+2:last_index-1]
  device_imei[x[0]]=imei_num

cur1.execute("PRAGMA temp_store = 2")
cur1 = main_con.cursor()
data_imei = cur1.execute("""select rowid,device from data""")

for x in data_imei:
  device_id_list.append(x[1])
  row_id_list.append(x[0])

cur1 = main_con.cursor()
cur1.execute("alter table data add column imei text")
i = 0

cur1 = main_con.cursor()
for device in device_id_list:
  try:
    column_field = device_imei[device]
  except KeyError:
    column_field = device

  print column_field, row_id_list[i]
  try:
    cur1.execute("""update data set imei="%s" where rowid="%d";""" % (column_field, row_id_list[i]))
  except lite.OperationalError:
    print column_field
  i += 1

cur1.execute("PRAGMA temp_store = 2")
cur1.execute("VACUUM data;")
main_con.commit()

'''
cur1 = main_con.cursor()
data = cur1.execute("select * from data;")

for x in data:
  print x
'''
