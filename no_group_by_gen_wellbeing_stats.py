#!/usr/bin/python
__author__ = 'Christin'
from collections import defaultdict
import utils
import glob
from time import localtime, strftime
import datetime
from datetime import date
import sqlite3 as lite
import string
import time
import csv
import math
from collections import Counter
from collections import OrderedDict
import optparse
import json
import numpy
from itertools import groupby
from operator import itemgetter
from geopy.distance import vincenty

import numpy as np
#from matplotlib import pyplot as plt

#from nitime import utils
from nitime import algorithms as alg
#from nitime.timeseries import TimeSeries
#from nitime.viz import plot_tseries


start_time = 0
end_time = 1432176252
parser = optparse.OptionParser("usage %prog "+"-p <path> -f <file> --start <epoch-time> --end <epoch-time> Ex: 'python %prog -p ~ -f my_test'")
parser.add_option('-p', dest='path', type='string', help='Please give the database path(all_data.db) excluding the file name.')
parser.add_option('-f', dest='file', type='string', help='This name will be prepended to the output csv`s generated')
parser.add_option('-i', dest='ignore', type='string', help='Put values as "true" to change the date range ')
parser.add_option('--start', dest='start', type='string', help='Start date for the wellbeing value calculation')
parser.add_option('--end', dest='end', type='string', help='End date for the wellbeing value calculation')
(options, args) = parser.parse_args()

if options.path == None:
  f = glob.glob("/net/garms/users/cj279/alldata/New version app/new/*.db")
  #f = glob.glob("/net/garms/users/cj279/alldata/New version app/test/*.db")

else:
  input_path = options.path + "/*.db"
  f = glob.glob(input_path)

file = ""
if options.file != None:
  file = options.file

if options.start != None and options.end != None:
  start_time = int(options.start)
  end_time = int(options.end)

print "Start time and End time"
print start_time
print end_time

if len(f) == 0:
  print("Enter a path where there is 'all_data.db' files")
  exit()
  
print(f)

print("WELLBEING INDEX... STATISTICS GENERATION IN PROGRESS...")
files_csv = {}
ar_values = {}
shannons_entropy_stats = {}
shannons_entropy_sms = {}
shannons_entropy_stats_disc = {}
shannons_entropy_sms_disc = {}
shannons_location = {}
shannons_location_update = {}
day_night_location_ratio = {}
weekday_weekend_location_ratio = {}
weekday_weekend_call_ratio = {}
weekday_weekend_sms_ratio = {}
device_imei = {}
location_stats = {}
location_stats_interval = {}
call_type_csv = []
sms_type_csv = []
eligible_days_call = {}
eligible_days_sms = {}
imei_timestamp = {}
imei_num_timestamp = {}
imei_smstimestamp = {}
imei_num_smstimestamp = {}

#New attributes
distinct_contacts = defaultdict(list)
distinct_contacts_sms = defaultdict(list)
call_duration_total = {}
incoming_call_count = {}
outgoing_call_count = {}
missed_call_count = {}
incoming_sms_count = {}
outgoing_sms_count = {}
day_night_call_ratio = {}
day_night_sms_ratio = {}

first_contacts_call = {}
second_contacts_call = {}
new_contacts_call = {}
first_contacts_call_disc = {}
second_contacts_call_disc = {}
new_contacts_call_disc = {}
new_contacts_call_outgoing = {}
first_contacts_call_outgoing = {}
second_contacts_call_outgoing = {}
first_contacts_sms = {}
second_contacts_sms = {}
new_contacts_sms = {}
first_contacts_sms_disc = {}
second_contacts_sms_disc = {}
new_contacts_sms_disc = {}
first_location = {}
second_location = {}
new_location = {}
location_list = {}
duration_call = {}
most_freq_contact_call = {}
response_params_call = {}
response_rate_count = {}
response_latency = {}
response_params_sms = {}
response_rate_count_sms = {}
response_latency_sms = {}
longest_call = {}
distinct_calls_outgoing = {}
first_location_discretionary = {}
second_location_discretionary = {}
new_location_discretionary = {}
shannons_location_discretionary = {}
shannons_location_discretionary3 = {}

con = 0

def write_stats_to_csv():
    d = defaultdict(list)
    dd = defaultdict(list)
    d_duration = defaultdict(list)

    cur = con.cursor()
    cur.execute("PRAGMA temp_store = 2")
    data_imei = cur.execute("""select device, imei from data
                             where probe like '%Hardware%'""")


    for x in data_imei:
      imei_num = x[1]
      if imei_num!=None:
        device_imei[x[0]]=str(imei_num)
  
    #for key, value in device_imei.iteritems():  
    #    print key+" "+value
    cur = con.cursor()
    cur.execute("PRAGMA temp_store = 2")
    data = cur.execute("""select device, timestamp, value from data
                          where probe like '%Call%'""")

    distinct_calls = {}
    call_duration = {}


    imei_list = device_imei.values()

    #print imei_list

    for imei in imei_list:
      date1 = datetime.date(2015, 2, 12)
      date2 = datetime.date(2015, 4, 26)
      day = datetime.timedelta(days=1)
      while date1 <= date2:
        std_mtime =  date1.strftime('%Y-%m-%d')
        try:
          location_stats_interval[imei]
        except KeyError:
          location_stats_interval[imei] = {}
        try:
          location_stats_interval[imei][std_mtime]
        except KeyError:
          location_stats_interval[imei][std_mtime] = {}
        location_stats_interval[imei][std_mtime]["00:00-01:00"] = 0      
        location_stats_interval[imei][std_mtime]["01:00-02:00"] = 0      
        location_stats_interval[imei][std_mtime]["02:00-03:00"] = 0      
        location_stats_interval[imei][std_mtime]["03:00-04:00"] = 0      
        location_stats_interval[imei][std_mtime]["04:00-05:00"] = 0      
        location_stats_interval[imei][std_mtime]["05:00-06:00"] = 0      
        location_stats_interval[imei][std_mtime]["06:00-07:00"] = 0      
        location_stats_interval[imei][std_mtime]["07:00-08:00"] = 0      
        location_stats_interval[imei][std_mtime]["08:00-09:00"] = 0      
        location_stats_interval[imei][std_mtime]["09:00-10:00"] = 0      
        location_stats_interval[imei][std_mtime]["10:00-11:00"] = 0      
        location_stats_interval[imei][std_mtime]["11:00-12:00"] = 0      
        location_stats_interval[imei][std_mtime]["12:00-13:00"] = 0      
        location_stats_interval[imei][std_mtime]["13:00-14:00"] = 0      
        location_stats_interval[imei][std_mtime]["14:00-15:00"] = 0      
        location_stats_interval[imei][std_mtime]["15:00-16:00"] = 0      
        location_stats_interval[imei][std_mtime]["16:00-17:00"] = 0      
        location_stats_interval[imei][std_mtime]["17:00-18:00"] = 0      
        location_stats_interval[imei][std_mtime]["18:00-19:00"] = 0      
        location_stats_interval[imei][std_mtime]["19:00-20:00"] = 0      
        location_stats_interval[imei][std_mtime]["20:00-21:00"] = 0      
        location_stats_interval[imei][std_mtime]["21:00-22:00"] = 0      
        location_stats_interval[imei][std_mtime]["22:00-23:00"] = 0      
        location_stats_interval[imei][std_mtime]["23:00-24:00"] = 0      
        date1 = date1 + day

    print "CALL RECORDS"

    for x in data:
        std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[1]))
        record_time = int(x[1])
        time_of_the_day = time.strftime('%H:%M', time.localtime(x[1]))
        
        number_index = x[2].find('"number"')
        last_index = x[2].find(',', number_index)
        call_id = x[2][number_index:last_index-4]
        
        # code below for the distinction between the type of call and recording it
        if record_time>start_time and record_time<end_time:
            formatted_call_record = x[2].replace('\\','').replace('"{','{').replace('}"','}')

            #print formatted_call_record
            json_call_record = json.loads(formatted_call_record)
            #print json_call_record['type']
            #print json_call_record['number']['ONE_WAY_HASH']
            call_type = json_call_record['type']
            call_type_csv_row = []

            # outgoing calls
            if call_type == 2:
                if device_imei[x[0]] in response_params_call:
                    if 'incoming' in response_params_call[device_imei[x[0]]]:
                        if json_call_record['number']['ONE_WAY_HASH'] in response_params_call[device_imei[x[0]]]['incoming']:
                            response_params_call[device_imei[x[0]]]['incoming'][json_call_record['number']['ONE_WAY_HASH']].add(json_call_record['timestamp'])
                        else:
                            response_params_call[device_imei[x[0]]]['incoming'][json_call_record['number']['ONE_WAY_HASH']] = set()
                            response_params_call[device_imei[x[0]]]['incoming'][json_call_record['number']['ONE_WAY_HASH']].add(json_call_record['timestamp'])
                    else:
                        response_params_call[device_imei[x[0]]]['incoming'] = {}
                        response_params_call[device_imei[x[0]]]['incoming'][json_call_record['number']['ONE_WAY_HASH']] = set()
                        response_params_call[device_imei[x[0]]]['incoming'][json_call_record['number']['ONE_WAY_HASH']].add(json_call_record['timestamp'])
                else:
                    response_params_call[device_imei[x[0]]] = {}
                    response_params_call[device_imei[x[0]]]['incoming'] = {}
                    response_params_call[device_imei[x[0]]]['incoming'][json_call_record['number']['ONE_WAY_HASH']] = set()
                    response_params_call[device_imei[x[0]]]['incoming'][json_call_record['number']['ONE_WAY_HASH']].add(json_call_record['timestamp'])
                
                #Find distinct outgoing calls
                
                # Get the first two weeks contact, comparison date is 3-20-2015
                if record_time<1426824000:
                    if device_imei[x[0]] in first_contacts_call_outgoing:
                        first_contacts_call_outgoing[device_imei[x[0]]].add(json_call_record['number']['ONE_WAY_HASH'])
                    else:
                        first_contacts_call_outgoing[device_imei[x[0]]] = set()
                        first_contacts_call_outgoing[device_imei[x[0]]].add(json_call_record['number']['ONE_WAY_HASH'])
                else:
                    if device_imei[x[0]] in second_contacts_call_outgoing:
                        second_contacts_call_outgoing[device_imei[x[0]]].add(json_call_record['number']['ONE_WAY_HASH'])
                    else:
                        second_contacts_call_outgoing[device_imei[x[0]]] = set()
                        second_contacts_call_outgoing[device_imei[x[0]]].add(json_call_record['number']['ONE_WAY_HASH'])


            if call_type == 3:
                if device_imei[x[0]] in response_params_call:
                    if 'missed' in response_params_call[device_imei[x[0]]]:
                        if json_call_record['number']['ONE_WAY_HASH'] in response_params_call[device_imei[x[0]]]['missed']:
                            response_params_call[device_imei[x[0]]]['missed'][json_call_record['number']['ONE_WAY_HASH']].add(json_call_record['timestamp'])
                        else:
                            response_params_call[device_imei[x[0]]]['missed'][json_call_record['number']['ONE_WAY_HASH']] = set()
                            response_params_call[device_imei[x[0]]]['missed'][json_call_record['number']['ONE_WAY_HASH']].add(json_call_record['timestamp'])
                    else:
                        response_params_call[device_imei[x[0]]]['missed'] = {}
                        response_params_call[device_imei[x[0]]]['missed'][json_call_record['number']['ONE_WAY_HASH']] = set()
                        response_params_call[device_imei[x[0]]]['missed'][json_call_record['number']['ONE_WAY_HASH']].add(json_call_record['timestamp'])
                else:
                    response_params_call[device_imei[x[0]]] = {}
                    response_params_call[device_imei[x[0]]]['missed'] = {}
                    response_params_call[device_imei[x[0]]]['missed'][json_call_record['number']['ONE_WAY_HASH']] = set()
                    response_params_call[device_imei[x[0]]]['missed'][json_call_record['number']['ONE_WAY_HASH']].add(json_call_record['timestamp'])


            try:
                if call_type == 1:
                    #Incoming Call
                    call_type_csv_row.append(device_imei[x[0]])
                    call_type_csv_row.append(json_call_record['number']['ONE_WAY_HASH'])
                    call_type_csv_row.append(json_call_record['timestamp'])
                    call_type_csv_row.append("Incoming")
                    call_type_csv_row.append(json_call_record['duration'])
                    if incoming_call_count.has_key(device_imei[x[0]]):
                        incoming_call_count[device_imei[x[0]]] += 1
                    else:
                        incoming_call_count[device_imei[x[0]]] = 1

                
                if call_type == 2:
                    #Outgoing Call
                    call_type_csv_row.append(json_call_record['number']['ONE_WAY_HASH'])
                    call_type_csv_row.append(device_imei[x[0]])
                    call_type_csv_row.append(json_call_record['timestamp'])
                    call_type_csv_row.append("Outgoing")
                    call_type_csv_row.append(json_call_record['duration'])
                    if outgoing_call_count.has_key(device_imei[x[0]]):
                        outgoing_call_count[device_imei[x[0]]] += 1
                    else:
                        outgoing_call_count[device_imei[x[0]]] = 1

                if call_type == 3:
                    #Missed Call
                    call_type_csv_row.append(device_imei[x[0]])
                    call_type_csv_row.append(json_call_record['number']['ONE_WAY_HASH'])
                    call_type_csv_row.append(json_call_record['timestamp'])
                    call_type_csv_row.append("Missed")
                    call_type_csv_row.append(json_call_record['duration'])
                    if missed_call_count.has_key(device_imei[x[0]]):
                        missed_call_count[device_imei[x[0]]] += 1
                    else:
                        missed_call_count[device_imei[x[0]]] = 1
            
                if len(call_type_csv_row)>0:
                    call_type_csv.append(call_type_csv_row)
                else:
                    print "Find the mysterious call types"
                    print call_type
            except KeyError:
                pass

        # code ends here

        try:
          distinct_calls[x[0]]
        except KeyError:
          distinct_calls[x[0]] = defaultdict(list)

        distinct_calls[x[0]][std_mtime].append(call_id)
         
        duration_index = x[2].find('"duration"')
        duration_index = x[2].find(':', duration_index)
        last_index = x[2].find(',', duration_index)
        duration_str = x[2][duration_index+1:last_index]
        try:
          call_duration[x[0]]
        except KeyError:
          call_duration[x[0]] = {}

        try:
          call_duration[x[0]][std_mtime] += int(duration_str)
        except KeyError:
          call_duration[x[0]][std_mtime] = 0
          call_duration[x[0]][std_mtime] += int(duration_str)

        if record_time>start_time and record_time<end_time:
          date_time = datetime.datetime.strptime(std_mtime,'%Y-%m-%d') 
          try:
            device_imei[x[0]]
          except KeyError:
            device_imei[x[0]] = x[0]

          print "imei: "+str(device_imei[x[0]])+"Date: "+str(std_mtime)
          try:
            shannons_entropy_stats[device_imei[x[0]]]
          except KeyError:
            call_duration_total[device_imei[x[0]]] = 0
            shannons_entropy_stats[device_imei[x[0]]] = {}
            day_night_call_ratio[device_imei[x[0]]] = {}
            weekday_weekend_call_ratio[device_imei[x[0]]] = {}
            first_contacts_call[device_imei[x[0]]] = set()
            second_contacts_call[device_imei[x[0]]] = set()
            new_contacts_call[device_imei[x[0]]] = set()
            first_contacts_call_disc[device_imei[x[0]]] = set()
            second_contacts_call_disc[device_imei[x[0]]] = set()
            new_contacts_call_disc[device_imei[x[0]]] = set()
            most_freq_contact_call[device_imei[x[0]]] = []
       
          try:
            shannons_entropy_stats[device_imei[x[0]]][call_id] += 1
          except KeyError:
            shannons_entropy_stats[device_imei[x[0]]][call_id] = 1

          # Get the first two weeks contact, comparison date is 3-20-2015
          if record_time<1426824000:
            first_contacts_call[device_imei[x[0]]].add(call_id)
          else:
            second_contacts_call[device_imei[x[0]]].add(call_id)

          # Avoid the first two weeks data. Divide the remaining data into two. 
          # Remove data before 2-26-2015 and split data at 3-26-2015
          if record_time>1425010915 and record_time<1427342400:
            first_contacts_call_disc[device_imei[x[0]]].add(call_id)
          elif record_time>1427342400:
            second_contacts_call_disc[device_imei[x[0]]].add(call_id)
            

          if date_time.weekday()==5 or date_time.weekday()==6:
            try:
              weekday_weekend_call_ratio[device_imei[x[0]]]["weekend"] += 1
            except KeyError:
              weekday_weekend_call_ratio[device_imei[x[0]]]["weekend"] = 1
          else:
            try:
              weekday_weekend_call_ratio[device_imei[x[0]]]["weekday"] += 1
            except KeyError:
              weekday_weekend_call_ratio[device_imei[x[0]]]["weekday"] = 1

          #Distinct contacts
          distinct_contacts[device_imei[x[0]]].append(call_id)

          call_duration_total[device_imei[x[0]]] += int(duration_str)
          if time_of_the_day>"18:00" and time_of_the_day<"6:00":
            try:
              day_night_call_ratio[device_imei[x[0]]]["NIGHT"] += 1
            except:
              day_night_call_ratio[device_imei[x[0]]]["NIGHT"] = 1
          else:
            try:
              day_night_call_ratio[device_imei[x[0]]]["DAY"] += 1
            except:
              day_night_call_ratio[device_imei[x[0]]]["DAY"] = 1
          
          if device_imei[x[0]] in duration_call:
            if call_id in duration_call[device_imei[x[0]]]:
                print duration_call[device_imei[x[0]]][call_id]
                duration_call[device_imei[x[0]]][call_id]+=int(duration_str)
            else:
                duration_call[device_imei[x[0]]][call_id] = int(duration_str)

          else:
            duration_call[device_imei[x[0]]] = {}
            duration_call[device_imei[x[0]]][call_id] = int(duration_str)
            
          if device_imei[x[0]] in longest_call:
            if longest_call[imei]<int(duration_str):
                longest_call[imei] = int(duration_str)
          else:
            longest_call[imei] = int(duration_str)
    #print d  
    #for row in data:

    print "Number of calls for the all the devices on a day"
    
    for imei, value in distinct_calls.iteritems():
      for date, inner_list in value.iteritems():
        print "\n"
        print "Device ID: {}".format(imei)
        print "==============================================="

        distinct_cal = set(inner_list)
        distinct_cal = list(distinct_cal)
        distinct_cal.sort()

        try:
          files_csv[imei]
        except KeyError:
          files_csv[imei] = {}
       
        if date>'2015-02-11' and date<'2015-04-27': 
            files_csv[imei][date] = {}

            j = len(inner_list)

            files_csv[imei][date]["Num of calls"] = j
            files_csv[imei][date]["Total Duration"] = call_duration[imei][date]
            files_csv[imei][date]["Number of SMS"] = 0
            files_csv[imei][date]["Distinct users SMS"] = 0
            files_csv[imei][date]["Distinct Locations"] = 0

            print "Number of calls on {} is {}".format(imei,j)
           
            print "Total call duration on {} is {} second(s)".format(date,call_duration[imei][date])
            j = len(distinct_cal)
            files_csv[imei][date]["Distinct Calls"] = j

            print "Number of distinct users called on {} is {}".format(date,j)

    #print "Call type CSV"
    #print call_type_csv

    # Response rate for call

    for imei, call_type in response_params_call.iteritems():
        for type, num_dict in call_type['missed'].iteritems():
            for missed_ts in num_dict:
                if type in response_params_call[imei]['incoming']:
                    for incoming in response_params_call[imei]['incoming'][type]:
                        if (missed_ts+3600)>=incoming and missed_ts<incoming:
                            if imei in response_rate_count: 
                                response_rate_count[imei]+=1
                                response_latency[imei]+=(missed_ts+3600)-incoming
                            else:
                                response_rate_count[imei] = 1
                                response_latency[imei]=(missed_ts+3600)-incoming
                            break

    # code ends here

    # distinct outgoing calls

    for imei, call_type in response_params_call.iteritems():
        distinct_calls_outgoing[imei] = len(call_type['incoming'])

    # code ends here


    ########################################
     
    # Discretionary call stats

    for imei, value in new_contacts_call_disc.iteritems():
        new_contacts_call_disc[imei] = second_contacts_call_disc[imei] - first_contacts_call_disc[imei]

        for call in new_contacts_call_disc[imei]:
            if imei in shannons_entropy_stats_disc:
                shannons_entropy_stats_disc[imei][call] = shannons_entropy_stats[imei][call]
            else:
                shannons_entropy_stats_disc[imei] = {}
                shannons_entropy_stats_disc[imei][call] = shannons_entropy_stats[imei][call]

    #print shannons_entropy_stats_disc
    #######################################

    d = defaultdict(list)
    dd = defaultdict(list)
    distinct_sms = {}

    cur = con.cursor()
    cur.execute("PRAGMA temp_store = 2")
    data = cur.execute("""select device, timestamp, value from data
                          where probe like '%Sms%'""")

    print "SMS data"
    for x in data:
        std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[1]))
        d[x[0]].append(std_mtime)
        time_of_the_day = time.strftime('%H:%M', time.localtime(x[1]))
        record_time = int(x[1])
        
        number_index = x[2].find('"address"')
        last_index = x[2].find(',', number_index)
        call_id = x[2][number_index:last_index-4]
        print "Device {} time {} date {} call_id {}".format(x[0],x[1],std_mtime,call_id)
       
        # code below for the distinction between the type of call and recording it
        if record_time>start_time and record_time<end_time:
            formatted_sms_record = x[2].replace('\\','').replace('"{','{').replace('}"','}')

            print formatted_sms_record
            json_sms_record = json.loads(formatted_sms_record)
            print json_sms_record['type']
            print json_sms_record['address']['ONE_WAY_HASH']
            sms_type = json_sms_record['type']
            sms_type_csv_row = []

            try:
                device_imei[x[0]]
            except KeyError:
                device_imei[x[0]] = x[0]

            if sms_type == 1:
                if device_imei[x[0]] in response_params_sms:
                    if 'incoming' in response_params_sms[device_imei[x[0]]]:
                        if json_sms_record['address']['ONE_WAY_HASH'] in response_params_sms[device_imei[x[0]]]['incoming']:
                            response_params_sms[device_imei[x[0]]]['incoming'][json_sms_record['address']['ONE_WAY_HASH']].add(json_sms_record['timestamp'])
                        else:
                            response_params_sms[device_imei[x[0]]]['incoming'][json_sms_record['address']['ONE_WAY_HASH']] = set()
                            response_params_sms[device_imei[x[0]]]['incoming'][json_sms_record['address']['ONE_WAY_HASH']].add(json_sms_record['timestamp'])
                    else:
                        response_params_sms[device_imei[x[0]]]['incoming'] = {}
                        response_params_sms[device_imei[x[0]]]['incoming'][json_sms_record['address']['ONE_WAY_HASH']] = set()
                        response_params_sms[device_imei[x[0]]]['incoming'][json_sms_record['address']['ONE_WAY_HASH']].add(json_sms_record['timestamp'])
                else:
                    response_params_sms[device_imei[x[0]]] = {}
                    response_params_sms[device_imei[x[0]]]['incoming'] = {}
                    response_params_sms[device_imei[x[0]]]['incoming'][json_sms_record['address']['ONE_WAY_HASH']] = set()
                    response_params_sms[device_imei[x[0]]]['incoming'][json_sms_record['address']['ONE_WAY_HASH']].add(json_sms_record['timestamp'])

            if sms_type == 2:
                if device_imei[x[0]] in response_params_sms:
                    if 'outgoing' in response_params_sms[device_imei[x[0]]]:
                        if json_sms_record['address']['ONE_WAY_HASH'] in response_params_sms[device_imei[x[0]]]['outgoing']:
                            response_params_sms[device_imei[x[0]]]['outgoing'][json_sms_record['address']['ONE_WAY_HASH']].add(json_sms_record['timestamp'])
                        else:
                            response_params_sms[device_imei[x[0]]]['outgoing'][json_sms_record['address']['ONE_WAY_HASH']] = set()
                            response_params_sms[device_imei[x[0]]]['outgoing'][json_sms_record['address']['ONE_WAY_HASH']].add(json_sms_record['timestamp'])
                    else:
                        response_params_sms[device_imei[x[0]]]['outgoing'] = {}
                        response_params_sms[device_imei[x[0]]]['outgoing'][json_sms_record['address']['ONE_WAY_HASH']] = set()
                        response_params_sms[device_imei[x[0]]]['outgoing'][json_sms_record['address']['ONE_WAY_HASH']].add(json_sms_record['timestamp'])
                else:
                    response_params_sms[device_imei[x[0]]] = {}
                    response_params_sms[device_imei[x[0]]]['outgoing'] = {}
                    response_params_sms[device_imei[x[0]]]['outgoing'][json_sms_record['address']['ONE_WAY_HASH']] = set()
                    response_params_sms[device_imei[x[0]]]['outgoing'][json_sms_record['address']['ONE_WAY_HASH']].add(json_sms_record['timestamp'])

            try:
                if sms_type == 1:
                    #Incoming Sms
                    sms_type_csv_row.append(device_imei[x[0]])
                    sms_type_csv_row.append(json_sms_record['address']['ONE_WAY_HASH'])
                    sms_type_csv_row.append(json_sms_record['timestamp'])
                    sms_type_csv_row.append("Incoming")
                    if incoming_sms_count.has_key(device_imei[x[0]]):
                        incoming_sms_count[device_imei[x[0]]] += 1
                    else:
                        incoming_sms_count[device_imei[x[0]]] = 1
                
                if sms_type == 2:
                    #Outgoing Sms
                    sms_type_csv_row.append(json_sms_record['address']['ONE_WAY_HASH'])
                    sms_type_csv_row.append(device_imei[x[0]])
                    sms_type_csv_row.append(json_sms_record['timestamp'])
                    sms_type_csv_row.append("Outgoing")
                    if outgoing_sms_count.has_key(device_imei[x[0]]):
                        outgoing_sms_count[device_imei[x[0]]] += 1
                    else:
                        outgoing_sms_count[device_imei[x[0]]] = 1

                if len(sms_type_csv_row)>0:
                    sms_type_csv.append(sms_type_csv_row)
                else:
                    print "DANGER"
                    print sms_type
            except KeyError:
                pass

        # code ends here

        try:
          distinct_sms[x[0]]
        except KeyError:
          distinct_sms[x[0]] = defaultdict(list)

        distinct_sms[x[0]][std_mtime].append(call_id)
        
        if record_time>start_time and record_time<end_time:
          date_time = datetime.datetime.strptime(std_mtime,'%Y-%m-%d') 
          try:
            device_imei[x[0]]
          except KeyError:
            device_imei[x[0]] = x[0]

          print "imei: "+str(device_imei[x[0]])+"Date: "+str(std_mtime)
          try:
            shannons_entropy_sms[device_imei[x[0]]]
          except KeyError:
            shannons_entropy_sms[device_imei[x[0]]] = {}
            day_night_sms_ratio[device_imei[x[0]]] = {}
            weekday_weekend_sms_ratio[device_imei[x[0]]] = {}
            first_contacts_sms[device_imei[x[0]]] = set()
            second_contacts_sms[device_imei[x[0]]] = set()
            new_contacts_sms[device_imei[x[0]]] = set()
            first_contacts_sms_disc[device_imei[x[0]]] = set()
            second_contacts_sms_disc[device_imei[x[0]]] = set()
            new_contacts_sms_disc[device_imei[x[0]]] = set()
          
          try:
            shannons_entropy_sms[device_imei[x[0]]][call_id] += 1
          except KeyError:
            shannons_entropy_sms[device_imei[x[0]]][call_id] = 1
          
          #Distinct contacts
          distinct_contacts_sms[device_imei[x[0]]].append(call_id)
          
          # Get the first two weeks contact, comparison date is 3-20-2015
          if record_time<1426824000:
            first_contacts_sms[device_imei[x[0]]].add(call_id)
          else:
            second_contacts_sms[device_imei[x[0]]].add(call_id)
          
          # Avoid the first two weeks data. Divide the remaining data into two. 
          # Remove data before 2-26-2015 and split data at 3-26-2015
          if record_time>1425010915 and record_time<1427342400:
            first_contacts_sms_disc[device_imei[x[0]]].add(call_id)
          elif record_time>1427342400:
            second_contacts_sms_disc[device_imei[x[0]]].add(call_id)
            
          if date_time.weekday()==5 or date_time.weekday()==6:
            try:
              weekday_weekend_sms_ratio[device_imei[x[0]]]["weekend"] += 1
            except KeyError:
              weekday_weekend_sms_ratio[device_imei[x[0]]]["weekend"] = 1
          else:
            try:
              weekday_weekend_sms_ratio[device_imei[x[0]]]["weekday"] += 1
            except KeyError:
              weekday_weekend_sms_ratio[device_imei[x[0]]]["weekday"] = 1
          
          if time_of_the_day>"18:00" and time_of_the_day<"6:00":
            try:
              day_night_sms_ratio[device_imei[x[0]]]["NIGHT"] += 1
            except:
              day_night_sms_ratio[device_imei[x[0]]]["NIGHT"] = 1
          else:
            try:
              day_night_sms_ratio[device_imei[x[0]]]["DAY"] += 1
            except:
              day_night_sms_ratio[device_imei[x[0]]]["DAY"] = 1
            
    # Response rate for sms

    for imei, sms_type in response_params_sms.iteritems():
        for type, num_dict in sms_type['incoming'].iteritems():
            for incoming_ts in num_dict:
                if 'outgoing' in response_params_sms[imei]:
                    if type in response_params_sms[imei]['outgoing']:
                        for outgoing in response_params_sms[imei]['outgoing'][type]:
                            if (incoming_ts+3600)>=outgoing and missed_ts<outgoing:
                                if imei in response_rate_count_sms: 
                                    response_rate_count_sms[imei]+=1
                                    response_latency_sms[imei]+=(missed_ts+3600)-outgoing
                                else:
                                    response_rate_count_sms[imei] = 1
                                    response_latency_sms[imei]=(missed_ts+3600)-outgoing
                                break

    # code ends here

    print "Number of SMS for all devices on a day"

    for imei, value in distinct_sms.iteritems():
      for date, inner_list in value.iteritems():
        print "\n"
        print "Device ID: {}".format(imei)
        print "==============================================="

        distinct_msg = set(inner_list)
        distinct_msg = list(distinct_msg)
        distinct_msg.sort()

        try:
          files_csv[imei]
        except KeyError:
          files_csv[imei] = {}
        

        j = len(inner_list)

        if date>'2015-02-11' and date<'2015-04-27': 
            try:
              files_csv[imei][date]
            except KeyError:
              files_csv[imei][date] = {}
              files_csv[imei][date]["Total Duration"] = 0
              files_csv[imei][date]["Distinct Calls"] = 0
              files_csv[imei][date]["Num of calls"] = 0
              files_csv[imei][date]["Distinct Locations"] = 0

            files_csv[imei][date]["Number of SMS"] = j

            print "Number of sms on {} is {}".format(imei,j)
           
            j = len(distinct_msg)
            files_csv[imei][date]["Distinct users SMS"] = j

            print "Number of distinct sms called on {} is {}".format(date,j)


    ########################################
     
    # Discretionary sms stats

    for imei, value in new_contacts_sms_disc.iteritems():
        new_contacts_sms_disc[imei] = second_contacts_sms_disc[imei] - first_contacts_sms_disc[imei]

        for call in new_contacts_sms_disc[imei]:
            if imei in shannons_entropy_sms_disc:
                shannons_entropy_sms_disc[imei][call] = shannons_entropy_sms[imei][call]
            else:
                shannons_entropy_sms_disc[imei] = {}
                shannons_entropy_sms_disc[imei][call] = shannons_entropy_sms[imei][call]

    #print shannons_entropy_sms_disc
    #exit()
    #######################################




    cur = con.cursor()
    times_list = []
    devices = []

    distinct_locations = {}

    cur = con.cursor()
    cur.execute("PRAGMA temp_store = 2")
    data = cur.execute("""select device, value, timestamp from data
                          where probe like '%Location%'""")

    old_str = 0 

    print "LOCATION DATA"
    for x in data:
        temp1 = x[1].find('"mTime"')
        if temp1 > 0:
          temp1 += 8
          try:
            new_str = int(x[1][temp1:temp1+13])/1000
            std_mtime = time.strftime('%Y-%m-%d', time.localtime(new_str))
          except ValueError:
            print "UNIQUE STRING"
            std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[2]))
        else:
          new_str = x[2];
          std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[2]))
        
        record_time = int(x[2])
        long_index = x[1].find('"mLongitude"')
        last_index = x[1].find(',', long_index)
        cordinate = x[1][long_index+13:last_index]
        fvalue_cordiante = float(cordinate)
        long_int = float("{0:.4f}".format(fvalue_cordiante))
        
        lat_index = x[1].find('"mLatitude"')
        last_index = x[1].find(',', lat_index)
        cordinate = x[1][lat_index+12:last_index]
        fvalue_cordiante = float(cordinate)
        lat_int = float("{0:.4f}".format(fvalue_cordiante))
        
        location_str = str(long_int) + " " + str(lat_int)
       
        try:
          distinct_locations[x[0]]
        except KeyError:
          distinct_locations[x[0]] = defaultdict(list)
        distinct_locations[x[0]][std_mtime].append(location_str)
        
        # For shannon's entropy calculation
        location_bin = str(long_int) + " " + str(lat_int)
        # Location verbose and intervals
        time_long_lat_list = [time.strftime('%H:%M', time.localtime(new_str)), str(long_int), str(lat_int), new_str]
        print time_long_lat_list
        
        if record_time>start_time and record_time<end_time:
          try:
            device_imei[x[0]]
          except KeyError:
            device_imei[x[0]] = x[0]

          print "imei: "+str(device_imei[x[0]])+"Date: "+str(std_mtime)
          try:
            shannons_location[device_imei[x[0]]]
          except KeyError:
            shannons_location[device_imei[x[0]]] = {}
          
          try:
            shannons_location[device_imei[x[0]]][location_bin] += 1
          except KeyError:
            shannons_location[device_imei[x[0]]][location_bin] = 1

        print "Entry made"
        try:
          device_imei[x[0]]
        except KeyError:
          device_imei[x[0]] = x[0]
        try:
          location_stats[device_imei[x[0]]]
        except KeyError:
          location_stats[device_imei[x[0]]] = {}
        
        try:
          location_stats[device_imei[x[0]]][std_mtime]
        except KeyError:
          location_stats[device_imei[x[0]]][std_mtime] = []

        location_stats[device_imei[x[0]]][std_mtime].append(time_long_lat_list)

    ########################################

    # Code to get only the first location of an hour.
    
    list_of_lists = []
    location_stats_interval_copy = location_stats_interval
    first_lat_long_hour = {}

    for imei, date in location_stats.iteritems():
      for date_key, time_long_lat in date.iteritems():
        for element in time_long_lat:
          time_hour = element[0][:element[0].index(":")]
          print(time_hour)
          record_time = int(element[3])
          plus_one = int(time_hour)+1
          if plus_one<10:
            plus_one = "0"+str(plus_one)
          else:
            plus_one = str(plus_one)
          interval_string = time_hour+":00-"+str(plus_one)+":00"
          print interval_string
          try:
            if location_stats_interval_copy[imei][date_key][interval_string] == 0 and int(element[3])>start_time and int(element[3])<end_time:
              location_stats_interval_copy[imei][date_key][interval_string]+=1
              ref = imei+date_key+interval_string
              location_bin = str(element[1]) + " " + str(element[2])
            
              try:
                shannons_location_update[imei]
                shannons_location_discretionary[imei]
                day_night_location_ratio[imei]
              except KeyError:
                shannons_location_update[imei] = {}
                shannons_location_discretionary[imei] = {}
                day_night_location_ratio[imei] = {}
                first_location[imei] = set()
                second_location[imei] = set()
                location_list[imei] = []
                first_location_discretionary[imei] = set()
                second_location_discretionary[imei] = set()
                new_location_discretionary[imei] = set()
                new_location[imei] = set()
              
              lng = float("{0:.2f}".format(float(element[1])))
              lat = float("{0:.2f}".format(float(element[2])))
              lng3 = float("{0:.3f}".format(float(element[1])))
              lat3 = float("{0:.3f}".format(float(element[2])))
              location_bin3 = str(lng3) + " " + str(lat3)
              # Get the first two weeks contact, comparison date is 3-20-2015
              if record_time<1426824000:
                first_location[imei].add(str(lng)+" "+str(lat))
              else:
                second_location[imei].add(str(lng)+" "+str(lat))
                
              location_list[imei].append(str(lng)+" "+str(lat))
              # Avoid the first two weeks data. Divide the remaining data into two. 
              # Remove data before 2-26-2015 and split data at 3-26-2015
              if record_time>1425010915 and record_time<1427342400:
                first_location_discretionary[imei].add(str(lng3)+" "+str(lat3))
              elif record_time>1427342400:
                second_location_discretionary[imei].add(str(lng3)+" "+str(lat3))
                
              try:
                shannons_location_update[imei][location_bin] += 1
                shannons_location_discretionary[imei][location_bin3] += 1
              except KeyError:
                shannons_location_update[imei][location_bin] = 1
                shannons_location_discretionary[imei][location_bin3] = 1

              if element[0]>"18:00" and element[0]<"6:00":
                try:
                  day_night_location_ratio[imei]["NIGHT"]
                except:
                  day_night_location_ratio[imei]["NIGHT"] = {}
                try:
                  day_night_location_ratio[imei]["NIGHT"][location_bin] += 1
                except KeyError:
                  day_night_location_ratio[imei]["NIGHT"][location_bin] = 1
              else:
                try:
                  day_night_location_ratio[imei]["DAY"]
                except:
                  day_night_location_ratio[imei]["DAY"] = {}
                try:
                  day_night_location_ratio[imei]["DAY"][location_bin] += 1
                except KeyError:
                  day_night_location_ratio[imei]["DAY"][location_bin] = 1
              first_lat_long_hour[ref] = [element[1], element[2]]
          except KeyError:
            print "KeyError in location interval"
            print imei

    print "OUPUT!!!!!"

    list_of_lists = []
    for imei, date_1 in location_stats_interval_copy.iteritems():
      for date_key, interval in date_1.iteritems():
        for time_slot, freq in interval.iteritems():
          if freq>0:
            ref = imei+date_key+time_slot
            row = [imei, date_key, time_slot, freq, first_lat_long_hour[ref][0], first_lat_long_hour[ref][1]]
          else:
            row = [imei, date_key, time_slot, freq, 0, 0]
          #print row
          list_of_lists.append(row)

    list_of_lists.sort(key=lambda x:(x[0], x[1], x[2]))

    if len(file) > 0:
      resultFile = open(file + "_location_interval_first_location.csv",'wb')
    else:  
      resultFile = open("output_location_interval_first_location.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Day','Time_slot','Frequency','Longitude','Latitude'])
    for x in list_of_lists:
      row = [x[0],x[1],x[2],x[3],x[4],x[5]]
      wr.writerow(row)

    resultFile.close()

    ########################################
     
    # Discretionary travel stats

    for imei, value in new_location_discretionary.iteritems():
        new_location_discretionary[imei] = second_location_discretionary[imei] - first_location_discretionary[imei]

        for location in new_location_discretionary[imei]:
            if imei in shannons_location_discretionary3:
                shannons_location_discretionary3[imei][location] = shannons_location_discretionary[imei][location]
            else:
                shannons_location_discretionary3[imei] = {}
                shannons_location_discretionary3[imei][location] = shannons_location_discretionary[imei][location]

        # add a new shannons dict for 2/3 precision values... :)

    #print shannons_location_discretionary3
    #######################################


    for imei, value in distinct_locations.iteritems():
      for date, inner_list in value.iteritems():
        print "\n"
        print "Device ID: {}".format(imei)
        print "==============================================="


        print "Value = {}".format(inner_list)
        distinct_loc = set(inner_list)
        distinct_loc = list(distinct_loc)
        distinct_loc.sort()

        j = len(distinct_loc)   

        try:
          files_csv[imei]
        except KeyError:
          files_csv[imei] = {}
        
        if date>'2015-02-11' and date<'2015-04-27': 
            try:
              files_csv[imei][date]
            except KeyError:
              files_csv[imei][date] = {}
              files_csv[imei][date]["Number of SMS"] = 0
              files_csv[imei][date]["Total Duration"] = 0
              files_csv[imei][date]["Distinct Calls"] = 0
              files_csv[imei][date]["Num of calls"] = 0
              files_csv[imei][date]["Distinct users SMS"] = 0

            files_csv[imei][date]["Distinct Locations"] = j
            print "Distinct locations on {} is {}".format(date,j)
   

    ar_coefficient()
    #print ar_values

        
    if len(file) > 0:
      resultFile = open(file + "ar_values.csv",'wb')
    else:  
      resultFile = open("ar_values.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','v1','v2','v3','v4','v5','v6','v7','v8','v9','v10','v11','v12','v13','v14','v15','v16','v17','v18','v19','v20','v21','v22','v23','v24','v25','v26','v27','v28','v29','v30','v31'])
    for imei,lis in ar_values.iteritems():
      x = []
      x.append(imei)
      for d in lis:
        x.append(d)
      wr.writerow(x)

    resultFile.close()
    #exit()
    #### Code to add the first call log date and number of days of good call

    cur = con.cursor()
    cur.execute("PRAGMA temp_store = 2")


    data = cur.execute("""select imei,timestamp from data
                          where probe like '%Call%'""")

    for d in data:
      imei_num_timestamp[d[0]] = []

    cur = con.cursor()
    data = cur.execute("""select imei,timestamp from data
                          where probe like '%Call%'""")

    for d in data:
      if time.strftime('%Y-%m-%d', time.localtime(d[1])) > '2015-02-11' and time.strftime('%Y-%m-%d', time.localtime(d[1])) < '2015-04-27':
        imei_num_timestamp[d[0]].append(time.strftime('%Y-%m-%d', time.localtime(d[1])))

    #print imei_num_timestamp
    for imei,timestamp in imei_num_timestamp.iteritems():
      imei_num_timestamp[imei] = len(set(imei_num_timestamp[imei]))

    cur = con.cursor()
    data = cur.execute("""select imei,timestamp from data
                          where probe like '%Call%'""")

    for d in data:
      print d
      imei_timestamp[d[0]] = 222222222222222

    cur = con.cursor()
    data = cur.execute("""select imei,timestamp from data
                          where probe like '%Call%'""")
    for d in data:
      #print "dfdfd"
      if imei_timestamp[d[0]]>d[1]:
        imei_timestamp[d[0]] = d[1]

    for imei,timestamp in imei_timestamp.iteritems():
      imei_timestamp[imei] = time.strftime('%Y-%m-%d', time.localtime(timestamp))

    cur = con.cursor()
    data = cur.execute("""select imei,timestamp from data
                          where probe like '%Sms%'""")

    for d in data:
      imei_num_smstimestamp[d[0]] = []

    cur = con.cursor()
    data = cur.execute("""select imei,timestamp from data
                          where probe like '%Sms%'""")

    for d in data:
      if time.strftime('%Y-%m-%d', time.localtime(d[1])) > '2015-02-11' and time.strftime('%Y-%m-%d', time.localtime(d[1])) < '2015-04-27':
        imei_num_smstimestamp[d[0]].append(time.strftime('%Y-%m-%d', time.localtime(d[1])))

    #print imei_num_timestamp
    for imei,timestamp in imei_num_smstimestamp.iteritems():
      imei_num_smstimestamp[imei] = len(set(imei_num_smstimestamp[imei]))

    cur = con.cursor()
    data = cur.execute("""select imei,timestamp from data
                          where probe like '%Sms%'""")

    for d in data:
      print d
      imei_smstimestamp[d[0]] = 222222222222222

    cur = con.cursor()
    data = cur.execute("""select imei,timestamp from data
                          where probe like '%Sms%'""")
    for d in data:
      #print "dfdfd"
      if imei_smstimestamp[d[0]]>d[1]:
        imei_smstimestamp[d[0]] = d[1]

    for imei,timestamp in imei_smstimestamp.iteritems():
      imei_smstimestamp[imei] = time.strftime('%Y-%m-%d', time.localtime(timestamp))
    
    for imei,date1 in imei_smstimestamp.iteritems():
      mdate = "2015-02-12"
      mdate1 = datetime.datetime.strptime(mdate, "%Y-%m-%d")

      #d = time.strftime('%Y-%m-%d', time.localtime(int(date1)))
      d = datetime.datetime.strptime(date1, "%Y-%m-%d")

      if d<mdate1:
        eligible_days_sms[imei] = 74
      else:
        rdate = "2015-04-27"
        rdate1 = datetime.datetime.strptime(rdate, "%Y-%m-%d")
        eligible_days_sms[imei] = (rdate1 - d).days

    for imei,date1 in imei_timestamp.iteritems():
      mdate = "2015-02-12"
      mdate1 = datetime.datetime.strptime(mdate, "%Y-%m-%d")

      #d = time.strftime('%Y-%m-%d', time.localtime(int(date1)))
      d = datetime.datetime.strptime(date1, "%Y-%m-%d")

      if d<mdate1:
        eligible_days_call[imei] = 74
      else:
        rdate = "2015-04-27"
        rdate1 = datetime.datetime.strptime(rdate, "%Y-%m-%d")
        eligible_days_call[imei] = (rdate1 - d).days
    #### Code Ends here

def create_csv():

    print device_imei
    location_entropy = {}
    call_entropy = {}
    sms_entropy = {}

    location_entropy_disc = {}
    call_entropy_disc = {}
    sms_entropy_disc = {}

    location_loyalty = {}
    call_loyalty = {}
    sms_loyalty = {}
    home_work_ratio = {}
    location_percentage_time_spent = {}
    
    location_loyalty_disc = {}
    call_loyalty_disc = {}
    sms_loyalty_disc = {}
    location_percentage_time_spent_disc = {}
    
    location_stie = {}
    call_stie = {}
    sms_stie = {}
    location_stie_value = {}
    call_stie_value = {}
    sms_stie_value = {}
    
    location_stie_disc = {}
    call_stie_disc = {}
    sms_stie_disc = {}
    location_wtie_disc = {}
    call_wtie_disc = {}
    sms_wtie_disc = {}
    
    location_wtie = {}
    call_wtie = {}
    sms_wtie = {}
    location_wtie_value = {}
    call_wtie_value = {}
    sms_wtie_value = {}
    
    location_count = {}
    location_unique_count = {}
    location_count_disc = {}
    location_unique_count_disc = {}
    call_count = {}
    sms_count = {}
    call_count_disc = {}
    sms_count_disc = {}
    day_night_ratio_location_value = {}
    day_night_ratio_call_value = {}
    day_night_ratio_sms_value = {}

    distinct_contacts_count = {}
    distinct_contacts_sms_count = {}
    number_of_strong_ties_location = {}
    number_of_strong_ties_call= {}
    number_of_strong_ties_sms = {}
    inout_ratio_call = {}
    inout_ratio_sms = {}
    response_rate = {}
    response_latency_value = {}
    response_rate_sms = {}
    response_latency_value_sms = {}
    missed_call_rate = {}
    weekday_weekend_ratio_location_value = {}
    weekday_weekend_ratio_call_value = {}
    weekday_weekend_ratio_sms_value = {}
    new_contacts_call_duration = {}
   
    gyradius_avg = {}
    ret_exp = {}
    gyradius_median = {}
    gyradiusk_median = {}
    top_3_locations = {}
    dist_above_100 = {}
    third_place_top_2 = {}
    third_place_top_3 = {}
    third_place_top_4 = {}
    third_place_top_5 = {}
    third_place_top_6 = {}
    table = []

    # Calculate the Shannon's Entropy 
    for imei, ndict in shannons_location_update.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      k = len(ndict)
      for location_bin, count in ndict.iteritems():
        p_x = float(count)/float(sum1)
        if p_x > 0 and k > 1:
          entropy += - p_x*math.log(p_x, k)
      location_entropy[imei] = entropy
    
    # Calculate the Shannon's Entropy 
    for imei, ndict in shannons_location_discretionary3.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      k = len(ndict)
      for location_bin, count in ndict.iteritems():
        p_x = float(count)/float(sum1)
        if p_x > 0 and k > 1:
          entropy += - p_x*math.log(p_x, k)
      location_entropy_disc[imei] = entropy
    
    print "SHANNONS ENTROPY FOR LOCATION" 
    #for imei, ndict in location_entropy.iteritems():
    #  print ndict

    for imei, ndict in shannons_entropy_stats.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      call_count[imei] = sum1
      k = len(ndict)
      for location_bin, count in ndict.iteritems():
        p_x = float(count)/float(sum1)
        if p_x > 0 and k > 1:
          entropy += - p_x*math.log(p_x, k)
      call_entropy[imei] = entropy

    print "SHANNONS ENTROPY FOR CALL" 
    #for imei, ndict in call_entropy.iteritems():
    #  print ndict
    
    for imei, ndict in shannons_entropy_sms.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      sms_count[imei] = sum1
      k = len(ndict)
      for location_bin, count in ndict.iteritems():
        p_x = float(count)/float(sum1)
        if p_x > 0 and k > 1:
          entropy += - p_x*math.log(p_x, k)
      sms_entropy[imei] = entropy

    for imei, ndict in shannons_entropy_stats_disc.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      call_count_disc[imei] = sum1
      k = len(ndict)
      for location_bin, count in ndict.iteritems():
        p_x = float(count)/float(sum1)
        if p_x > 0 and k > 1:
          entropy += - p_x*math.log(p_x, k)
      call_entropy_disc[imei] = entropy

    print "SHANNONS ENTROPY FOR CALL" 
    #for imei, ndict in call_entropy.iteritems():
    #  print ndict
    
    for imei, ndict in shannons_entropy_sms_disc.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      sms_count_disc[imei] = sum1
      k = len(ndict)
      for location_bin, count in ndict.iteritems():
        p_x = float(count)/float(sum1)
        if p_x > 0 and k > 1:
          entropy += - p_x*math.log(p_x, k)
      sms_entropy_disc[imei] = entropy

    #print "SHANNONS ENTROPY FOR SMS" 
    #for imei, ndict in sms_entropy.iteritems():
    #  print ndict
    
    # Calculate the Loyalty 
    for imei, ndict in shannons_location_update.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      unique_locations = list(ndict.keys())
      unique_locations_set = set(unique_locations)

      location_unique_count[imei] = len(unique_locations_set)
      location_count[imei] = sum1
      top_3 = dict(Counter(ndict).most_common(3))
      top_2 = dict(Counter(ndict).most_common(2))
      top_4 = dict(Counter(ndict).most_common(4))
      top_5 = dict(Counter(ndict).most_common(5))
      top_6 = dict(Counter(ndict).most_common(6))
      #print top_3
      top_3_locations[imei] = []
      for location_bin, count in top_3.iteritems():
        #top_3_locations[imei].add(location_bin)
        temp_list = [location_bin,]*count
        for d in temp_list:
                top_3_locations[imei].append(d)
        p_x = float(count)/sum1
        if p_x > 0:
          entropy += p_x
      location_loyalty[imei] = entropy
      
      top_k_count=min(top_2.itervalues())
      third_place_top_2[imei] = (float(top_k_count)/float(sum1))*100

      top_k_count=min(top_3.itervalues())
      third_place_top_3[imei] = (float(top_k_count)/float(sum1))*100

      top_k_count=min(top_4.itervalues())
      third_place_top_4[imei] = (float(top_k_count)/float(sum1))*100

      top_k_count=min(top_5.itervalues())
      third_place_top_5[imei] = (float(top_k_count)/float(sum1))*100

      top_k_count=min(top_6.itervalues())
      third_place_top_6[imei] = (float(top_k_count)/float(sum1))*100

    # Calculate the Loyalty DISC
    for imei, ndict in shannons_location_discretionary3.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      unique_locations = list(ndict.keys())
      unique_locations_set = set(unique_locations)

      location_unique_count_disc[imei] = len(unique_locations_set)
      location_count_disc[imei] = sum1
        
      top_3 = dict(Counter(ndict).most_common(3))
      print "HERE"  
      #top_3_locations[imei] = []
      for location_bin, count in top_3.iteritems():
        #top_3_locations[imei].append([location_bin,]*count)
        p_x = float(count)/sum1
        print count
        print sum1
        if p_x > 0:
          entropy += p_x
      location_loyalty_disc[imei] = entropy

    # Calculate the Percentage of the time spent 
    for imei, ndict in shannons_location_update.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      top_1 = dict(Counter(ndict).most_common(1))
      #print top_3
      for location_bin, count in top_1.iteritems():
        percent = (float(count)/sum1)*100
        
      location_percentage_time_spent[imei] = float("{0:.2f}".format(percent))
    
    # Calculate the Percentage of the time spent DISC 
    for imei, ndict in shannons_location_discretionary3.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      top_1 = dict(Counter(ndict).most_common(1))
      #print top_3
      for location_bin, count in top_1.iteritems():
        percent = (float(count)/sum1)*100
        
      location_percentage_time_spent_disc[imei] = float("{0:.2f}".format(percent))
    
    # Calculate the Day/night location ratio
    day_count = 0
    night_count = 0
    print day_night_location_ratio
    for imei, time_of_day in day_night_location_ratio.iteritems():
      for time, location_string in time_of_day.iteritems():
        if time=="DAY":
          day_count = len(location_string)
        else:
          night_count = len(location_string)
      try:  
        day_night_ratio_location_value[imei]= float("{0:.2f}".format(float(day_count)/float(night_count)))
      except ZeroDivisionError:
        day_night_ratio_location_value[imei] = "NA"

    #print "LOYALTY FOR LOCATION" 
    #for imei, ndict in location_loyalty.iteritems():
    #  print ndict

    for imei, ndict in shannons_entropy_stats.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      top_3 = dict(Counter(ndict).most_common(3))
      #print top_3
      for location_bin, count in top_3.iteritems():
        p_x = float(count)/sum1
        if p_x > 0:
          entropy += p_x
      call_loyalty[imei] = entropy
      #calculate the number of contacts here
      distinct_contacts_count[imei] = len(set(distinct_contacts[imei]))
      print max(((k, sum(1 for i in g)) for k, g in groupby(sorted(distinct_contacts[imei]))), key=itemgetter(1))
      inout_ratio_call[imei] = (float(incoming_call_count[imei])/outgoing_call_count[imei])*100
      response_rate[imei] =  float("{0:.2f}".format((float(response_rate_count[imei])/float(missed_call_count[imei]))*100))
      response_latency_value[imei] =  float("{0:.2f}".format((float(response_latency[imei])/float(response_rate_count[imei]))))
      missed_call_rate[imei] = (float(missed_call_count[imei])/call_count[imei])*100
      day_night_ratio_call_value[imei]= float("{0:.2f}".format(float(day_night_call_ratio[imei]["DAY"])/float(day_night_call_ratio[imei]["NIGHT"])))
      try:
        weekday_weekend_ratio_call_value[imei]= float("{0:.2f}".format(float(weekday_weekend_call_ratio[imei]["weekday"])/float(weekday_weekend_call_ratio[imei]["weekend"])))
      except KeyError:
        print "CATCH ERROR"
    #print "LOYALTY FOR CALL" 
    #for imei, ndict in call_loyalty.iteritems():
    #  print ndict
   

    # Call loyalty DISC
    for imei, ndict in shannons_entropy_stats_disc.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      top_3 = dict(Counter(ndict).most_common(3))
      #print top_3
      for location_bin, count in top_3.iteritems():
        p_x = float(count)/sum1
        if p_x > 0:
          entropy += p_x
      call_loyalty_disc[imei] = entropy


    for imei, ndict in shannons_entropy_sms.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      top_3 = dict(Counter(ndict).most_common(3))
      #print top_3
      for location_bin, count in top_3.iteritems():
        p_x = float(count)/sum1
        if p_x > 0:
          entropy += p_x
      sms_loyalty[imei] = entropy
      distinct_contacts_sms_count[imei] = len(set(distinct_contacts_sms[imei]))
      if imei in response_rate_sms:
        response_rate_sms[imei] =  float("{0:.2f}".format((float(response_rate_count_sms[imei])/float(incoming_sms_count[imei]))*100))
        response_latency_value_sms[imei] =  float("{0:.2f}".format((float(response_latency_sms[imei])/float(response_rate_count_sms[imei]))))
      else:
        print "No response rate for this IMEI"
        print imei
      try:  
        inout_ratio_sms[imei] = (float(incoming_sms_count[imei])/outgoing_sms_count[imei])*100
      except KeyError:
        pass
      try:  
        day_night_ratio_sms_value[imei]= float("{0:.2f}".format(float(day_night_sms_ratio[imei]["DAY"])/float(day_night_sms_ratio[imei]["NIGHT"])))
      except KeyError:
        day_night_ratio_sms_value[imei]= "NA"
      try:  
        weekday_weekend_ratio_sms_value[imei]= float("{0:.2f}".format(float(weekday_weekend_sms_ratio[imei]["weekday"])/float(weekday_weekend_sms_ratio[imei]["weekend"])))
      except KeyError:
        print "CATCH ERROR"
    #print "LOYALTY FOR SMS" 
    #for imei, ndict in sms_loyalty.iteritems():
    #  print ndict

    # Sms loyalty DISC
    for imei, ndict in shannons_entropy_sms_disc.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      top_3 = dict(Counter(ndict).most_common(3))
      #print top_3
      for location_bin, count in top_3.iteritems():
        p_x = float(count)/sum1
        if p_x > 0:
          entropy += p_x
      sms_loyalty_disc[imei] = entropy

    # Strong weak ties, DISC
    print "Strong ties for locations"
    for imei, ndict in shannons_location_discretionary3.iteritems():
      strong_tie = 0
      last = 0
      strong_tie_num = float(len(ndict))/3
      sum1 = sum(ndict.values())
      fraction = strong_tie_num - int(strong_tie_num)
      
      if fraction > 0:
        pick_k = int(strong_tie_num) + 1
      else:
        pick_k = int(strong_tie_num)

      #number_of_strong_ties_location[imei] = pick_k
      
      #top_2 = dict(Counter(ndict).most_common(2))
      #value = 0;
      #top_2_list = top_2.values()
      #print top_2_list
      #try:
      #  if top_2_list[0]>top_2_list[1]:
      #    home_work_ratio[imei] = float("{0:.2f}".format(float(top_2_list[0])/float(top_2_list[1])))
      #  else:
      #    home_work_ratio[imei] = float("{0:.2f}".format(float(top_2_list[1])/float(top_2_list[0])))
      #except:
      #   home_work_ratio[imei] = "NA"
      top_k = dict(Counter(ndict).most_common(pick_k))
      bottom_k = sorted(ndict.values())[:pick_k]
      for location_bin, count in top_k.iteritems():
        strong_tie += count
        last = count
      
      #print "Fraction" 
      #print fraction 
      if fraction > 0:
        strong_tie -= last
        strong_tie += float(last)*fraction

      #location_stie_value[imei] = strong_tie
      #print "Strong tie value above"+str(strong_tie)
      strong_tie = (strong_tie/float(sum1)) * 100
      #print "Strong tie"+str(strong_tie)

      location_stie_disc[imei] = strong_tie

      weak_tie = sum(bottom_k)
      if fraction > 0:
        weak_tie -= bottom_k[pick_k-1]
        weak_tie += float(bottom_k[pick_k-1])*fraction

      #location_wtie_value[imei] = weak_tie
      weak_tie = (weak_tie/float(sum1)) * 100

      location_wtie_disc[imei] = weak_tie


    #print "STRONG FOR LOCATION" 
    #for imei, ndict in location_stie.iteritems():
    #  print ndict

    for imei, ndict in shannons_entropy_stats_disc.iteritems():
      strong_tie = 0
      last = 0
      strong_tie_num = float(len(ndict))/3
      sum1 = sum(ndict.values())
      print strong_tie_num
      fraction = strong_tie_num - int(strong_tie_num)
      
      if fraction > 0:
        pick_k = int(strong_tie_num) + 1
      else:
        pick_k = int(strong_tie_num)

      #print pick_k

      #number_of_strong_ties_call[imei] = pick_k

      top_k = dict(Counter(ndict).most_common(pick_k))
      bottom_k = sorted(ndict.values())[:pick_k]
      #print top_3
      for location_bin, count in top_k.iteritems():
        strong_tie += count
        last = count
      
      #print "Fraction" 
      #print fraction 
      if fraction > 0:
        strong_tie -= last
        strong_tie += float(last)*fraction

      #call_stie_value[imei] = strong_tie
      strong_tie = (strong_tie/float(sum1)) * 100
      #print strong_tie

      call_stie_disc[imei] = strong_tie
      weak_tie = sum(bottom_k)
      if fraction > 0:
        weak_tie -= bottom_k[pick_k-1]
        weak_tie += float(bottom_k[pick_k-1])*fraction

      #call_wtie_value[imei] = weak_tie
      weak_tie = (weak_tie/float(sum1)) * 100
      #print weak_tie

      call_wtie_disc[imei] = weak_tie

    #print "STRONG FOR CALLS" 
    #for imei, ndict in call_stie.iteritems():
    #  print ndict


    for imei, ndict in shannons_entropy_sms_disc.iteritems():
      strong_tie = 0
      last = 0
      strong_tie_num = float(len(ndict))/3
      sum1 = sum(ndict.values())
      print strong_tie_num
      fraction = strong_tie_num - int(strong_tie_num)
      
      if fraction > 0:
        pick_k = int(strong_tie_num) + 1
      else:
        pick_k = int(strong_tie_num)
      
      #number_of_strong_ties_sms[imei] = pick_k

      #print pick_k
      top_k = dict(Counter(ndict).most_common(pick_k))
      bottom_k = sorted(ndict.values())[:pick_k]
      #print top_3
      for location_bin, count in top_k.iteritems():
        strong_tie += count
        last = count
      
      #print "Fraction" 
      #print fraction 
      if fraction > 0:
        strong_tie -= last
        strong_tie += float(last)*fraction

      #sms_stie_value[imei] = strong_tie
      strong_tie = (strong_tie/float(sum1)) * 100
      #print strong_tie

      sms_stie_disc[imei] = strong_tie

      weak_tie = sum(bottom_k)
      if fraction > 0:
        weak_tie -= bottom_k[pick_k-1]
        weak_tie += float(bottom_k[pick_k-1])*fraction

      #sms_wtie_value[imei] = weak_tie
      weak_tie = (weak_tie/float(sum1)) * 100
      #print weak_tie

      sms_wtie_disc[imei] = weak_tie
    #print "STRONG FOR LOCATION" 
    #for imei, ndict in sms_stie.iteritems():
    #  print ndict



    #print "Strong ties for locations"
    for imei, ndict in shannons_location_update.iteritems():
      strong_tie = 0
      last = 0
      strong_tie_num = float(len(ndict))/3
      sum1 = sum(ndict.values())
      fraction = strong_tie_num - int(strong_tie_num)
      
      if fraction > 0:
        pick_k = int(strong_tie_num) + 1
      else:
        pick_k = int(strong_tie_num)

      number_of_strong_ties_location[imei] = pick_k
      
      top_2 = dict(Counter(ndict).most_common(2))
      value = 0;
      top_2_list = top_2.values()
      print top_2_list
      try:
        if top_2_list[0]>top_2_list[1]:
          home_work_ratio[imei] = float("{0:.2f}".format(float(top_2_list[0])/float(top_2_list[1])))
        else:
          home_work_ratio[imei] = float("{0:.2f}".format(float(top_2_list[1])/float(top_2_list[0])))
      except:
         home_work_ratio[imei] = "NA"
      top_k = dict(Counter(ndict).most_common(pick_k))
      bottom_k = sorted(ndict.values())[:pick_k]
      for location_bin, count in top_k.iteritems():
        strong_tie += count
        last = count
      
      #print "Fraction" 
      #print fraction 
      if fraction > 0:
        strong_tie -= last
        strong_tie += float(last)*fraction

      location_stie_value[imei] = strong_tie
      #print "Strong tie value above"+str(strong_tie)
      strong_tie = (strong_tie/float(sum1)) * 100
      #print "Strong tie"+str(strong_tie)

      location_stie[imei] = strong_tie

      weak_tie = sum(bottom_k)
      if fraction > 0:
        weak_tie -= bottom_k[pick_k-1]
        weak_tie += float(bottom_k[pick_k-1])*fraction

      location_wtie_value[imei] = weak_tie
      weak_tie = (weak_tie/float(sum1)) * 100

      location_wtie[imei] = weak_tie


    #print "STRONG FOR LOCATION" 
    #for imei, ndict in location_stie.iteritems():
    #  print ndict

    for imei, ndict in shannons_entropy_stats.iteritems():
      strong_tie = 0
      last = 0
      strong_tie_num = float(len(ndict))/3
      sum1 = sum(ndict.values())
      print strong_tie_num
      fraction = strong_tie_num - int(strong_tie_num)
      
      if fraction > 0:
        pick_k = int(strong_tie_num) + 1
      else:
        pick_k = int(strong_tie_num)

      print pick_k

      number_of_strong_ties_call[imei] = pick_k

      top_k = dict(Counter(ndict).most_common(pick_k))
      bottom_k = sorted(ndict.values())[:pick_k]
      #print top_3
      for location_bin, count in top_k.iteritems():
        strong_tie += count
        last = count
      
      #print "Fraction" 
      #print fraction 
      if fraction > 0:
        strong_tie -= last
        strong_tie += float(last)*fraction

      call_stie_value[imei] = strong_tie
      strong_tie = (strong_tie/float(sum1)) * 100
      #print strong_tie

      call_stie[imei] = strong_tie
      weak_tie = sum(bottom_k)
      if fraction > 0:
        weak_tie -= bottom_k[pick_k-1]
        weak_tie += float(bottom_k[pick_k-1])*fraction

      call_wtie_value[imei] = weak_tie
      weak_tie = (weak_tie/float(sum1)) * 100
      #print weak_tie

      call_wtie[imei] = weak_tie

    #print "STRONG FOR CALLS" 
    #for imei, ndict in call_stie.iteritems():
    #  print ndict


    for imei, ndict in shannons_entropy_sms.iteritems():
      strong_tie = 0
      last = 0
      strong_tie_num = float(len(ndict))/3
      sum1 = sum(ndict.values())
      print strong_tie_num
      fraction = strong_tie_num - int(strong_tie_num)
      
      if fraction > 0:
        pick_k = int(strong_tie_num) + 1
      else:
        pick_k = int(strong_tie_num)
      
      number_of_strong_ties_sms[imei] = pick_k

      print pick_k
      top_k = dict(Counter(ndict).most_common(pick_k))
      bottom_k = sorted(ndict.values())[:pick_k]
      #print top_3
      for location_bin, count in top_k.iteritems():
        strong_tie += count
        last = count
      
      #print "Fraction" 
      #print fraction 
      if fraction > 0:
        strong_tie -= last
        strong_tie += float(last)*fraction

      sms_stie_value[imei] = strong_tie
      strong_tie = (strong_tie/float(sum1)) * 100
      print strong_tie

      sms_stie[imei] = strong_tie

      weak_tie = sum(bottom_k)
      if fraction > 0:
        weak_tie -= bottom_k[pick_k-1]
        weak_tie += float(bottom_k[pick_k-1])*fraction

      sms_wtie_value[imei] = weak_tie
      weak_tie = (weak_tie/float(sum1)) * 100
      print weak_tie

      sms_wtie[imei] = weak_tie



    if len(file) > 0:
      resultFile = open(file + "_entropy.csv",'wb')
    else:  
      resultFile = open("output_entropy.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Call Count','Sms Count','Location Count','Unique location count','Day night location ratio','Percentage of time spent in a location','Number of new locations','First Call log date','Days with call data','Number of days in which call data is spread','Number of calls per day','Distinct users call','Distinct users called outgoing','Total Call duration','In-Out ratio','Call Response rate','Call Response Latency','Missed call percentage','Number of new contacts call','Time spend with new contacts','Percentage of time spend with new contacts','Number of new contacts in call for outgoing calls','Longest call Percentage','First Sms log date','Days with Sms data','Number of days in which sms data is spread','Number of Sms per day','Distinct users sms','Inout ratio SMS','Number of new contacts SMS','Call Entropy','Sms Entropy','Location Entropy','Call Loyalty','Sms Loyalty','Location Loyalty','Call Strong ties','Sms Strong ties','Location Strong ties','Call Weak ties','Sms Weak ties','Location Weak ties','Call Strong ties value','Sms Strong ties value','Location Strong ties value','Call Weak ties value','Sms Weak ties value','Location Weak ties value','Call S/W ties ratio','Sms S/W ties ratio','Location S/W ties ratio','Home Work Ratio','Day night Call ratio','Day night sms ratio','Weekday weekend call ratio','Weekday weekend SMS ratio','Gyradius Average','Gyradius median','Returner(1) vs Explorer(0)','Discretionary vs All','Distance above 100 kms percentage','Third place:k=2','Third place:k=3','Third place:k=4','Third place:k=5','Third place:k=6','AR cyclicity 1','AR cyclicity 7','AR cyclicity 31'])
    imei_list = list(set(device_imei.values()))
    for imei in imei_list:
      row_list = []
      row_list.append(imei)
      if imei.find("androidId")>0:
        continue


      
      if options.ignore == None and imei in new_contacts_call:
          new_contacts_call[imei] = second_contacts_call[imei] - first_contacts_call[imei]
          if imei in new_contacts_call_outgoing:
            if imei in second_contacts_call_outgoing:
                if imei in first_contacts_call_outgoing:
                    new_contacts_call_outgoing[imei] = second_contacts_call_outgoing[imei] - first_contacts_call_outgoing[imei]
          else:
            if imei in second_contacts_call_outgoing:
                if imei in first_contacts_call_outgoing:
                    new_contacts_call_outgoing[imei] = set()
                    new_contacts_call_outgoing[imei] = second_contacts_call_outgoing[imei] - first_contacts_call_outgoing[imei]

          # find the new contact durations, add all the new contact durations

          new_contacts_call_duration[imei] = 0
          for d in new_contacts_call[imei]:
            new_contacts_call_duration[imei] += duration_call[imei][d]

          new_contacts_sms[imei] = second_contacts_sms[imei] - first_contacts_sms[imei]
          if imei in second_location:
            if imei in first_location:
                new_location[imei] = second_location[imei] - first_location[imei]
                new_location_discretionary[imei] = second_location_discretionary[imei] - first_location_discretionary[imei]
            if imei in location_list:
                gyradius_avg[imei] = get_gyradius(location_list[imei],1)
                gyradius_median[imei] = get_gyradius(location_list[imei],0)
                gyradiusk_median[imei] = get_gyradius(top_3_locations[imei],0)
                dist_above_100[imei] = get_distance_above_100_percent(location_list[imei])
                if gyradius_median[imei] != 0:
                        ret_exp[imei] = float(gyradiusk_median[imei])/float(gyradius_median[imei])
    
      try:
        row_list.append(call_count[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(sms_count[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(location_count[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(location_unique_count[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(day_night_ratio_location_value[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(location_percentage_time_spent[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(len(new_location[imei]))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(imei_timestamp[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(imei_num_timestamp[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(eligible_days_call[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(int(call_count[imei]/eligible_days_call[imei]))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(distinct_contacts_count[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(distinct_calls_outgoing[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(call_duration_total[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(inout_ratio_call[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(response_rate[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(response_latency_value[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(missed_call_rate[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(len(new_contacts_call[imei]))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(new_contacts_call_duration[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format((float(new_contacts_call_duration[imei])/float(call_duration_total[imei]))*100)))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(len(new_contacts_call_outgoing[imei]))
      except KeyError:
        row_list.append("NA")
      try:
        print longest_call[imei]
        print call_duration_total[imei]
        print float("{0:.2f}".format((float(longest_call[imei])/float(call_duration_total[imei]))*100))
        row_list.append(float("{0:.2f}".format((float(longest_call[imei])/float(call_duration_total[imei]))*100)))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(imei_smstimestamp[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(imei_num_smstimestamp[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(eligible_days_sms[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(int(sms_count[imei]/eligible_days_sms[imei]))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(distinct_contacts_sms_count[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(inout_ratio_sms[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(len(new_contacts_sms[imei]))
      except KeyError:
        row_list.append("NA")
      try:
        print "^^^^^^"
        rd_2 = call_entropy[imei] * 100
        print rd_2
        rd_2 = int(round(rd_2))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = sms_entropy[imei] * 100
        print rd_2
        rd_2 = int(round(rd_2))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = location_entropy[imei] * 100
        print rd_2
        rd_2 = int(round(rd_2))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = call_loyalty[imei] * 100
        print rd_2
        rd_2 = int(round(rd_2))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = sms_loyalty[imei] * 100
        print rd_2
        rd_2 = int(round(rd_2))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = location_loyalty[imei] * 100
        print rd_2
        rd_2 = int(round(rd_2))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        print call_stie[imei]
        rd_2 = int(round(call_stie[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        print sms_stie[imei]
        rd_2 = int(round(sms_stie[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        print location_stie[imei]
        rd_2 = int(round(location_stie[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        print call_wtie[imei]
        rd_2 = int(round(call_wtie[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        #rd_2 = float("{0:.2f}".format(sms_wtie[imei]))
        #rd_2 = int(rd_2)
        #rd_2 = sms_wtie[imei]
        print sms_wtie[imei]
        rd_2 = int(round(sms_wtie[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        #rd_2 = float("{0:.2f}".format(location_wtie[imei]))
        #rd_2 = int(rd_2)
        print location_wtie[imei]
        rd_2 = int(round(location_wtie[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = int(round(call_stie_value[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = int(round(sms_stie_value[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = int(round(location_stie_value[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = int(round(call_wtie_value[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = int(round(sms_wtie_value[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = int(round(location_wtie_value[imei]))
        row_list.append(rd_2)
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = int(round(call_stie[imei]))
        rd_3 = int(round(call_wtie[imei]))
        row_list.append(float("{0:.2f}".format((float(rd_2)/float(rd_3)))))
      except KeyError:
        row_list.append("NA")
      try:
        rd_2 = int(round(sms_stie[imei]))
        rd_3 = int(round(sms_wtie[imei]))
        row_list.append(float("{0:.2f}".format((float(rd_2)/float(rd_3)))))
      except:
        row_list.append("NA")
      try:
        rd_2 = int(round(location_stie[imei]))
        rd_3 = int(round(location_wtie[imei]))
        row_list.append(float("{0:.2f}".format((float(rd_2)/float(rd_3)))))
      except:
        row_list.append("NA")
      try:
        row_list.append(home_work_ratio[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(day_night_ratio_call_value[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(day_night_ratio_sms_value[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(weekday_weekend_ratio_call_value[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(weekday_weekend_ratio_sms_value[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(gyradius_avg[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(gyradius_median[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(ret_exp[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format((float(len(new_location_discretionary[imei]))/float(len(first_location_discretionary[imei].union(second_location_discretionary[imei]))))))*100)
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(dist_above_100[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(third_place_top_2[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(third_place_top_3[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(third_place_top_4[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(third_place_top_5[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(third_place_top_6[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(ar_values[imei][0])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(ar_values[imei][6])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(float("{0:.2f}".format(ar_values[imei][30])))
      except KeyError:
        row_list.append("NA")
          
      wr.writerow(row_list)

    resultFile.close()

    if len(file) > 0:
      resultFile = open(file + "_imei_list.csv",'wb')
    else:  
      resultFile = open("output_imei_list.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    imei_list = list(set(device_imei.values()))
    for imei in imei_list:
      row_list = []
      row_list.append(imei)
      wr.writerow(row_list)

    resultFile.close()

    #################################################3

    # Dicretionary csv file


    if len(file) > 0:
      resultFile = open(file + "_entropy_disc.csv",'wb')
    else:  
      resultFile = open("output_entropy_disc.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Call Count','Sms Count','Unique location count','Percentage of time spent in a location','Call Entropy','Sms Entropy','Location Entropy','Call Loyalty','Sms Loyalty','Location Loyalty','Call Strong ties','Sms Strong ties','Location Strong ties','Call Weak ties','Sms Weak ties','Location Weak ties'])

    for imei in imei_list:
      row_list = []
      row_list.append(imei)
      if imei.find("androidId")>0:
        continue

      try:
        row_list.append(call_count_disc[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(sms_count_disc[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(location_unique_count_disc[imei])
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(round(location_percentage_time_spent[imei]))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(round(call_entropy_disc[imei]*100))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(round(sms_entropy_disc[imei]*100))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(round(location_entropy_disc[imei]*100))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(round(call_loyalty_disc[imei]*100))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(round(sms_loyalty_disc[imei]*100))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(round(location_loyalty_disc[imei]*100))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(int(round(call_stie_disc[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(int(round(sms_stie_disc[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(int(round(location_stie_disc[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(int(round(call_wtie_disc[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(int(round(sms_wtie_disc[imei])))
      except KeyError:
        row_list.append("NA")
      try:
        row_list.append(int(round(location_wtie_disc[imei])))
      except KeyError:
        row_list.append("NA")
          
      wr.writerow(row_list)

    resultFile.close()




########################################################

############### Call type csv file ######################

    if len(file) > 0:
      resultFile = open(file + "_call_type.csv",'wb')
    else:  
      resultFile = open("output_call_type.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['To','From','Timestamp','Type','Duration'])
    for row in call_type_csv:
      wr.writerow(row)

    resultFile.close()
    
    if len(file) > 0:
      resultFile = open(file + "_sms_type.csv",'wb')
    else:  
      resultFile = open("output_sms_type.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['To','From','Timestamp','Type'])
    for row in sms_type_csv:
      wr.writerow(row)

    resultFile.close()
    

#########################################################



###########Location Stats more verbose ##################
    list_of_lists = []

    for imei, date in location_stats.iteritems():
      for date_key, time_long_lat in date.iteritems():
        for element in time_long_lat:
          time_hour = element[0][:element[0].index(":")]
          print(time_hour)
          plus_one = int(time_hour)+1
          if plus_one<10:
            plus_one = "0"+str(plus_one)
          else:
            plus_one = str(plus_one)
          interval_string = time_hour+":00-"+str(plus_one)+":00"
          print interval_string
          try:  
            location_stats_interval[imei][date_key][interval_string]+=1
          except KeyError:
            print "KeyError in location interval"
            print imei
          row = [imei, date_key, element[0], element[1], element[2], element[3]]
          list_of_lists.append(row)

    list_of_lists.sort(key=lambda x:(x[0], x[1], x[2]))

    print "TESTING ---> list of locations"
    for d in list_of_lists:
      print d

    new_list = []
    prev = 0
    prev_imei = " "
    for d in list_of_lists:
      diff = int(d[5]) - prev
      if diff > 3500 or d[0] != prev_imei:
        new_list.append(d)
        prev = int(d[5])
        prev_imei = d[0]

    for d in new_list:
      print d
      

    new_list.sort(key=lambda x:(x[0], x[1], x[2]))

    if len(file) > 0:
      resultFile = open(file + "_location_verbose.csv",'wb')
    else:  
      resultFile = open("output_location_verbose.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Day','Time','Longitude','Latitude'])
    for x in new_list:
      row = [x[0],x[1],x[2],x[3],x[4]]
      wr.writerow(row)

    resultFile.close()

    print "OUPUT!!!!!"

    list_of_lists = []
    for imei, date_1 in location_stats_interval.iteritems():
      for date_key, interval in date_1.iteritems():
        for time_slot, freq in interval.iteritems():
          row = [imei, date_key, time_slot, freq]
          #print row
          list_of_lists.append(row)

    list_of_lists.sort(key=lambda x:(x[0], x[1], x[2]))

    if len(file) > 0:
      resultFile = open(file + "_location_interval.csv",'wb')
    else:  
      resultFile = open("output_location_interval.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Day','Time_slot','Frequency'])
    for x in list_of_lists:
      row = [x[0],x[1],x[2],x[3]]
      wr.writerow(row)

    resultFile.close()

    for imei, date_1 in location_stats_interval.iteritems():
      for date_key, interval in date_1.iteritems():
        for time_slot, freq in interval.iteritems():
          if freq>1:
            location_stats_interval[imei][date_key][time_slot] = 1

    # New code for graphs

    threshold_loc = {}

    for imei, date_1 in location_stats_interval.iteritems():
      for date_key, interval in date_1.iteritems():
        sum_freq = sum(interval.values())
        sum_per = (float(sum_freq)/24)*100

        print "Sum, percentage"
        print sum_freq,sum_per

        try:
          threshold_loc[imei]
        except KeyError:
          threshold_loc[imei] = {}
        
        # To change the percentage for the location graph change the value below
        if sum_per >= 75:
          threshold_loc[imei][date_key] = 1
        else:
          threshold_loc[imei][date_key] = 0

    graph_list = {}
    for imei, date_1 in threshold_loc.iteritems():
      sum_freq = sum(date_1.values())
      graph_list[imei] = sum_freq

    print threshold_loc

    print "Graph list"
    print graph_list


    ######## Code below to select only the final few IMEI ########
    cherry_picked_graph_list = {}

    #imei_file = open('imei_selected','r')

    with open('imei_selected', 'r') as f:
        content = f.readlines()

    print "Deleting the unwanted IMEIs"
    for ele in content:
        ele = ele.strip('\n').strip()
        try:
            cherry_picked_graph_list[ele] = graph_list[ele]
        except KeyError:
            print "Key Error for cherry picking: "+ele


    count_of_days = dict(Counter(cherry_picked_graph_list.values()))
    cherry_picked_graph_list = count_of_days  

    list_of_lists = []
    for imei, date_1 in cherry_picked_graph_list.iteritems():
      row = [imei, date_1]
      list_of_lists.append(row)

    list_of_lists.sort(key=lambda x:(x[0], x[1]))
    if len(file) > 0:
      resultFile = open(file + "_location_graph_freq_selected_imei.csv",'wb')
    else:  
      resultFile = open("output_location_graph_freq_selected_imei.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Num of days'])
    for x in list_of_lists:
      row = [x[0],x[1]]
      wr.writerow(row)

    resultFile.close()
    # Graph code ends here

    list_of_lists = []
    for imei, date_1 in location_stats_interval.iteritems():
      for date_key, interval in date_1.iteritems():
        for time_slot, freq in interval.iteritems():
          row = [imei, date_key, time_slot, freq]
          #print row
          list_of_lists.append(row)

    list_of_lists.sort(key=lambda x:(x[0], x[1], x[2]))

    if len(file) > 0:
      resultFile = open(file + "_location_graph_selected_imei.csv",'wb')
    else:  
      resultFile = open("output_location_graph_selected_imei.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Day','Time_slot','Frequency'])
    for x in list_of_lists:
      row = [x[0],x[1],x[2],x[3]]
      wr.writerow(row)

    resultFile.close()


    ################################################################

    count_of_days = dict(Counter(graph_list.values()))
    graph_list = count_of_days  

    list_of_lists = []
    for imei, date_1 in graph_list.iteritems():
      row = [imei, date_1]
      list_of_lists.append(row)

    list_of_lists.sort(key=lambda x:(x[0], x[1]))
    if len(file) > 0:
      resultFile = open(file + "_location_graph_freq.csv",'wb')
    else:  
      resultFile = open("output_location_graph_freq.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Num of days'])
    for x in list_of_lists:
      row = [x[0],x[1]]
      wr.writerow(row)

    resultFile.close()
    # Graph code ends here

    list_of_lists = []
    for imei, date_1 in location_stats_interval.iteritems():
      for date_key, interval in date_1.iteritems():
        for time_slot, freq in interval.iteritems():
          row = [imei, date_key, time_slot, freq]
          #print row
          list_of_lists.append(row)

    list_of_lists.sort(key=lambda x:(x[0], x[1], x[2]))

    if len(file) > 0:
      resultFile = open(file + "_location_graph.csv",'wb')
    else:  
      resultFile = open("output_location_graph.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Day','Time_slot','Frequency'])
    for x in list_of_lists:
      row = [x[0],x[1],x[2],x[3]]
      wr.writerow(row)

    resultFile.close()




#######################################################




    for device, date_value in files_csv.iteritems():
      for date, column_value in files_csv[device].iteritems():
        try:
          imei = device_imei[device]
        except KeyError:
          imei = 0            
        #print([device,date,files_csv[device][date]["Total Duration"],files_csv[device][date]["Distinct Calls"],files_csv[device][date]["Num of calls"],files_csv[device][date]["Number of SMS"],files_csv[device][date]["Distinct users SMS"],files_csv[device][date]["Distinct Locations"]])
        row = [imei,device,date,files_csv[device][date]["Total Duration"],files_csv[device][date]["Distinct Calls"],files_csv[device][date]["Num of calls"],files_csv[device][date]["Number of SMS"],files_csv[device][date]["Distinct users SMS"],files_csv[device][date]["Distinct Locations"]]
        table.append(row)
        #print row
        #print "\n"

    table.sort(key=lambda x:(x[1], x[2]))
    
    if len(file) > 0:
      resultFile = open(file + ".csv",'wb')
    else:  
      resultFile = open("output.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Device ID','Day','Total Call duration','Distinct users called','Number of calls','Number of SMS','Distinct users SMS','Distinct Locations'])
    for x in table:
      wr.writerow(x)

    resultFile.close()
#####################################################################

def ar_coefficient():
        list_calls = []

        for device, date_value in files_csv.iteritems():
                od = OrderedDict(sorted(files_csv[device].items()))
                print od
                new_list = []
                for date_val, calls in od.iteritems():
                        new_list.append(calls['Num of calls'])
                print new_list
                flag = 0                

                for d in new_list:
                        if d!=0:
                                flag = 1
                if flag == 0 or len(new_list)<=31:
                        continue

                X = np.array(new_list)

                print X
                print X.shape
                if len(new_list)<=31:
                        coefs_est, sigma_est = alg.AR_est_YW(X, len(new_list)-1)
                else:
                        coefs_est, sigma_est = alg.AR_est_YW(X, 31)
                # no rigorous purpose behind 100 transients
                #X_hat, _, _ = utils.ar_generator(
                #    N=npts, sigma=sigma_est, coefs=coefs_est, drop_transients=100, v=noise
                #    )
                print "Coefficients"
                ar_values[device_imei[device]]=[]
                for d in coefs_est:
                    print d
                    ar_values[device_imei[device]].append(round(float(d),2))






def get_gyradius(uset,average):
    x = []
    y = []
    avg_radius = []

    for pair in uset:
        lstr = pair.split()
        x.append(float(lstr[0]))    
        y.append(float(lstr[1]))    

    #print uset
    centroid = get_centroid(x,y)

    for i in range(len(x)):
        #a = numpy.array((x[i],y[i]))
        a = (x[i],y[i])
        #avg_radius.append(numpy.sqrt(numpy.sum((a-centroid)**2)))
        avg_radius.append(vincenty(centroid, a).km)

    #print avg_radius/len(x)
    if average == 1:
        return numpy.average(avg_radius)
    else:
        return numpy.median(avg_radius)

def get_distance_above_100_percent(uset):
    x = []
    y = []
    count = 0
    dist_list = []

    for pair in uset:
        lstr = pair.split()
        y.append(float(lstr[0]))    
        x.append(float(lstr[1]))    

    #print uset

    centroid = get_centroid(x,y)

    print "POINTS"
    for i in range(len(x)):
        #a = numpy.array((x[i],y[i]))
        a = (x[i],y[i])
        print a
        #avg_radius.append(numpy.sqrt(numpy.sum((a-centroid)**2)))
        dist = vincenty(centroid, a).km
        dist_list.append(dist)
        #print dist
        if dist>100:
            count+=1

    #print avg_radius/len(x)
    per = (float(count)/float(len(x)))*100
     
    #if int(per) == 100:
    #    print centroid
    #    print dist_list

    return per

def get_centroid(x_list,y_list):
    if len(x_list) == 1:
        return (x_list[0],y_list[0])

    x = 0
    y = 0
    z = 0

    for i in range(len(x_list)):
        latitude = x_list[i] * math.pi / 180
        longitude = y_list[i] * math.pi / 180

        x += math.cos(latitude) * math.cos(longitude)
        y += math.cos(latitude) * math.sin(longitude)
        z += math.sin(latitude)

    total = len(x_list)

    x = x / total
    y = y / total
    z = z / total

    centralLongitude = math.atan2(y, x)
    centralSquareRoot = math.sqrt(x * x + y * y)
    centralLatitude = math.atan2(z, centralSquareRoot)

    return (centralLatitude * 180 /math.pi, centralLongitude * 180 / math.pi)
     

start = strftime("%Y-%m-%d %H:%M:%S", localtime())






for db_file in f:
    print("Current DB file is",db_file)

    new_db = utils.Db_Utils(db_file)

    con = lite.connect(db_file)

    write_stats_to_csv()

    continue

    new_db.distinct_sms_for_day_opt()

    new_db.distinct_calls_for_day_opt()


    new_db.distinct_locations_for_a_device_opt()
    
    count = new_db.get_file_count()

    print("Total Count =",count)
    c_call = new_db.get_probe_count("Call")
    c_loc = new_db.get_probe_count("Location")

    print(new_db.__doc__)


    print("Total Call Count =",c_call)

    print("Total Location Count =",c_loc)

    c_call = new_db.get_distinct_probe_count("Call")
    c_loc = new_db.get_distinct_probe_count("Location")

    #print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1347517370)))


    print("Total Distinct Call Count =",c_call)

    print("Total Distinct Location Count =",c_loc)

    new_db.print_distinct_devices()


    new_db.test()


    new_db.print_number_of_probes_per_device("Call")

    new_db.print_number_of_distinct_probes_per_device("Call")

    new_db.print_number_of_probes_per_device("Sms")

    new_db.print_number_of_distinct_probes_per_device("Sms")

    #new_db.print_distinct_sms_types()

    #new_db.display_standard_time_for_location_service()
    '''
    new_db.distinct_calls_for_day()

    new_db.distinct_calls_for_day_extended()

    new_db.distinct_calls_for_day_total_minutes()

    new_db.distinct_locations_for_a_device()
    '''


create_csv()
end = strftime("%Y-%m-%d %H:%M:%S", localtime())

print start
print end




