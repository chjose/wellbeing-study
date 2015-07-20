#!/usr/bin/python
__author__ = 'Christin'
from collections import defaultdict
import utils
import glob
from time import localtime, strftime
import datetime
import sqlite3 as lite
import string
import time
import csv
import math
from collections import Counter
import optparse
import json

start_time = 0
end_time = 1432176252
parser = optparse.OptionParser("usage %prog "+"-p <path> -f <file> --start <epoch-time> --end <epoch-time> Ex: 'python %prog -p ~ -f my_test'")
parser.add_option('-p', dest='path', type='string', help='Please give the database path(all_data.db) excluding the file name.')
parser.add_option('-f', dest='file', type='string', help='This name will be prepended to the output csv`s generated')
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
shannons_entropy_stats = {}
shannons_entropy_sms = {}
shannons_location = {}
shannons_location_update = {}
device_imei = {}
location_stats = {}
location_stats_interval = {}
call_type_csv = []
sms_type_csv = []

con = 0

def write_stats_to_csv():
    d = defaultdict(list)
    dd = defaultdict(list)
    d_duration = defaultdict(list)

    cur = con.cursor()
    cur.execute("PRAGMA temp_store = 2")
    data_imei = cur.execute("""select device, value from data
                             where probe like '%Hardware%'""")


    for x in data_imei:
      duration_index = x[1].find('"deviceId"')
      duration_index = x[1].find(':', duration_index)
      last_index = x[1].find(',', duration_index)
      imei_num = x[1][duration_index+2:last_index-1]
      device_imei[x[0]]=imei_num
    
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

            try:
                if call_type == 1:
                    #Incoming Call
                    call_type_csv_row.append(device_imei[x[0]])
                    call_type_csv_row.append(json_call_record['number']['ONE_WAY_HASH'])
                    call_type_csv_row.append(json_call_record['timestamp'])
                    call_type_csv_row.append("Incoming")
                    call_type_csv_row.append(json_call_record['duration'])
                
                if call_type == 2:
                    #Outgoing Call
                    call_type_csv_row.append(json_call_record['number']['ONE_WAY_HASH'])
                    call_type_csv_row.append(device_imei[x[0]])
                    call_type_csv_row.append(json_call_record['timestamp'])
                    call_type_csv_row.append("Outgoing")
                    call_type_csv_row.append(json_call_record['duration'])

                if call_type == 3:
                    #Missed Call
                    call_type_csv_row.append(device_imei[x[0]])
                    call_type_csv_row.append(json_call_record['number']['ONE_WAY_HASH'])
                    call_type_csv_row.append(json_call_record['timestamp'])
                    call_type_csv_row.append("Missed")
                    call_type_csv_row.append(json_call_record['duration'])
            
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
          
          try:
            device_imei[x[0]]
          except KeyError:
            device_imei[x[0]] = x[0]

          print "imei: "+str(device_imei[x[0]])+"Date: "+str(std_mtime)
          try:
            shannons_entropy_stats[device_imei[x[0]]]
          except KeyError:
            shannons_entropy_stats[device_imei[x[0]]] = {}
        
          try:
            shannons_entropy_stats[device_imei[x[0]]][call_id] += 1
          except KeyError:
            shannons_entropy_stats[device_imei[x[0]]][call_id] = 1
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
                if sms_type == 1:
                    #Incoming Sms
                    sms_type_csv_row.append(device_imei[x[0]])
                    sms_type_csv_row.append(json_sms_record['address']['ONE_WAY_HASH'])
                    sms_type_csv_row.append(json_sms_record['timestamp'])
                    sms_type_csv_row.append("Incoming")
                
                if sms_type == 2:
                    #Outgoing Sms
                    sms_type_csv_row.append(json_sms_record['address']['ONE_WAY_HASH'])
                    sms_type_csv_row.append(device_imei[x[0]])
                    sms_type_csv_row.append(json_sms_record['timestamp'])
                    sms_type_csv_row.append("Outgoing")

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
          try:
            device_imei[x[0]]
          except KeyError:
            device_imei[x[0]] = x[0]

          print "imei: "+str(device_imei[x[0]])+"Date: "+str(std_mtime)
          try:
            shannons_entropy_sms[device_imei[x[0]]]
          except KeyError:
            shannons_entropy_sms[device_imei[x[0]]] = {}
          
          try:
            shannons_entropy_sms[device_imei[x[0]]][call_id] += 1
          except KeyError:
            shannons_entropy_sms[device_imei[x[0]]][call_id] = 1

    #print d  
    #for row in data:

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
          plus_one = int(time_hour)+1
          if plus_one<10:
            plus_one = "0"+str(plus_one)
          else:
            plus_one = str(plus_one)
          interval_string = time_hour+":00-"+str(plus_one)+":00"
          print interval_string
          try:
            if location_stats_interval_copy[imei][date_key][interval_string] == 0:
              location_stats_interval_copy[imei][date_key][interval_string]+=1
              ref = imei+date_key+interval_string
              location_bin = str(element[1]) + " " + str(element[2])
            
              try:
                shannons_location_update[imei]
              except KeyError:
                shannons_location_update[imei] = {}
              
              try:
                shannons_location_update[imei][location_bin] += 1
              except KeyError:
                shannons_location_update[imei][location_bin] = 1
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

def create_csv():

    print device_imei
    location_entropy = {}
    call_entropy = {}
    sms_entropy = {}

    location_loyalty = {}
    call_loyalty = {}
    sms_loyalty = {}
    
    location_stie = {}
    call_stie = {}
    sms_stie = {}
    
    location_wtie = {}
    call_wtie = {}
    sms_wtie = {}
    
    location_count = {}
    call_count = {}
    sms_count = {}
    
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

    #print "SHANNONS ENTROPY FOR SMS" 
    #for imei, ndict in sms_entropy.iteritems():
    #  print ndict
    
    # Calculate the Loyalty 
    for imei, ndict in shannons_location_update.iteritems():
      entropy = 0
      sum1 = sum(ndict.values())
      location_count[imei] = sum1
      top_3 = dict(Counter(ndict).most_common(3))
      #print top_3
      for location_bin, count in top_3.iteritems():
        p_x = float(count)/sum1
        if p_x > 0:
          entropy += p_x
      location_loyalty[imei] = entropy

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

    #print "LOYALTY FOR CALL" 
    #for imei, ndict in call_loyalty.iteritems():
    #  print ndict
    
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

    #print "LOYALTY FOR SMS" 
    #for imei, ndict in sms_loyalty.iteritems():
    #  print ndict

    print "Strong ties for locations"
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

      top_k = dict(Counter(ndict).most_common(pick_k))
      bottom_k = sorted(ndict.values())[:pick_k]
      #print "Top k"+str(top_k)
      #print "Bottom k"+str(bottom_k)
      #print top_3
      for location_bin, count in top_k.iteritems():
        strong_tie += count
        last = count
      
      #print "Fraction" 
      #print fraction 
      if fraction > 0:
        strong_tie -= last
        strong_tie += float(last)*fraction

      #print "Strong tie value above"+str(strong_tie)
      strong_tie = (strong_tie/float(sum1)) * 100
      #print "Strong tie"+str(strong_tie)

      location_stie[imei] = strong_tie

      weak_tie = sum(bottom_k)
      if fraction > 0:
        weak_tie -= bottom_k[pick_k-1]
        weak_tie += float(bottom_k[pick_k-1])*fraction

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

      strong_tie = (strong_tie/float(sum1)) * 100
      print strong_tie

      call_stie[imei] = strong_tie
      weak_tie = sum(bottom_k)
      if fraction > 0:
        weak_tie -= bottom_k[pick_k-1]
        weak_tie += float(bottom_k[pick_k-1])*fraction

      weak_tie = (weak_tie/float(sum1)) * 100
      print weak_tie

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

      strong_tie = (strong_tie/float(sum1)) * 100
      print strong_tie

      sms_stie[imei] = strong_tie

      weak_tie = sum(bottom_k)
      if fraction > 0:
        weak_tie -= bottom_k[pick_k-1]
        weak_tie += float(bottom_k[pick_k-1])*fraction

      weak_tie = (weak_tie/float(sum1)) * 100
      print weak_tie

      sms_wtie[imei] = weak_tie
    #print "STRONG FOR LOCATION" 
    #for imei, ndict in sms_stie.iteritems():
    #  print ndict


    if len(file) > 0:
      resultFile = open(file + "_entropy.csv",'wb')
    else:  
      resultFile = open("output_entropy.csv",'wb')
    wr = csv.writer(resultFile, dialect='excel')
    wr.writerow(['IMEI','Call Count','Sms Count','Location Count','Call Entropy','Sms Entropy','Location Entropy','Call Loyalty','Sms Loyalty','Location Loyalty','Call Strong ties','Sms Strong ties','Location Strong ties','Call Weak ties','Sms Weak ties','Location Weak ties'])
    imei_list = list(set(device_imei.values()))
    for imei in imei_list:
      row_list = []
      row_list.append(imei)
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



