#!/usr/bin/python

from collections import defaultdict
import string
import time
import csv

__author__ = 'Christin'

import sqlite3 as lite

class Db_Utils:
    '''
    Call the constructor with the SQLite3 database name in the double code
    Example: utils.Db_Utils('test.db')

    Methods defined:
    1- get_file_count() : List the number of entries in the database file which includes the duplicate entries as well
    2- get_probe_count(probe) : Returns the count of a particular probe which can be any of the five attributes which we
                                have, Use the below conventions for the function argument:
                                a) For HardwareInfoProbe: Use "Hardware"
                                b) For CallLogProbe: Use "Call"
                                c) For SmsProbe: Use "Sms"
                                d) For CellTowerProbe: Use "Tower"
                                e) For LocationProbe: Use "Location"
    3- get_distinct_probe_count(probe)  : It has the same functionality as the above but returns the distinct entry
                                          count. Has the same calling conventions.
    4- print_number_of_probes_per_device(probe)  : Prints the number of probes per device.
                                                   Has the same calling conventions.
    5- print_number_of_distinct_probes_per_device(probe)  : Prints the number of distinct probe counts per device.
                                                            Has the same calling conventions.
    6- print_distinct_devices()  : Prints all the distinct devices in the given table.
    '''

    def __init__(self,db_name):
        self.con = lite.connect(db_name)

    def get_file_count(self):
        cur = self.con.cursor()
        data = cur.execute("select count(*) from data;")
        for row in data:
            count = row[0]
        return count

    def test(self):
        cur = self.con.cursor()
        data = cur.execute("select * from data;")
        for row in data:
	    print row

    def get_probe_count(self, probe):
        cur = self.con.cursor()
        make_probe = '%'+probe+'%'
        data = cur.execute("select  count(*) from data where probe like ?;",(make_probe,))

        for row in data:
            count = row[0]
        return count

    def get_distinct_probe_count(self, probe):
        cur = self.con.cursor()
        make_probe = '%'+probe+'%'
        data = cur.execute("select  count(DISTINCT timestamp) from data where probe like ?;",(make_probe,))
        for row in data:
            count = row[0]
        return count

    def print_number_of_probes_per_device(self, probe):
        cur = self.con.cursor()
        make_probe = '%'+probe+'%'
        distinct_devices = cur.execute("select DISTINCT device from data where probe like ?;",(make_probe,))
        cur = self.con.cursor()
        print()
        print("Total",probe,"data for a device")
        print("-------------------------------")
        for row in distinct_devices:
            print("Device id: ",row[0])
            data = cur.execute("select  count(*) from data where probe like ? AND device=?;",(make_probe,row[0]))
            for x in data:
                print("Total ",probe,"=",x[0])

    def print_number_of_distinct_probes_per_device(self, probe):
        cur = self.con.cursor()
        make_probe = '%'+probe+'%'
        distinct_devices = cur.execute("select DISTINCT device from data where probe like ?;",(make_probe,))
        cur = self.con.cursor()
        print()
        print("Distinct",probe,"data for a device")
        print("----------------------------------")
        for row in distinct_devices:
            print("Device id: ",row[0])
            data = cur.execute("select  count(DISTINCT timestamp) from data where probe like ? AND device=?;",(make_probe,row[0]))
            for x in data:
                print("Total ",probe,"=",x[0])

    def print_distinct_devices(self):
        cur = self.con.cursor()
        distinct_devices = cur.execute("select DISTINCT device from data;")
        print()
        print("Distinct Devices")
        print("------------------")
        for row in distinct_devices:
            print("Device id: ",row[0])

    def print_same_call_ids(self):
        cur = self.con.cursor()
        distinct_devices = cur.execute("select timestamp,value from data where probe like '%Call%';")
        print()
        print("Distinct Devices")
        print("------------------")
        for row in distinct_devices:
            #print(type(row[1]))
            temp = row[1].find('_id":13555,')
            if temp > 0:
                print("Device id: ", row[0], row[1])

    def print_distinct_sms_types(self):
        cur = self.con.cursor()
        distinct_devices = cur.execute("select timestamp,value from data where probe like '%Sms%';")
        print()
        print("Distinct Sms types")
        print("------------------")
        for row in distinct_devices:
            #print(type(row[1]))
            temp = row[1].find('"type":5')
            #temp1 = row[1].find('"type":2')
            if temp > 0:
                print("Device id: ", row[0], row[1])


    def display_standard_time_for_location_service(self):
        cur = self.con.cursor()
        distinct_devices = cur.execute("select timestamp,value from data where probe like '%Location%' ORDER BY timestamp;")
        print()
        print("Time stamp and mtime from value field")
        print("--------------------------------------")
        for row in distinct_devices:
            timestamp = int(row[0])
            print(timestamp)
            std_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            temp = row[1].find('"mTime"')
            if temp > 0:
                temp += 8
                new_str = int(row[1][temp:temp+13])/1000
                print(new_str)
                std_mtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(new_str))
                #std_mtime = time.strftime('%Y-%m-%d', time.localtime(new_str))

            print("Time stamp in Date time", std_timestamp)
            print("M time in Date time", std_mtime)

    def distinct_calls_for_day(self):
        cur = self.con.cursor()
        distinct_devices = cur.execute("select DISTINCT device from data where probe like '%Call%';")
        
	print "Here"
	cur = self.con.cursor()
        times_list = []
        devices = []
        for row in distinct_devices:
            temp = []
            #print("Device id: ",row[0])
            devices.append(row[0])
            data = cur.execute("""select timestamp from data
                                  where probe like '%Call%' and device = ?
                                  group by timestamp;""",(row[0],))
	    print "Here"
            for x in data:
                std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[0]))
                #print(std_mtime)
                temp.append(std_mtime)
            times_list.append(temp)

        #print(times_list)
        print("Number of calls for the all the devices on a day")

        i=0
        for y in times_list:
            print("Device ID",devices[i])
            i += 1
            distinct_times = set(y)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            #print("Distinct dates")
            #print(distinct_times)

            for x in distinct_times:
                temp = y.count(x)
                print("Number of calls on",x)
                print(temp)

    def distinct_calls_for_day_extended(self):
        cur = self.con.cursor()
        distinct_devices = cur.execute("select DISTINCT device from data where probe like '%Call%';")
        cur = self.con.cursor()
        times_list = []
        devices = []
        for row in distinct_devices:
            temp = []
            #print()
            #print("Device id: ",row[0])
            devices.append(row[0])
            data = cur.execute("""select timestamp, value from data
                                  where probe like '%Call%' and device = ?
                                  group by timestamp;""",(row[0],))
            for x in data:
                std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[0]))
                number_index = x[1].find('"number"')
                last_index = x[1].find(',', number_index)
                temp_str = std_mtime + " " + x[1][number_index:last_index-4]
                temp.append(temp_str)

            #print(temp)
            times_list.append(temp)

        print("Number of calls for the all the devices on a day")

        i=0
        for y in times_list:
            print()
            print("Device ID:",devices[i])
            print("===============================================")
            i += 1

            distinct_times = set(y)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += 1
                else:
                    print("Number of distinct users called on",previous_date,"is",j)
                    j = 1
                previous_date = current_date

            print("Number of distinct users called on",previous_date,"is",j)

    def distinct_calls_for_day_total_minutes(self):
        cur = self.con.cursor()
        distinct_devices = cur.execute("select DISTINCT device from data where probe like '%Call%';")
        cur = self.con.cursor()
        times_list = []
        devices = []
        for row in distinct_devices:
            temp = []
            #print()
            #print("Device id: ",row[0])
            devices.append(row[0])
            data = cur.execute("""select timestamp, value from data
                                  where probe like '%Call%' and device = ?
                                  group by timestamp;""",(row[0],))
            for x in data:
                std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[0]))
                duration_index = x[1].find('"duration"')
                duration_index = x[1].find(':', duration_index)
                last_index = x[1].find(',', duration_index)
                temp_str = std_mtime + " " + x[1][duration_index+1:last_index]
                temp.append(temp_str)

            #print(temp)
            times_list.append(temp)

        print("Number of calls for the all the devices on a day")

        i=0
        for y in times_list:
            print()
            print("Device ID:",devices[i])
            print("===============================================")
            i += 1

            distinct_times = set(y)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += int(x[1])
                else:
                    print("Total call duration on",previous_date,"is",j,"second(s)")
                    j = int(x[1])
                previous_date = current_date

            print("Total call duration on",previous_date,"is",j,"second(s)")

    def distinct_locations_for_a_device(self):
        cur = self.con.cursor()
        distinct_devices = cur.execute("select DISTINCT device from data where probe like '%Location%';")
        cur = self.con.cursor()
        times_list = []
        devices = []
        for row in distinct_devices:
            temp = []
            #print()
            #print("Device id: ",row[0])
            devices.append(row[0])
            data = cur.execute("""select timestamp, value
                                  from data
                                  where probe like '%Location%' and device = ?
                                  group by timestamp;""",(row[0],))
            #print()
            for x in data:
                temp1 = x[1].find('"mTime"')
                if temp1 > 0:
                    temp1 += 8
                    new_str = int(x[1][temp1:temp1+13])/1000
                    #print(new_str)
                    std_mtime = time.strftime('%Y-%m-%d', time.localtime(new_str))
                else:
                    std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[0]))

                long_index = x[1].find('"mLongitude"')
                last_index = x[1].find(',', long_index)
                cordinate = x[1][long_index+13:last_index]
                #print(cordinate)
                fvalue_cordiante = float(cordinate)
                long_int = float("{0:.4f}".format(fvalue_cordiante))
                lat_index = x[1].find('"mLatitude"')
                last_index = x[1].find(',', lat_index)
                cordinate = x[1][lat_index+12:last_index]
                #print(cordinate)
                fvalue_cordiante = float(cordinate)
                lat_int = float("{0:.4f}".format(fvalue_cordiante))
                temp_str = std_mtime + " " + str(long_int) + " " + str(lat_int)
                #print(temp_str)
                temp.append(temp_str)

            #print(temp)
            times_list.append(temp)

        i = 0
        for y in times_list:
            print()
            print("Device ID:",devices[i])
            print("===============================================")
            i += 1

            distinct_times = set(y)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            #print(previous_date)
            #print("List before analysis")
            #for x in distinct_times:
            #    print(x)

            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += 1
                else:
                    print("Distinct locations on",previous_date,"is",j)
                    j = 1
                previous_date = current_date


            print("Distinct locations on",previous_date,"is",j)

    def distinct_calls_for_day_opt(self):

        d = defaultdict(list)
        dd = defaultdict(list)
        d_duration = defaultdict(list)

        cur = self.con.cursor()
        data = cur.execute("""select device, timestamp, value from data
                              where probe like '%Call%'
                              group by timestamp;""")

        for x in data:
            std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[1]))
            d[x[0]].append(std_mtime)
            
	    number_index = x[2].find('"number"')
            last_index = x[2].find(',', number_index)
            dist_calls_day = std_mtime + " " + x[2][number_index:last_index-4]
            dd[x[0]].append(dist_calls_day)
             
	    duration_index = x[2].find('"duration"')
            duration_index = x[2].find(':', duration_index)
            last_index = x[2].find(',', duration_index)
            duration_str = std_mtime + " " + x[2][duration_index+1:last_index]
            d_duration[x[0]].append(duration_str)

	#print d  
        #for row in data:
        print "Number of calls for the all the devices on a day"

	for key, value in d.iteritems():
            print("Device ID",key)
            
            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            #print("Distinct dates")
            #print(distinct_times)

            for x in distinct_times:
                temp = value.count(x)
                print "Number of calls on {}".format(x)
                print temp

	# Distinct numbers

	#print dd

	for key, value in dd.iteritems():
            print "\n"
            print "Device ID: {}".format(key)
            print "==============================================="

            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += 1
                else:
                    print "Number of distinct users called on {} is {}".format(previous_date,j)
                    j = 1
                previous_date = current_date

                print "Number of distinct users called on {} is {}".format(previous_date,j)



	for key, value in d_duration.iteritems():
            print "\n"
            print "Device ID: {}".format(key)
            print "==============================================="

            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += int(x[1])
                else:
                    print "Total call duration on {} is {} second(s)".format(previous_date,j)
                    j = int(x[1])
                previous_date = current_date

            print "Total call duration on {} is {} second(s)".format(previous_date,j)



    def distinct_locations_for_a_device_opt(self):
        cur = self.con.cursor()
        times_list = []
        devices = []
        

        d = defaultdict(list)

        cur = self.con.cursor()
        data = cur.execute("""select device, value, timestamp from data
                              where probe like '%Location%'
                              group by timestamp;""")

        for x in data:
            temp1 = x[1].find('"mTime"')
	    if temp1 > 0:
	      temp1 += 8
	      new_str = int(x[1][temp1:temp1+13])/1000
	      std_mtime = time.strftime('%Y-%m-%d', time.localtime(new_str))
	    else:
	      std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[2]))
      # Remove below line
	    #std_mtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x[2]))
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
	    location_str = std_mtime + " " + str(long_int) + " " + str(lat_int)
            d[x[0]].append(location_str)

	for key, value in d.iteritems():
            print "\n"
            print "Device ID: {}".format(key)
            print "==============================================="

            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            #print(previous_date)
            print("List before analysis")
            for x in distinct_times:
                print(x)

            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += 1
                else:
                    print "Distinct locations on {} is {}".format(previous_date,j)
                    j = 1
                previous_date = current_date

            print "Distinct locations on {} is {}".format(previous_date,j)

    def distinct_sms_for_day_opt(self):

        d = defaultdict(list)
        dd = defaultdict(list)

        cur = self.con.cursor()
        data = cur.execute("""select device, timestamp, value from data
                              where probe like '%Sms%'
                              group by timestamp;""")

        for x in data:
            std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[1]))
            d[x[0]].append(std_mtime)
            
	    number_index = x[2].find('"address"')
            last_index = x[2].find(',', number_index)
            dist_calls_day = std_mtime + " " + x[2][number_index:last_index-4]
            dd[x[0]].append(dist_calls_day)
             
	#print d  
        #for row in data:
        print "Number of SMS for all devices on a day"

	for key, value in d.iteritems():
            print "Device ID: {}".format(key)
            
            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            #print("Distinct dates")
            #print(distinct_times)

            for x in distinct_times:
                temp = value.count(x)
                print "Number of SMS on {}".format(x)
                print temp

	# Distinct numbers

	#print dd

	for key, value in dd.iteritems():
            print "\n"
            print "Device ID: {}".format(key)
            print "==============================================="

            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += 1
                else:
                    print "Number of distinct users SMS on {} is {}".format(previous_date,j)
                    j = 1
                previous_date = current_date

            print "Number of distinct users SMS on {} is {}".format(previous_date,j)


    ###########################################################################################################

    def write_stats_to_csv(self, files_csv, device_imei):


        d = defaultdict(list)
        dd = defaultdict(list)
        d_duration = defaultdict(list)

        cur = self.con.cursor()
        data = cur.execute("""select device, timestamp, value from data
                              where probe like '%Call%'
                              group by timestamp;""")

        for x in data:
            std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[1]))
            d[x[0]].append(std_mtime)
            
            number_index = x[2].find('"number"')
            last_index = x[2].find(',', number_index)
            dist_calls_day = std_mtime + " " + x[2][number_index:last_index-4]
            dd[x[0]].append(dist_calls_day)
             
            duration_index = x[2].find('"duration"')
            duration_index = x[2].find(':', duration_index)
            last_index = x[2].find(',', duration_index)
            duration_str = std_mtime + " " + x[2][duration_index+1:last_index]
            d_duration[x[0]].append(duration_str)

        #print d  
        #for row in data:
        print "Number of calls for the all the devices on a day"

        for key, value in d.iteritems():
            print("Device ID",key)
            
            files_csv[key] = {}
            #files_csv[key].setdefault(key,[]).append(value)

            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            #print("Distinct dates")
            #print(distinct_times)

            for x in distinct_times:
                temp = value.count(x)
                files_csv[key][x] = {}


                files_csv[key][x]["Num of calls"] = temp
                files_csv[key][x]["Number of SMS"] = 0
                files_csv[key][x]["Distinct users SMS"] = 0
                files_csv[key][x]["Distinct Locations"] = 0

                print "Number of calls on {}".format(x)
                print temp

        # Distinct numbers
        #print "NEW CSV FILE ----->"
        #print files_csv



	for key, value in dd.iteritems():
            print "\n"
            print "Device ID: {}".format(key)
            print "==============================================="

            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += 1
                else:
                    print "Number of distinct users called on {} is {}".format(previous_date,j)
                    files_csv[key][previous_date]["Distinct Calls"] = j
                    j = 1
                previous_date = current_date

                print "Number of distinct users called on {} is {}".format(previous_date,j)
                files_csv[key][previous_date]["Distinct Calls"] = j



	for key, value in d_duration.iteritems():
            print "\n"
            print "Device ID: {}".format(key)
            print "==============================================="

            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += int(x[1])
                else:
                    print "Total call duration on {} is {} second(s)".format(previous_date,j)
                    files_csv[key][previous_date]["Total Duration"] = j
                    j = int(x[1])
                previous_date = current_date

            print "Total call duration on {} is {} second(s)".format(previous_date,j)
            files_csv[key][previous_date]["Total Duration"] = j

        #print files_csv 

    

        d = defaultdict(list)
        dd = defaultdict(list)

        cur = self.con.cursor()
        data = cur.execute("""select device, timestamp, value from data
                              where probe like '%Sms%'
                              group by timestamp;""")

        for x in data:
            std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[1]))
            d[x[0]].append(std_mtime)
            
	    number_index = x[2].find('"address"')
            last_index = x[2].find(',', number_index)
            dist_calls_day = std_mtime + " " + x[2][number_index:last_index-4]
            dd[x[0]].append(dist_calls_day)
             
	#print d  
        #for row in data:
        print "Number of SMS for all devices on a day"

	for key, value in d.iteritems():
            print "Device ID: {}".format(key)
            
            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            #print("Distinct dates")
            #print(distinct_times)

            for x in distinct_times:
                temp = value.count(x)
                try:
                  files_csv[key]

                except KeyError:
                  files_csv[key] = {}
                try:
                  files_csv[key][x]["Number of SMS"] = temp
                except KeyError:
                  files_csv[key][x] = {}
                  files_csv[key][x]["Number of SMS"] = temp
                  files_csv[key][x]["Total Duration"] = 0
                  files_csv[key][x]["Distinct Calls"] = 0
                  files_csv[key][x]["Num of calls"] = 0
                  files_csv[key][x]["Distinct Locations"] = 0

                print "Number of SMS on {}".format(x)
                print temp

	# Distinct numbers

	#print dd

	for key, value in dd.iteritems():
            print "\n"
            print "Device ID: {}".format(key)
            print "==============================================="

            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += 1
                else:
                    files_csv[key][previous_date]["Distinct users SMS"] = j
                    print "Number of distinct users SMS on {} is {}".format(previous_date,j)
                    j = 1
                previous_date = current_date

            files_csv[key][previous_date]["Distinct users SMS"] = j
            print "Number of distinct users SMS on {} is {}".format(previous_date,j)
       



        cur = self.con.cursor()
        times_list = []
        devices = []
        

        d = defaultdict(list)

        cur = self.con.cursor()
        data = cur.execute("""select device, value, timestamp from data
                              where probe like '%Location%'
                              group by timestamp;""")

        for x in data:
            temp1 = x[1].find('"mTime"')
	    if temp1 > 0:
	      temp1 += 8
	      new_str = int(x[1][temp1:temp1+13])/1000
	      std_mtime = time.strftime('%Y-%m-%d', time.localtime(new_str))
	    else:
	      std_mtime = time.strftime('%Y-%m-%d', time.localtime(x[2]))
      # Remove below line
	    #std_mtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x[2]))
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
	    location_str = std_mtime + " " + str(long_int) + " " + str(lat_int)
            d[x[0]].append(location_str)

	for key, value in d.iteritems():
            print "\n"
            print "Device ID: {}".format(key)
            print "==============================================="

            distinct_times = set(value)
            distinct_times = list(distinct_times)
            distinct_times.sort()

            for x in range(len(distinct_times)):
                distinct_times[x] = distinct_times[x].split(" ")

            previous_date = distinct_times[0][0]
            j = 0
            #print(previous_date)
            print("List before analysis")
            for x in distinct_times:
                print(x)

            for x in distinct_times:
                current_date = x[0]
                if current_date == previous_date:
                    j += 1
                else:
                  try:
                    files_csv[key][previous_date]["Distinct Locations"] = j
                  except KeyError:
                    try:
                      files_csv[key]
                    except KeyError:
                      files_csv[key] = {}
                    files_csv[key][previous_date] = {}
                    files_csv[key][previous_date]["Distinct Locations"] = j
                    files_csv[key][previous_date]["Number of SMS"] = 0
                    files_csv[key][previous_date]["Total Duration"] = 0
                    files_csv[key][previous_date]["Distinct Calls"] = 0
                    files_csv[key][previous_date]["Num of calls"] = 0
                    files_csv[key][previous_date]["Distinct users SMS"] = 0
                    print "Distinct locations on {} is {}".format(previous_date,j)
                    j = 1
                previous_date = current_date

            try:
              files_csv[key][previous_date]["Distinct Locations"] = j
            except KeyError:
              try:
                files_csv[key]
              except KeyError:
                files_csv[key] = {}
              files_csv[key][previous_date] = {}
              files_csv[key][previous_date]["Distinct Locations"] = j
              files_csv[key][previous_date]["Number of SMS"] = 0
              files_csv[key][previous_date]["Total Duration"] = 0
              files_csv[key][previous_date]["Distinct Calls"] = 0
              files_csv[key][previous_date]["Num of calls"] = 0
              files_csv[key][previous_date]["Distinct users SMS"] = 0
            print "Distinct locations on {} is {}".format(previous_date,j)



        cur = self.con.cursor()
        data_imei = cur.execute("""select device, value from data
                                 where probe like '%Hardware%'
                                 group by device;""")


        for x in data_imei:
          duration_index = x[1].find('"deviceId"')
          duration_index = x[1].find(':', duration_index)
          last_index = x[1].find(',', duration_index)
          imei_num = x[1][duration_index+2:last_index-1]
          device_imei[x[0]]=imei_num

    def create_csv(self, device_imei, files_csv):

        print device_imei

        table = []

        for device, date_value in files_csv.iteritems():
          for date, column_value in files_csv[device].iteritems():
            imei = device_imei[device]            
            print([device,date,files_csv[device][date]["Total Duration"],files_csv[device][date]["Distinct Calls"],files_csv[device][date]["Num of calls"],files_csv[device][date]["Number of SMS"],files_csv[device][date]["Distinct users SMS"],files_csv[key][previous_date]["Distinct Locations"]])
            row = [imei,device,date,files_csv[device][date]["Total Duration"],files_csv[device][date]["Distinct Calls"],files_csv[device][date]["Num of calls"],files_csv[device][date]["Number of SMS"],files_csv[device][date]["Distinct users SMS"],files_csv[key][previous_date]["Distinct Locations"]]
            table.append(row)
            #print row
            #print "\n"

        table.sort(key=lambda x:(x[0], x[1]))
        
        resultFile = open("output.csv",'wb')
        wr = csv.writer(resultFile, dialect='excel')
        wr.writerow(['IMEI','Device ID','Day','Total Call duration','Distinct users called','Number of calls','Number of SMS','Distinct users SMS','Distinct Locations'])
        for x in table:
          wr.writerow(x)

        resultFile.close()
