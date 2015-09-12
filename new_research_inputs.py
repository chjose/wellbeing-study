import csv
from geopy.geocoders import Nominatim
import datetime
import time
import sqlite3 as lite
import json
import pickle

with open('Survey.csv', mode='r') as infile:
    reader = csv.reader(infile)
    survey_time = {rows[3]:time.mktime(datetime.datetime.strptime(rows[0], "%m/%d/%Y %H:%M").timetuple()) for rows in reader}
    infile.seek(0)
    survey_calls_day = {rows[3]:rows[9] for rows in reader}
    infile.seek(0)
    survey_calls_week = {rows[3]:rows[10] for rows in reader}
    infile.seek(0)
    survey_calls_month = {rows[3]:rows[11] for rows in reader}
    infile.seek(0)
    survey_contacts_day = {rows[3]:rows[12] for rows in reader}
    infile.seek(0)
    survey_contacts_week = {rows[3]:rows[13] for rows in reader}
    infile.seek(0)
    survey_contacts_month = {rows[3]:rows[14] for rows in reader}
    infile.seek(0)
    survey_locations_day = {rows[3]:rows[30] for rows in reader}
    infile.seek(0)
    survey_locations_week = {rows[3]:rows[31] for rows in reader}
    infile.seek(0)
    survey_locations_month = {rows[3]:rows[32] for rows in reader}
    
real_value = {}
unique_contacts_sets = {}

for imei, value in survey_time.iteritems():
    real_value[imei] = {}
    real_value[imei]["day_start"]=int(value)-86400
    real_value[imei]["week_start"]=int(value)-604800
    real_value[imei]["month_start"]=int(value)-2592000
    real_value[imei]["call_count"] = 0
    real_value[imei]["call_week_count"] = 0
    real_value[imei]["call_month_count"] = 0
    unique_contacts_sets[imei] = {}
    unique_contacts_sets[imei]["day_contacts"] = set()
    unique_contacts_sets[imei]["week_contacts"] = set()
    unique_contacts_sets[imei]["month_contacts"] = set()
    unique_contacts_sets[imei]["day_locations"] = set()
    unique_contacts_sets[imei]["week_locations"] = set()
    unique_contacts_sets[imei]["month_locations"] = set()

#print real_value
      
con = lite.connect("/home/ubuntu/data/only_56_imeis.db")
cur = con.cursor()
cur.execute("PRAGMA temp_store = 2")
data = cur.execute("""select imei, device, timestamp, value from data
                      where probe like '%Call%'""")

imei_excluded = set()

for x in data:
    # Get the call id from the value field
    formatted_call_record = x[3].replace('\\','').replace('"{','{').replace('}"','}')

    json_call_record = json.loads(formatted_call_record)
    #print json_call_record['number']['ONE_WAY_HASH']

    record_time = int(x[2])
    try:
        if record_time <= survey_time[x[0]] and record_time >= real_value[x[0]]["day_start"]:
            real_value[x[0]]["call_count"]+=1
            unique_contacts_sets[x[0]]["day_contacts"].add(json_call_record['number']['ONE_WAY_HASH'])
        if record_time <= survey_time[x[0]] and record_time >= real_value[x[0]]["week_start"]:
            real_value[x[0]]["call_week_count"]+=1
            unique_contacts_sets[x[0]]["week_contacts"].add(json_call_record['number']['ONE_WAY_HASH'])
        if record_time <= survey_time[x[0]] and record_time >= real_value[x[0]]["month_start"]:
            real_value[x[0]]["call_month_count"]+=1
            unique_contacts_sets[x[0]]["month_contacts"].add(json_call_record['number']['ONE_WAY_HASH'])
    except KeyError:
        #print x[0]
        imei_excluded.add(x[0])

    
print imei_excluded
#print real_value

cur = con.cursor()
cur.execute("PRAGMA temp_store = 2")
data = cur.execute("""select imei, device, timestamp, value from data
                      where probe like '%Location%'""")

imei_excluded = set()

city_map = {"45 34":"Sample"}

pickle.dump(city_map, open( "save.p", "wb" ))

city_map = pickle.load(open( "save.p", "rb" ))

print city_map

for x in data:
    # Get the call id from the value field
    formatted_call_record = x[3].replace('\\','').replace('"{','{').replace('}"','}')

    json_call_record = json.loads(formatted_call_record)

    lat = round(json_call_record['mLatitude'],2)
    lng = round(json_call_record['mLongitude'],2)
      
    record_time = int(x[2])
    #url = "http://maps.googleapis.com/maps/api/geocode/json?latlng="+str(lat)+",%20+"+str(lng)+"&sensor=true"

    try:
        if record_time <= survey_time[x[0]] and record_time >= real_value[x[0]]["month_start"]:
            hash = str(lat)+" "+str(lng)
            if hash in city_map:
                city = city_map[hash]
            else:
                geolocator = Nominatim()
                while True:
                    #file = urllib.urlopen(url)
                    try:
                            location = geolocator.reverse(str(lat)+", "+str(lng))
                            city = (location.address).split(",")
                            city = city[-5]
                            city_map[hash] = city
                            break
                    except:
                            try:
                              time.sleep(1)
                              continue 
                            except KeyboardInterrupt:
                              pickle.dump(city_map, open( "save.p", "wb" ))
                              exit()
            print city

            if record_time <= survey_time[x[0]] and record_time >= real_value[x[0]]["day_start"]:
                unique_contacts_sets[x[0]]["day_locations"].add(city)
            if record_time <= survey_time[x[0]] and record_time >= real_value[x[0]]["week_start"]:
                unique_contacts_sets[x[0]]["week_locations"].add(city)
            if record_time <= survey_time[x[0]] and record_time >= real_value[x[0]]["month_start"]:
                unique_contacts_sets[x[0]]["month_locations"].add(city)
    except KeyError:
        #print x[0]
        imei_excluded.add(x[0])

    
print imei_excluded

list_of_lists = []
for imei, real_dict in real_value.iteritems():
      if int(real_dict["call_count"])==0 and int(survey_calls_day[imei])==0:
        call_day_ratio = 1
      else:
        #print float(survey_calls_day[imei])
        #print float(real_dict["call_count"])
        call_day_ratio = round(1 - abs(float(survey_calls_day[imei])-float(real_dict["call_count"]))/max(float(survey_calls_day[imei]),float(real_dict["call_count"])),2)
      if int(real_dict["call_week_count"])==0 and int(survey_calls_week[imei])==0:
        call_week_ratio = 1
      else:
        call_week_ratio = round(1 - abs(float(survey_calls_week[imei])-float(real_dict["call_week_count"]))/max(float(survey_calls_week[imei]),float(real_dict["call_week_count"])),2)
      if int(real_dict["call_month_count"])==0 and int(survey_calls_month[imei])==0:
        call_month_ratio = 1
      else:
        call_month_ratio = round(1 - abs(float(survey_calls_month[imei])-float(real_dict["call_month_count"]))/max(float(survey_calls_month[imei]),float(real_dict["call_month_count"])),2)

      if len(unique_contacts_sets[imei]["day_contacts"])==0 and int(survey_contacts_day[imei])==0:
        contact_day_ratio = 1
      else:
        contact_day_ratio = round(1 - abs(float(survey_contacts_day[imei])-float(len(unique_contacts_sets[imei]["day_contacts"])))/max(float(survey_contacts_day[imei]),float(len(unique_contacts_sets[imei]["day_contacts"]))),2)
      if len(unique_contacts_sets[imei]["week_contacts"])==0 and int(survey_contacts_week[imei])==0:
        contact_week_ratio = 1
      else:
        contact_week_ratio = round(1 - abs(float(survey_contacts_week[imei])-float(len(unique_contacts_sets[imei]["week_contacts"])))/max(float(survey_contacts_week[imei]),float(len(unique_contacts_sets[imei]["week_contacts"]))),2)
      if len(unique_contacts_sets[imei]["month_contacts"])==0 and int(survey_contacts_month[imei])==0:
        contact_month_ratio = 1
      else:
        contact_month_ratio = round(1 - abs(float(survey_contacts_month[imei])-float(len(unique_contacts_sets[imei]["month_contacts"])))/max(float(survey_contacts_month[imei]),float(len(unique_contacts_sets[imei]["month_contacts"]))),2)

      if len(unique_contacts_sets[imei]["day_locations"])==0 and int(survey_locations_day[imei])==0:
        location_day_ratio = 1
      else:
        location_day_ratio = round(1 - abs(float(survey_locations_day[imei])-float(len(unique_contacts_sets[imei]["day_locations"])))/max(float(survey_locations_day[imei]),float(len(unique_contacts_sets[imei]["day_locations"]))),2)
      if len(unique_contacts_sets[imei]["week_locations"])==0 and int(survey_locations_week[imei])==0:
        location_week_ratio = 1
      else:
        location_week_ratio = round(1 - abs(float(survey_locations_week[imei])-float(len(unique_contacts_sets[imei]["week_locations"])))/max(float(survey_locations_week[imei]),float(len(unique_contacts_sets[imei]["week_locations"]))),2)
      if len(unique_contacts_sets[imei]["month_locations"])==0 and int(survey_locations_month[imei])==0:
        location_month_ratio = 1
      else:
        location_month_ratio = round(1 - abs(float(survey_locations_month[imei])-float(len(unique_contacts_sets[imei]["month_locations"])))/max(float(survey_locations_month[imei]),float(len(unique_contacts_sets[imei]["month_locations"]))),2)

      row = [imei, call_day_ratio, call_week_ratio, call_month_ratio, contact_day_ratio, contact_week_ratio, contact_month_ratio, location_day_ratio, location_week_ratio, location_month_ratio]
      list_of_lists.append(row)

resultFile = open("survey_vs_real.csv",'wb')
wr = csv.writer(resultFile, dialect='excel')
wr.writerow(['IMEI','Consistency at day call count','Consistency at week call count','Consistency at month call count','Consistency at day contacts','Consistency at week contacts','Consistency at month contacts', 'Consistency at daily distinct location count','Consistency at weekly distinct location count','Consistency at monthly distinct location count'])
for x in list_of_lists:
  wr.writerow(x)

resultFile.close()


pickle.dump(city_map, open( "save.p", "wb" ))




