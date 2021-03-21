import pandas as pd
import geopandas as gpd
import numpy as np
import os, urllib, json, csv, zipfile, math

INPUT = "iso_input.csv"
OUTPUT = "iso_output.csv"
OUTPUT_PC = "distances_pc4.json"

# Create table with all origin-distination pairs to plan
with zipfile.ZipFile('gtfs-nl.zip') as z:
    with z.open('stops.txt') as f:
        df = pd.read_csv(f)
df = df[['stop_lat','stop_lon']]
df['stop_lat'] = df['stop_lat'].round(2)
df['stop_lon'] = df['stop_lon'].round(2)
df['Date'] = '03/17/2021'
df['Time'] = '04:00PM'
df['start_lat'] = '52.088208'
df['start_lon'] = '5.113234'
df = df[['Date', 'Time', 'start_lat', 'start_lon', 'stop_lat', 'stop_lon']]
df = df.drop_duplicates()
df.to_csv(INPUT, index=False)

# Find all travel times
URL = 'http://localhost:8080/otp/routers/default/plan?'

outputFile = open(OUTPUT, 'w')
writer = csv.writer(outputFile)
writer.writerow(['StartDate','StartTime','StartLat','StartLon','EndLat','EndLon', 'DurationMin'])

# Takes CSV input, creates URLs, stores data locally in row array
for c, d in df.iterrows():
    params =  {'date'            : d['Date'],
               'time'            : d['Time'],
               'fromPlace'       : '%s,%s' % (d['start_lat'], d['start_lon']),
               'toPlace'         : '%s,%s' % (d['stop_lat'], d['stop_lon']),
               'maxWalkDistance' : 2000,
               'mode'            : 'WALK,TRANSIT', 
               'numItineraries'  : 1,
               'arriveBy'        : 'false' }
    req = urllib.request.Request(URL + urllib.parse.urlencode(params))
    req.add_header('Accept', 'application/json')

    response = urllib.request.urlopen(req) 
    try :
        content = response.read()
        objs = json.loads(content)
        i = objs['plan']['itineraries'][0]
        outrow = d.tolist() + [ int(i['duration']/60) ]
        print(str(c) + ' ' + str(outrow))
        writer.writerow(outrow)
    except :
        print ('no itineraries')
        continue
outputFile.close()

# group them on postal code level
pc = gpd.read_file(os.path.join('PC_4-shp','PC4.shp'))
df = pd.read_csv(OUTPUT)
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.EndLon, df.EndLat))[['geometry', 'DurationMin']]

def get_avg_traveltime(pc4):
    v = gdf[gdf.within(pc4['geometry'] )].mean(skipna=True)
    if math.isnan(v):
        return np.NaN
    else:
        return int(v[0])
    
pc['Traveltime'] = pc.apply(lambda x: get_avg_traveltime(x), axis=1)

# Unreachable? Closest other postal area and add 15 min
for index, pcode in pc.iterrows():   
    if math.isnan(pcode.Traveltime):
        # get 'not disjoint' countries
        dst = pc[pc.geometry.touches(pcode.geometry)]['Traveltime'].mean(skipna=True)
        # add names of neighbors as NEIGHBORS value
        if ~math.isnan(dst) and (dst > 0):
            pc.at[index, "Traveltime"] = int(dst + 15)
        else:
            pc.at[index, "Traveltime"] = pc.Traveltime.max()
            
pc.to_file(OUTPUT_PC, driver="GeoJSON")
