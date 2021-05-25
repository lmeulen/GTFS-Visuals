import pandas as pd
import geopandas as gpd
import numpy as np
import urllib
import json 
import csv
import zipfile
import sys
import matplotlib.pyplot as plt
from geopandas.tools import sjoin
from shapely.geometry import LineString, Point

with zipfile.ZipFile('gtfs-nl.zip') as z:
    with z.open('stops.txt') as f:
        routes = pd.read_csv(f)
routes = routes[['stop_lat', 'stop_lon']]
routes['Date'] = '03/17/2021'
routes['Time'] = '04:00PM'
routes['utc_lat'] = '52.088208'
routes['utc_lon'] = '5.113234'
routes = routes[['Date', 'Time', 'utc_lat', 'utc_lon', 'stop_lat', 'stop_lon']]
routes = routes.drop_duplicates()

routes['point'] = routes.apply(lambda x: Point(x.stop_lon, x.stop_lat), axis=1)
routes = gpd.GeoDataFrame(routes, geometry=routes['point']).set_crs("EPSG:4326")

world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
nl = world[world.name == "Netherlands"]

routes = sjoin(routes, nl, how='left')
routes = routes[routes.name == 'Netherlands']


def get_value(jsn, key1, key2=None):
    if jsn and key1 and (key1 in jsn):
        if key2:
            return jsn[key1][key2] if key2 in jsn[key1] else None
        else:
            return jsn[key1]
    else:
        return None


URL = 'http://localhost:8080/otp/routers/default/plan?'
legs = []

# Takes CSV input, creates URLs, stores data locally in row array
for c, d in routes.iterrows():
    print(str(c) + ' / ' + str(len(routes)))
    params = {'date': d['Date'],
              'time': d['Time'],
              'fromPlace': '%s,%s' % (d['stop_lat'], d['stop_lon']),
              'toPlace': '%s,%s' % (d['utc_lat'], d['utc_lon']),
              'maxWalkDistance': 2000,
              'mode': 'WALK,TRANSIT',
              'numItineraries': 1,
              'arriveBy': 'false'}
    req = urllib.request.Request(URL + urllib.parse.urlencode(params))
    req.add_header('Accept', 'application/json')
    response = urllib.request.urlopen(req)
    trip = json.loads(response.read())
    itineraries = get_value(trip, 'plan', 'itineraries')
    if itineraries and len(itineraries) > 0:
        try:
            for leg in itineraries[0]['legs']:
                mode = get_value(leg, 'mode')
                if mode != 'WALK':
                    legs.append([mode, get_value(leg, 'routeId'), get_value(leg, 'tripId'),
                                 get_value(leg, 'tripShortName'),
                                 get_value(leg, 'from', 'stopIndex'), get_value(leg, 'from', 'stopId'),
                                 get_value(leg, 'from', 'stopCode'), get_value(leg, 'to', 'stopIndex'),
                                 get_value(leg, 'to', 'stopId'), get_value(leg, 'from', 'stopCode'),
                                 get_value(leg, 'legGeometry', 'points'),
                                 get_value(leg, 'legGeometry', 'length')])
        except:
            print('Error routing')

legs_df = pd.DataFrame(legs)
legs_df.columns = ['mode', 'routeId', 'tripId', 'tripShortName', 'fromStopIndex', 'fromStopId', 'fromStopCode',
               'toStopIndex', 'toStopId', 'toStopCode', 'geometryPoints', 'geometryLength']
legs_df.to_csv('allroadstoUT.csv', index=False)

legs_df = legs_df[['geometryPoints', 'mode']].groupby(['geometryPoints']).count().reset_index()
legs_df = legs_df.rename(columns={'mode': 'count'}).sort_values('count')


def next_val(datastring, index):
    b = None
    res = 0
    shift = 0
    while b is None or b >= 0x20:
        b = ord(datastring[index])  # integer  representing the Unicode character
        b -= 63
        b = (b & 0x1f) << shift
        res |= b
        index += 1
        shift += 5
        comp = res & 1
    res = res >> 1
    if comp:
        res = ~res
    return res, index


def decode(datastring):
    coordinates = []
    lat = lon = 0

    index = 0
    while index < len(datastring):
        delta_lat, index = next_val(datastring, index)
        delta_lon, index = next_val(datastring, index)
        lat += delta_lat
        lon += delta_lon
        coordinates.append((lat / (10**5), lon / (10**5)))

    return coordinates

segments = []
for c, g in legs_df.iterrows():
    cnt = g['count']
    lat_prev = 0.0
    lon_prev = 0.0
    for s in decode(g['geometryPoints']):
        segments.append([lat_prev, lon_prev, s[0], s[1], cnt])
        lat_prev = s[0]
        lon_prev = s[1]
segments_df = pd.DataFrame(segments)
segments_df.columns = ['lat_x', 'lon_x', 'lat_y', 'lon_y', 'count']
segments_df = segments_df[segments_df.lat_x > 0.0]

segments_df = segments_df.groupby(['lat_x', 'lon_x', 'lat_y', 'lon_y']).sum()
segments_df = segments_df.reset_index().sort_values('count')

segments_df['line'] = segments_df.apply(lambda x: LineString([Point(x["lon_x"], x["lat_x"]),
                                                              Point(x["lon_y"], x["lat_y"])]), axis=1)

geodata = gpd.GeoDataFrame(segments_df, geometry=segments_df['line'])

geodata['width'] = 5 * (geodata['count']) / (geodata['count'].max())

geodata.plot(figsize=(15, 15))

geodata.plot(figsize=(15, 15), linewidth=np.minimum(np.maximum(df5['width'], 0.1), 2.75))