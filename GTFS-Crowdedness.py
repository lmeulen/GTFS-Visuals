import os
import sys
import requests
import zipfile
import pandas as pd
from geojson import LineString, Feature, FeatureCollection, dump

GTFSDIR = 'gtfs-nl'    # https://transitfeeds.com/p/ov/814/20190705
NDOVDIR = 'ndov'
displaydate = '20210309'

def min2str(minutes):
    """
    Convert hh:mm:ss to seconds since midnight
    :param show_sec: only show :ss if True
    :param scnds: Seconds to translate to hh:mm:ss
    """
    h = int((int(minutes) / 60) % 24)
    m = int(minutes % 60)
    return "{:02d}:{:02d}".format(h, m)

def str2min(time_str):
    """
    Convert hh:mm:ss to minutes since midnight
    :param time_str: String in format hh:mm:ss
    """
    spl = time_str.strip().split(":")
    h, m, s = spl
    return int(h) * 60 + int(m)

# Show progress bar
def update_progress(part, total=100, barLength=50):
    progress = (float(part)) / total
    status = ""
    if progress < 0:
        progress = 0
        status = "Negative...\r\n"
    if progress >= 1:
        progress = 1
        status = "Done...\r\n"
    block = int(round(barLength*progress))
    text = "\rProgress: [{0}] ({2}/{3}) {1}% {4}".format("#"*block + "-"*(barLength-block), 
                                              int(progress*100), part, total,
                                              status)
    sys.stdout.write(text)
    sys.stdout.flush()
    
########################################################################################################
############################### DOWNLOAD NDOV OPEN DATA ################################################
########################################################################################################
def get_crowdedness_operator(operatorcode, type, date):
    url = "https://data.ndovloket.nl/bezetting/" + operatorcode.lower() + "/OC_" + operatorcode.upper() + \
          "_" + date + "." + type
    filename = url.split("/")[-1]
    if not os.path.exists(os.path.join(NDOVDIR,filename)):
        with open(os.path.join(NDOVDIR, filename), "wb") as f:
            r = requests.get(url)
            f.write(r.content)
    return pd.read_csv(os.path.join(NDOVDIR, filename), low_memory=False, 
                       compression='gzip' if type == 'csv.gz' else 'zip')

########################################################################################################
############################### READ CROWDEDNESS INFORMATION ###########################################
########################################################################################################
druktedata = get_crowdedness_operator('ns', 'csv.gz', displaydate)
# druktedata = druktedata.append(get_crowdedness_operator('arr', 'zip', displaydate))
# druktedata = druktedata.append(get_crowdedness_operator('cxx', 'csv.gz', displaydate))
# druktedata = druktedata.append(get_crowdedness_operator('keolis', 'csv.zip', displaydate)) # includes 'Syntus'
druktedata = druktedata[['DataOwnerCode', 'JourneyNumber', 'OperatingDay', 'UserStopCodeBegin', 
                         'UserStopCodeEnd', 'VehicleType', 'TotalNumberOfCoaches', 'Occupancy']]
druktedata.columns=['operator', 'ritnumber', 'date', 'departure', 'arrival', 'wagontype', 'coaches', 'classification']
druktedata = druktedata[druktedata.departure.str.len() < 6] # Filter station abbrev
# Use an estimation of capacity per wagon based on wikipedia, just as rough estimation
druktedata = druktedata.merge(pd.DataFrame({'wagontype' : ['ICM', 'VIRM', 'SW7-25KV', 'SW9-25KV', 'SNG', 
                                                           'SLT', 'FLIRT FFF', 'SGMM', 'DDZ'],
                                            'seats' : [60, 100, 50, 50, 85, 60, 55, 45, 50]}))
druktedata['seats'] = druktedata['seats'] * druktedata['coaches']

########################################################################################################
######################################### DOWNLOAD GTFS ################################################
########################################################################################################
if not os.path.exists(os.path.join(GTFSDIR,'gtfs-nl.zip')):
    url = 'http://gtfs.ovapi.nl/nl/gtfs-nl.zip'
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(os.path.join(GTFSDIR,'gtfs-nl.zip'), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1073741824):
                f.write(chunk)
                
########################################################################################################
############################# READ GTFS AND FILTER ON AGENCIES #########################################
########################################################################################################
def read_csv(csvfile):
    with zipfile.ZipFile(os.path.join(GTFSDIR,'gtfs-nl.zip')) as z:
        with z.open(csvfile) as f:
            return pd.read_csv(f)

#AGENCYNAMES = ['NS', 'ARR', 'CXX', 'KEOLIS', 'SYNTUS']
AGENCYNAMES = ['NS']
agencies = read_csv('agency.txt')
agency_ids = agencies[agencies.agency_name.isin(AGENCYNAMES)]['agency_id'].values
agencies = agencies[agencies.agency_name.isin(AGENCYNAMES)][['agency_id', 'agency_name']]

routes = read_csv('routes.txt')
routes = routes[routes.agency_id.isin(agency_ids)]
route_ids = routes.route_id.values
routes = routes[['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type']]

trips = read_csv('trips.txt')
trips = trips[trips.route_id.isin(route_ids)]
trip_ids = trips.trip_id.values
service_ids = trips.service_id.values
trips = trips[['route_id', 'service_id', 'trip_id', 'trip_headsign', 'trip_short_name', 'trip_long_name', 'direction_id', 'shape_id']] 
trips.trip_short_name = trips.trip_short_name.astype(int)
trips.shape_id = trips.shape_id.astype('Int64')

calendar = read_csv('calendar_dates.txt')
calendar[calendar.service_id.isin(service_ids)]
calendar.date = calendar.date.astype(str)

trips = trips.merge(calendar[['service_id', 'date']], on='service_id')

stoptimes = read_csv('stop_times.txt')
stoptimes = stoptimes[stoptimes.trip_id.isin(trip_ids)]
stoptimes.stop_id = stoptimes.stop_id.astype(str)
stop_ids = stoptimes.stop_id.unique()
stoptimes = stoptimes[['trip_id', 'stop_sequence', 'stop_id', 'arrival_time', 'departure_time', 'shape_dist_traveled']]
stoptimes.arrival_time = stoptimes.arrival_time.apply(lambda x: str2min(x))
stoptimes.departure_time = stoptimes.departure_time.apply(lambda x: str2min(x))

# First get the stops (platforms)
stops_full = read_csv('stops.txt')
stops_full.stop_id = stops_full.stop_id.astype(str)
stops = stops_full[stops_full.stop_id.isin(stop_ids)].copy()

# Now add the stopareas (stations)
stopareas = stops.parent_station.unique()
stops = stops.append(stops_full[stops_full.stop_id.isin(stopareas)].copy())

stops.zone_id = stops.zone_id.str.replace('IFF:', '').str.upper()
stops.stop_code = stops.stop_code.str.upper()
stops = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'parent_station', 'platform_code', 'stop_code', 'zone_id']]

stops.loc[stops['zone_id'].isnull(),'zone_id'] = stops['stop_code']
stops.loc[stops['stop_code'].isnull(),'stop_code'] = stops['zone_id']

# shapes = pd.read_csv(os.path.join(GTFSDIR, 'shapes.txt'))
shapes = read_csv('shapes.txt')

########################################################################################################
################### DETERMINE FOR ALL TRIPS FOR EVERY MINUTE THE LOCATION ##############################
########################################################################################################
def interpolate_lat_lon(tripshape, dist):
        ps = tripshape[tripshape['shape_dist_traveled'].le(dist)].index[-1]
        ns = tripshape[tripshape['shape_dist_traveled'].ge(dist)].index[0]
        lat1 = tripshape[tripshape.index == ps].iloc[0]['shape_pt_lat']
        lon1 = tripshape[tripshape.index == ps].iloc[0]['shape_pt_lon']
        lat2 = tripshape[tripshape.index == ns].iloc[0]['shape_pt_lat']
        lon2 = tripshape[tripshape.index == ns].iloc[0]['shape_pt_lon']
        dst1 = tripshape[tripshape.index == ps].iloc[0]['shape_dist_traveled']
        dst2 = tripshape[tripshape.index == ns].iloc[0]['shape_dist_traveled']
        prc = ((dist - dst1) / (dst2 - dst1)) if dst2 != dst1 else 1
        lat = lat1 + prc * (lat2 - lat1)
        lon = lon1 + prc * (lon2 - lon1)
        return lat, lon

def get_trip_data(ritnumber, ritdate):
    trip = trips[(trips.trip_short_name == ritnumber) & (trips.date == ritdate)].iloc[0]
    tripid = trip['trip_id']
    shapeid = trip['shape_id']
    tripshape = shapes[shapes.shape_id == shapeid]
    tripstops = stoptimes[stoptimes.trip_id == tripid].sort_values('stop_sequence')

    lastdep = -1
    lastdist= -1
    seq = 0
    dl = []

    for position in tripstops.iterrows():
        arr = position[1][3]
        dep = position[1][4]
        dist = position[1][5]
        dist = dist + 0.1 if dist == lastdist else dist
        arr = arr + 1 if arr == lastdep else arr
        stop = position[1][2]
        if (dist == dist) & (arr == arr):
            if lastdep > 0:
                seq = seq+1
                stepsize = ((dist-lastdist) / (arr-lastdep))
                for t in range(lastdep+1, arr):
                    dst = int(lastdist + ( (t - lastdep) * stepsize))
                    # prc = (dst - lastdist) / (dist - lastdist)
                    lat, lon = interpolate_lat_lon(tripshape, dst)
                    # lat, lon = interpolate_lat_lon(tripshape, dst, prc)
                    # print('{:2d}: {:4d} - {:4.2f} - {:8d} - ({:6.4f} , {:6.4f})'.format(seq, t, prc, dst, lat, lon))
                    dl.append({'trip_id': tripid, 'time': int(t), 'lat': lat, 'lon': lon, 'stop_id': stop,
                                    'ritnumber' : ritnumber, 'sequence' : seq})
            seq = seq+1
            for t in range(arr, dep+1):
                dst = int(dist)
                # prc = 1
                lat, lon = interpolate_lat_lon(tripshape, dst)
                # lat, lon = interpolate_lat_lon(tripshape, dst, prc)
                dl.append({'trip_id': tripid, 'time': int(t), 'lat': lat, 'lon': lon, 'stop_id': stop,
                                'ritnumber' : ritnumber, 'sequence' : seq})
            lastdep = dep
            lastdist = dist
    df = pd.DataFrame(dl)
    if 'trip_id' in df.columns :
        df.trip_id = df.trip_id.astype(int)
        df.sequence = df.sequence.astype(int)
        df.time = df.time.astype(int)
        df.ritnumber = df.ritnumber.astype(int)
        df.stop_id = df.stop_id.astype(int).astype(str)
        df = df[['trip_id', 'ritnumber', 'sequence', 'time', 'stop_id', 'lat', 'lon']]
#         df = df.merge(stops[['stop_id', 'stop_code', 'stop_name']])
        return df
    else:
        return None

timedata = pd.DataFrame()
mask1 = (trips.trip_short_name > 0) & (trips.trip_short_name < 999999)
# mask1 = (trips.trip_short_name > 3000) & (trips.trip_short_name < 4000)
mask2 = trips.date == displaydate
total = len(trips[mask1 & mask2].trip_short_name.unique())
i = 0
for r in trips[mask1 & mask2].trip_short_name.unique():
    df_tmp = get_trip_data(ritnumber = r, ritdate = displaydate)
    if df_tmp is not None:
        timedata = timedata.append(df_tmp)
        update_progress(i, total=total, barLength=50)
    i += 1
timedata = timedata.merge(stops[['stop_id', 'stop_code', 'stop_name']])

########################################################################################################
############################# COMBINE TRIPDATA WITH CROWDEDNESS ########################################
########################################################################################################
timedata = timedata.merge(druktedata[['ritnumber', 'departure', 'classification', 'seats', 'operator']], 
               left_on=['ritnumber', 'stop_code'], right_on=['ritnumber', 'departure'])

timedata.time = timedata.time.apply(lambda x: min2str(x))
timedata['passengers'] = ((timedata.classification - 1) * timedata.seats * 0.33).astype(int)
timedata['timestamp'] = (pd.to_datetime(timedata['time']) - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
timedata['elevation'] = 0
timedata = timedata.drop(columns=['trip_id'])

########################################################################################################
################################ EXPORT GEOJSON FOR ANIMATION #########################################
########################################################################################################
features=[]
total = len(timedata.ritnumber.unique())
i=0
for rn in timedata.ritnumber.unique():
    i+= 1
    total_trip = timedata[timedata.ritnumber == rn].dropna()
    for sq in total_trip.sequence.unique():
        trip = total_trip[total_trip.sequence == sq]
        tripfirst = trip.iloc[0]
        triplast = trip.iloc[-1]
        name = str(tripfirst["ritnumber"]) + " : " + tripfirst["stop_code"] + " - " + triplast["stop_code"]
        features.append(
            Feature(geometry=LineString(trip[['lon', 'lat', 'elevation', 'timestamp']].to_numpy().tolist()) ,
                                        properties=dict(label=str(tripfirst["ritnumber"]),
                                                        name=name,
                                                        capacity=str(tripfirst["seats"]),
                                                        passengers=str(tripfirst["passengers"]),
                                                        crowding=str(tripfirst["classification"])))    
        )
    update_progress(i, total=total, barLength=50) 

feature_collection = FeatureCollection(features)
with open('trainpag.geojson', 'w') as outfile:
        dump(feature_collection, outfile)
    
########################################################################################################
############################### EXPORT GTFS WITH TRAIN NETWORK #########################################
########################################################################################################
network = trips.drop_duplicates('route_id').dropna()
network = network[~(network.trip_long_name == 'Stopbus i.p.v. trein')]

features=[]
total = len(network.shape_id.unique())
i=0
for si in network.shape_id.unique():
    i+= 1
    section = shapes[shapes.shape_id == si]
    features.append(
        Feature(geometry=LineString(section[['shape_pt_lon', 'shape_pt_lat']].to_numpy().tolist()) ,
                                    properties=dict(label=str(si)))
    )
    update_progress(i, total=total, barLength=50) 

feature_collection = FeatureCollection(features)
with open('network.geojson', 'w') as outfile:
        dump(feature_collection, outfile)
    
