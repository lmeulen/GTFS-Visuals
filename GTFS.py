import os
import zipfile
import pandas as pd
import geopandas as gpd
import urllib.request
import gtfs_realtime_OVapi_pb2  # Neccessary to find additional field of OVapi
import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict
from datetime import datetime, timedelta


class GTFS:
    stops = None
    routes = None
    trips = None
    calendar = None
    stoptimes = None
    trip_updates = None
    train_updates = None
    vehicle_positions = None
    alerts = None
    alerts_to_routes = None
    alerts_to_stops = None
    date = None

    def file_age(self, filename):
        """
        Return file age in minutes if the file is from the current day
        Returns 99999 if file does not exist or is from yestrday or before
        """
        if os.path.exists(filename):
            yesterday = datetime.now().date() + timedelta(days=-1)
            filedate = datetime.fromtimestamp(os.path.getmtime(filename)).date()
            if filedate <= yesterday:
                return 9999  # Files from yesterday are always to old
            return int((datetime.now().timestamp() - os.path.getmtime(filename)) / 60)
        else:
            return 99999

    def download_url(self, url, filename, max_days=0, max_minutes=0):
        """
        Download the given URL and save under filename.
        If the filename contains a directory, it is assoumed the directory exists.

        :param str url: The URL to download
        :param str filename: The filename to save the file under
        :param int max_days: Maximum age in days the already downloaded file may be
        :param int max_minutes: Maximum age in minutes the already downloaded file may be
        """
        max_age = max_days * 1440 + max_minutes
        if os.path.exists(filename) and self.file_age(filename) < max_age:
            # Cached version exists and is still valid
            # print('Using existing file {}'.format(filename))
            return
        try:
            # print('Downloading new file from {}'.format(url))
            with urllib.request.urlopen(url) as resp:
                file_content = resp.read()
            with open(filename, 'wb') as f:
                f.write(file_content)
        except Exception as e:
            print(e)

    def read_from_zip(self, zipfn, csvfile, cols=None, types=None):
        with zipfile.ZipFile(zipfn) as z:
            with z.open(csvfile) as f:
                return pd.read_csv(f, usecols=cols, dtype=types)

    def update_static(self, day=None):
        if not day:
            day = datetime.now().strftime('%Y-%m-%d')

        self.download_url('http://gtfs.ovapi.nl/nl/gtfs-nl.zip', 'gtfs-nl.zip', max_days=7)

        fn = 'stops.pcl'
        if self.file_age(fn) < 24 * 60:
            self.stops = pd.read_pickle(fn)
        else:
            cols = ['stop_id', 'stop_code', 'stop_name', 'stop_lat', 'stop_lon',
                    'location_type', 'parent_station', 'platform_code', 'zone_id']
            types = {'stop_lat': float, 'stop_lon': float, 'location_type': int, 'stop_id': str}
            self.stops = self.read_from_zip('gtfs-nl.zip', 'stops.txt', cols, types)
            self.stops = gpd.GeoDataFrame(self.stops,
                                          geometry=gpd.points_from_xy(self.stops.stop_lon, self.stops.stop_lat))
            self.stops.to_pickle(fn)
        print('\nStops             : {}'.format(len(self.stops)))

        fn = 'routes.pcl'
        if self.file_age(fn) < 24 * 60:
            self.routes = pd.read_pickle(fn)
        else:
            cols = ['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type']
            types = {'route_id': str}
            self.routes = self.read_from_zip('gtfs-nl.zip', 'routes.txt', cols, types)
            self.routes.to_pickle(fn)
        print('Routes            : {}'.format(len(self.routes)))

        fn = 'calendar.pcl'
        if self.file_age(fn) < 24 * 60:
            self.calendar = pd.read_pickle(fn)
        else:
            types = {'date': str}
            self.calendar = self.read_from_zip('gtfs-nl.zip', 'calendar_dates.txt', types=types)
            self.calendar['date'] = self.calendar.date.str[:4] + '-' + \
                                    self.calendar.date.str[4:6] + '-' + self.calendar.date.str[6:8]
            self.calendar = self.calendar[self.calendar.date == day]
            self.calendar.to_pickle(fn)
        self.date = datetime.strptime(self.calendar.iloc[0, 1], "%Y-%m-%d")
        print('Services          : {}'.format(len(self.calendar)))

        fn = 'trips.pcl'
        if self.file_age(fn) < 24 * 60:
            self.trips = pd.read_pickle(fn)
        else:
            cols = ['route_id', 'service_id', 'trip_id', 'trip_headsign', 'trip_short_name',
                    'trip_long_name', 'direction_id', 'shape_id']
            types = {'trip_short_name': 'Int64', 'shape_id': 'Int64', 'trip_id': str, 'route_id': str}
            self.trips = self.read_from_zip('gtfs-nl.zip', 'trips.txt', cols, types)
            self.trips = self.trips.merge(self.routes)
            self.trips = self.trips.merge(self.calendar[['service_id', 'date']], on='service_id')
            self.trips.to_pickle(fn)
        print('Trips             : {}'.format(len(self.trips)))

        fn = 'stoptimes.pcl'
        if self.file_age(fn) < 24 * 60:
            self.stoptimes = pd.read_pickle(fn)
        else:
            tripids = self.trips.trip_id.values
            cols = ['trip_id', 'stop_sequence', 'stop_id', 'stop_headsign', 'arrival_time',
                    'departure_time', 'shape_dist_traveled']
            types = {'trip_id': str, 'stop_id': str}
            with zipfile.ZipFile('gtfs-nl.zip') as z:
                with z.open('stop_times.txt') as f:
                    iter_csv = pd.read_csv(f, usecols=cols, dtype=types, iterator=True, chunksize=10000)
                    self.stoptimes = pd.concat([chunk[chunk['trip_id'].isin(tripids)] for chunk in iter_csv])
                self.stoptimes.arrival_time = \
                    self.stoptimes.apply(lambda x: gtfs.businessday_to_datetime(time=x['arrival_time']), axis=1)
                self.stoptimes.departure_time = \
                    self.stoptimes.apply(lambda x: gtfs.businessday_to_datetime(time=x['departure_time']), axis=1)
            self.stoptimes.to_pickle(fn)
        print('Halteringen       : {}'.format(len(self.stoptimes)))

    def convert_times(self, df, columns):
        for c in columns:
            df[c] = pd.to_datetime(df[c], unit='s', utc=True).map(lambda x: x.tz_convert('Europe/Amsterdam'))
            df[c] = df[c].apply(lambda x: x.replace(tzinfo=None))
        return df

    def businessday_to_datetime(self, date=None, time=None):
        try:
            res = datetime.strptime(date, '%Y%m%d') if date else self.date
            hr = int(time[:2])
            if hr >= 24:
                res = res + timedelta(days=1)
                hr -= 24
            res = res + timedelta(hours=hr, minutes=int(time[3:5]), seconds=int(time[6:8]))
            return res
        except:
            return None

    def update_realtime(self, max_age=15):
        """
        Update the realtime information from GTFS
        :param max_age: Maxium age of cache file (in mnutes)
        :return:
        """
        feed = gtfs_realtime_pb2.FeedMessage()
        print()

        self.download_url('https://gtfs.ovapi.nl/nl/tripUpdates.pb', 'tripUpdates.pb', max_minutes=max_age)
        with open('tripUpdates.pb', 'rb') as file:
            data = file.read()
        feed.ParseFromString(data)
        tripUpdates = protobuf_to_dict(feed)
        print("Trip updates      : {}".format(len(tripUpdates['entity'])))

        self.download_url('https://gtfs.ovapi.nl/nl/trainUpdates.pb', 'trainUpdates.pb', max_minutes=max_age)
        with open('trainUpdates.pb', 'rb') as file:
            data = file.read()
        feed.ParseFromString(data)
        trainUpdates = protobuf_to_dict(feed)
        print("Train updates     : {}".format(len(trainUpdates['entity'])))

        self.download_url('https://gtfs.ovapi.nl/nl/vehiclePositions.pb', 'vehiclePositions.pb',
                          max_minutes=max_age)
        with open('vehiclePositions.pb', 'rb') as file:
            data = file.read()
        feed.ParseFromString(data)
        vehiclePositions = protobuf_to_dict(feed)
        print("Vehicle positions : {}".format(len(vehiclePositions['entity'])))

        self.download_url('https://gtfs.ovapi.nl/nl/alerts.pb', 'alerts.pb', max_minutes=max_age)
        with open('alerts.pb', 'rb') as file:
            data = file.read()
        feed.ParseFromString(data)
        alerts = protobuf_to_dict(feed)
        print("Alerts            : {}".format(len(alerts['entity'])))

        # Additional fields are stored under these keys, not the correct names
        rtid_keys = ['___X', '1003']
        stop_keys = ['___X', '1003']
        delay_keys = ['___X', '1003']

        ###
        # Trip updates
        ###
        uids = []
        rt_ids = []
        trip_ids = []
        start_times = []
        route_ids = []
        direction_ids = []
        vehicles = []
        stop_sequences = []
        arrival_times = []
        arrival_delays = []
        departure_times = []
        departure_delays = []
        timestamps = []

        timestamp = datetime.fromtimestamp(tripUpdates['header']['timestamp'])
        for tu in tripUpdates['entity']:
            uid = tu['id']
            trip_update = tu['trip_update']
            vehicle = trip_update['vehicle']['label'] if 'vehicle' in trip_update else None
            trip = trip_update['trip']
            trip_id = trip['trip_id']
            start_time = trip['start_time'] if 'start_time' in trip else None
            start_date = trip['start_date']
            start_time = self.businessday_to_datetime(start_date, start_time)
            route_id = trip['route_id']
            direction_id = int(trip['direction_id']) if 'direction_id' in trip else None
            rt_id = trip[rtid_keys[0]][rtid_keys[1]]['realtime_trip_id'] if rtid_keys[0] in trip else None
            for stu in trip_update['stop_time_update'] if 'stop_time_update' in trip_update else []:
                stop_sequence = stu['stop_sequence']
                if 'arrival' in stu:
                    arr = stu['arrival']
                    arrival_time = arr['time'] if 'time' in arr else None
                    arrival_time = datetime.fromtimestamp(arrival_time)
                    arrival_delay = arr['delay'] if 'delay' in arr else None
                else:
                    arrival_time = None
                    arrival_delay = None
                if 'departure' in stu:
                    dep = stu['departure']
                    departure_time = dep['time'] if 'time' in dep else None
                    departure_time = datetime.fromtimestamp(departure_time)
                    departure_delay = dep['delay'] if 'delay' in dep else None
                else:
                    departure_time = None
                    departure_delay = None
                ###
                uids.append(uid)
                rt_ids.append(rt_id)
                trip_ids.append(trip_id)
                start_times.append(start_time)
                route_ids.append(route_id)
                direction_ids.append(direction_id)
                vehicles.append(vehicle)
                stop_sequences.append(stop_sequence)
                arrival_times.append(arrival_time)
                arrival_delays.append(arrival_delay)
                departure_times.append(departure_time)
                departure_delays.append(departure_delay)
                timestamps.append(timestamp)
                ###
        df_trip_updates = pd.DataFrame({'id': uids, 'RT_id': rt_ids, 'trip_id': trip_ids, 'start_time': start_times,
                                        'route_id': route_ids, 'direction_id': direction_ids, 'vehicle': vehicles,
                                        'stop_sequence': stop_sequences, 'arrival_time': arrival_times,
                                        'arrival_delay': arrival_delays,
                                        'departure_time': departure_times, 'departure_delay': departure_delays,
                                        'timestamp': timestamps})
        self.trip_updates = df_trip_updates
        print("\nTrip updates      : {}".format(len(self.trip_updates)))

        ###
        # Train updates
        ###
        trip_ids = []
        rt_ids = []
        start_times = []
        route_ids = []
        direction_ids = []
        stop_ids = []
        arrival_times = []
        arrival_delays = []
        departure_times = []
        departure_delays = []
        timestamps = []
        station_ids = []
        train_numbers = []
        scheduled_tracks = []

        timestamp = trainUpdates['header']['timestamp']
        timestamp = datetime.fromtimestamp(timestamp)
        for tu in trainUpdates['entity']:
            trip_id = tu['id']
            trip_update = tu['trip_update']
            trip = trip_update['trip']
            route_id = trip['route_id'] if 'route_id' in trip else None
            direction_id = int(trip['direction_id'])
            additional = trip[rtid_keys[0]][rtid_keys[1]] if rtid_keys[0] in trip else None
            rt_id = additional['realtime_trip_id'] if 'realtime_trip_id' in additional else None
            train_number = additional['trip_short_name'] if 'trip_short_name' in additional else None
            start_time = trip['start_time']
            start_date = trip['start_date']
            start_time = self.businessday_to_datetime(start_date, start_time)
            if 'stop_time_update' in trip_update:
                for su in trip_update['stop_time_update']:
                    if 'stop_id' in su:
                        if 'arrival' in su:
                            arr = su['arrival']
                            arrival_time = arr['time'] if 'time' in arr else None
                            arrival_time = datetime.fromtimestamp(arrival_time)
                            arrival_delay = arr['delay'] if 'delay' in arr else None
                        else:
                            arrival_time = None
                            arrival_delay = None

                        if 'departure' in su:
                            dep = su['departure']
                            departure_time = dep['time'] if 'time' in dep else None
                            departure_time = datetime.fromtimestamp(departure_time)
                            departure_delay = dep['delay'] if 'delay' in dep else None
                        else:
                            departure_time = None
                            departure_delay = None
                        stop_id = su['stop_id']
                        additional = su[stop_keys[0]][stop_keys[1]] if stop_keys[0] in su else None
                        if additional:
                            scheduled_track = additional['scheduled_track'] if 'scheduled_track' in additional else None
                            station_id = additional['station_id'] if 'station_id' in additional else None
                        else:
                            scheduled_track = station_id = None
                        ###
                        trip_ids.append(trip_id)
                        rt_ids.append(rt_id)
                        start_times.append(start_time)
                        route_ids.append(route_id)
                        direction_ids.append(direction_id)
                        stop_ids.append(stop_id)
                        arrival_times.append(arrival_time)
                        arrival_delays.append(arrival_delay)
                        departure_times.append(departure_time)
                        departure_delays.append(departure_delay)
                        timestamps.append(timestamp)
                        station_ids.append(station_id)
                        train_numbers.append(train_number)
                        scheduled_tracks.append(scheduled_track)
                        ###
        df_train_updates = pd.DataFrame({'trip_id': trip_ids, 'RT_id': rt_ids, 'start_time': start_times,
                                         'route_id': route_ids, 'direction_id': direction_ids, 'stop_id': stop_ids,
                                         'arrival_time': arrival_times, 'arrival_delay': arrival_delays,
                                         'departure_time': departure_times, 'departure_delay': departure_delays,
                                         'timestamp': timestamps, 'station_id': station_ids,
                                         'train_number': train_numbers, 'scheduled_track': scheduled_tracks})
        df_train_updates['stop_id'] = df_train_updates['stop_id'].astype(str)
        df_train_updates['route_id'] = df_train_updates['route_id'].astype(str)
        df_train_updates['trip_id'] = df_train_updates['trip_id'].astype(str)
        self.train_updates = df_train_updates
        print("Train updates     : {}".format(len(self.train_updates)))

        ###
        # Vehicle positions
        ###
        vids = []
        rt_ids = []
        trip_ids = []
        start_times = []
        route_ids = []
        direction_ids = []
        latitudes = []
        longitudes = []
        current_stop_seqs = []
        timestamps = []
        labels = []
        delays = []
        for vp in vehiclePositions['entity']:
            vid = vp['id']
            vehicle = vp['vehicle']
            trip = vehicle['trip']
            trip_id = trip['trip_id']
            start_time = trip['start_time'] if 'start_time' in trip else None
            start_date = trip['start_date']
            start_time = self.businessday_to_datetime(start_date, start_time)
            route_id = trip['route_id']
            rt_id = trip[rtid_keys[0]][rtid_keys[1]]['realtime_trip_id'] if rtid_keys[0] in trip else None
            direction_id = int(trip['direction_id']) if 'direction_id' in trip else None
            latitude = vehicle['position']['latitude'] if 'position' in vehicle else None
            longitude = vehicle['position']['longitude'] if 'position' in vehicle else None
            current_stop_seq = vehicle['current_stop_sequence'] if 'current_stop_sequence' in vehicle else None
            timestamp = vehicle['timestamp']
            timestamp = datetime.fromtimestamp(timestamp)
            label = vehicle['vehicle']['label'] if 'vehicle' in vehicle else None
            delay = int(vehicle[delay_keys[0]][delay_keys[1]]['delay']) if delay_keys[0] in vehicle else None
            ###
            vids.append(vid)
            rt_ids.append(rt_id)
            trip_ids.append(trip_id)
            start_times.append(start_time)
            route_ids.append(route_id)
            direction_ids.append(direction_id)
            latitudes.append(latitude)
            longitudes.append(longitude)
            current_stop_seqs.append(current_stop_seq)
            timestamps.append(timestamp)
            labels.append(label)
            delays.append(delay)
            ###
        df_vehicle_positions = pd.DataFrame(
            {'id': vids, 'RT_id': rt_ids, 'trip_id': trip_ids, 'start_time': start_times,
             'route_id': route_ids, 'direction_id': direction_ids, 'latitude': latitudes,
             'longitude': longitudes, 'current_stop_seq': current_stop_seqs,
             'timestamp': timestamps, 'label': labels, 'delay': delays})
        df_vehicle_positions['current_stop_seq'] = df_vehicle_positions['current_stop_seq'].astype('Int64')
        df_vehicle_positions = gpd.GeoDataFrame(df_vehicle_positions,
                                                geometry=gpd.points_from_xy(df_vehicle_positions.longitude,
                                                                            df_vehicle_positions.latitude))
        self.vehicle_positions = df_vehicle_positions
        print("Vehicle positions : {}".format(len(self.vehicle_positions)))

        updates = []
        routemapping = []
        stopmapping = []
        timestamp = alerts['header']['timestamp']
        causes = {0: 'UNKNOWN_CAUSE', 1: 'OTHER_CAUSE', 2: 'TECHNICAL_PROBLEM', 3: 'STRIKE', 4: 'DEMONSTRATION',
                  5: 'ACCIDENT', 6: 'HOLIDAY', 7: 'WEATHER', 8: 'MAINTENANCE', 9: 'CONSTRUCTION',
                  10: 'POLICE_ACTIVITY', 11: 'MEDICAL_EMERGENCY'}
        effects = {-1: None, 0: 'NO_SERVICE', 1: 'REDUCED_SERVICE', 2: 'SIGNIFICANT_DELAYS', 3: 'DETOUR',
                   4: 'ADDITIONAL_SERVICE', 5: 'MODIFIED_SERVICE', 6: 'OTHER_EFFECT',
                   7: 'UNKNOWN_EFFECT', 8: 'STOP_MOVED'}
        for al in alerts['entity']:
            aid = al['id']
            alert = al['alert']
            cause = int(alert['cause']) if 'cause' in alert else 0
            effect = int(alert['effect']) if 'effect' in alert else -1
            header_text = alert['header_text']['translation'][0]['text']
            description_text = alert['description_text']['translation'][0]['text']
            for ap in alert['active_period']:
                start = ap['start']
                end = ap['end']
                if 'informed_entity' in alert:
                    for inf in alert['informed_entity']:
                        informed_stop = inf['stop_id']
                        stopmapping.append({'alert_id': aid, 'stop_id': informed_stop, 'start': start, 'end': end})
                        if 'route_id' in inf:
                            informed_route = inf['route_id']
                            routemapping.append({'alert_id': aid, 'route_id': int(informed_route),
                                                 'start': start, 'end': end})
                updates.append({'id': aid, 'timestamp': timestamp,
                                'cause_id': cause, 'cause': causes[cause], 'effect_id': effect,
                                'effect': effects[effect], 'start': start, 'end': end,
                                'header': header_text, 'description': description_text})
        df_alerts = pd.DataFrame(updates)
        df_alerts = self.convert_times(df_alerts, ['timestamp', 'start', 'end'])
        df_alerts_to_stops = pd.DataFrame(stopmapping)
        df_alerts_to_stops = self.convert_times(df_alerts_to_stops, ['start', 'end'])
        df_alerts_to_routes = pd.DataFrame(routemapping)
        df_alerts_to_routes = self.convert_times(df_alerts_to_routes, ['start', 'end'])
        self.alerts = df_alerts
        self.alerts_to_routes = df_alerts_to_routes
        self.alerts_to_stops = df_alerts_to_stops
        print("Alerts            : {}".format(len(self.alerts)))

    def get_train_departures_at(self, station):
        df = self.train_updates.merge(self.stops, on='stop_id')
        df = df.merge(self.trips, on='trip_id')
        df = df[df.stop_name == station]
        df = df.loc[df.departure_time.notnull()].sort_values('departure_time')
        df = df[df.platform_code.notnull() & (df['departure_time'] > datetime.now())]
        df['departure_time'] = df['departure_time'].apply(lambda x: x.replace(tzinfo=None))
        return df

    def get_departures_at(self, halte):
        df = self.trip_updates.merge(self.stoptimes[['trip_id', 'stop_sequence', 'stop_id']])
        df = df.merge(self.stops, on='stop_id')
        df = df.merge(self.trips, on='trip_id')
        df = df[df.stop_name.str.contains(halte)]
        return df

    def get_planned_stops(self, halte):
        planned = pd.merge(
            self.stoptimes[['trip_id', 'stop_sequence', 'stop_id', 'arrival_time',
                            'departure_time']],
            self.stops[(self.stops.stop_name.str.contains(halte)) & (self.stops.stop_code.notnull())]
            [['stop_id', 'stop_code', 'stop_name', 'platform_code']],
            on=['stop_id']
        )
        planned = planned.merge(self.trips[['trip_id', 'route_short_name', 'trip_short_name', 'trip_headsign']])
        planned = planned[['stop_id', 'stop_code', 'stop_name', 'platform_code', 'trip_id', 'stop_sequence',
                           'route_short_name', 'trip_short_name', 'trip_headsign', 'arrival_time', 'departure_time']]
        return planned

    def get_actual_stops(self, halte):
        actual = self.stops[['stop_id', 'stop_code', 'stop_name']][self.stops.stop_name.str.contains(halte)]
        actual = actual.merge(self.stoptimes[['trip_id', 'stop_sequence', 'stop_id']])
        actual = actual.merge(self.trip_updates[['RT_id', 'trip_id', 'route_id', 'direction_id', 'stop_sequence',
                                                 'arrival_time', 'departure_time', 'departure_delay']])
        actual = actual.merge(self.trips[['trip_id', 'route_short_name', 'trip_headsign']])
        actual = actual[['stop_id', 'stop_code', 'stop_name', 'trip_id', 'stop_sequence',
                         'route_short_name', 'trip_headsign', 'arrival_time', 'departure_time',
                         'departure_delay', 'RT_id']]
        return actual

    def get_departures_at_stop(self, halte):
        planned = self.get_planned_stops(halte)
        actual = self.get_actual_stops(halte)
        tt = planned.merge(actual[['trip_id', 'departure_time', 'departure_delay', 'RT_id']],
                           on='trip_id', how='left', suffixes=['_planned', '_actual'])
        tt.departure_time_actual.fillna(tt.departure_time_planned, inplace=True)
        tt.departure_delay.fillna(0, inplace=True)
        tt['RT'] = tt.RT_id.notnull()
        tt = tt.drop(columns=['stop_code', 'RT_id', 'arrival_time', 'departure_time_planned'])
        tt = tt.rename(columns={'departure_time_actual': 'departure_time'})
        return tt.sort_values('departure_time')
