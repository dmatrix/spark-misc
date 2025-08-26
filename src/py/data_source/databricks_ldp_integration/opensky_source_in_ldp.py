import dlt

import requests
import time
import datetime

from pyspark.sql.datasource import SimpleDataSourceStreamReader, DataSource
from pyspark.sql.types import *


DS_NAME = "opensky-flights"

class OpenSkyStreamReader(SimpleDataSourceStreamReader):
    def initialOffset(self):
        return {'last_fetch': 0}

    def __init__(self, schema, options):
        super().__init__()
        self.schema = schema
        self.options = options
        self.session = requests.Session()
        self.EUROPE_BBOX = (35.0, 72.0, -25.0, 45.0)

    def read(self, start):
        try:
            time.sleep(float(self.options.get("interval", "10")))
            response = self._fetch_states()
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Error: {str(e)}")
            return ([], start)

        return (
            [self.parse_state(s, data['time']) for s in data.get('states', []) if self.valid_state(s)],
            {'last_fetch': data.get('time', int(time.time()))}
        )

    def valid_state(self, state):
        return (len(state) >= 17 and 
                state[5] is not None and 
                state[6] is not None and
                state[7] is not None and 
                state[9] is not None)

    def parse_state(self, state, timestamp):
        return (
            datetime.datetime.utcfromtimestamp(timestamp),
            state[0],  # icao24
            (state[1] or "").strip(),
            state[2],  # country
            float(state[5]),  # longitude
            float(state[6]),  # latitude
            float(state[7]),  # altitude
            float(state[9]),  # velocity
            float(state[11] or 0),  # vertical rate
            bool(state[8])  # on_ground
        )

    def _fetch_states(self):
        url = "https://opensky-network.org/api/states/all"
        EUROPE = {
            'lamin': self.EUROPE_BBOX[0],
            'lamax': self.EUROPE_BBOX[1],
            'lomin': self.EUROPE_BBOX[2],
            'lomax': self.EUROPE_BBOX[3]
        }
        return self.session.get(url, params=EUROPE, timeout=10)

class OpenSkyDataSource(DataSource):
    def __init__(self, options=dict()):
        super().__init__(options)
        self.options = options

    @classmethod
    def name(cls):
        return DS_NAME

    def schema(self):
        return StructType([
            StructField("timestamp", TimestampType()),
            StructField("icao24", StringType()),
            StructField("callsign", StringType()),
            StructField("country", StringType()),
            StructField("longitude", DoubleType()),
            StructField("latitude", DoubleType()),
            StructField("altitude_m", DoubleType()),
            StructField("velocity_ms", DoubleType()),
            StructField("vertical_rate", DoubleType()),
            StructField("on_ground", BooleanType())
        ])

    def simpleStreamReader(self, schema):
        return OpenSkyStreamReader(schema, self.options)
     
spark.dataSource.register(OpenSkyDataSource)

@dlt.table
def opensky_flights():
    return spark.readStream.format("opensky-flights").load()
