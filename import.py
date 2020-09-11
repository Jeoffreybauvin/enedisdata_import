#!/usr/bin/env python3

import sys
import requests
import json
from ratelimit import limits, sleep_and_retry
from influxdb import InfluxDBClient
import datetime
import argparse
import logging


# Rate limiting is mandatory
MAX_HITS_PER_SECOND=5
ONE_SECOND=1
DATAHUB_HOST = 'enedis.valent1.fr'
DATAHUB_SCHEME = 'https'
TYPES = (
    'consumption_load_curve',
    'consumption_max_power',
    'daily_consumption',
    'production_load_curve',
    'daily_production',
    'identity',
    'contact_data',
    'contracts',
    'addresses',
)

parser = argparse.ArgumentParser()

parser.add_argument("--influxdb-host", help="InfluxDB host", dest="INFLUXDB_HOST", default="influxdb-api.loc")
parser.add_argument("--influxdb-port", help="InfluxDB port", dest="INFLUXDB_PORT", default=8086)
parser.add_argument("--influxdb-username", help="InfluxDB username", dest="INFLUXDB_USERNAME", default="username")
parser.add_argument("--influxdb-password", help="InfluxDB password", dest="INFLUXDB_PASSWORD", default="password")
parser.add_argument("--influxdb-database", help="InfluxDB database", dest="INFLUXDB_DATABASE", default="database")
parser.add_argument("--usage-point-id", help="Your usage point id", dest="USAGE_POINT_ID", default="00000000")
parser.add_argument("--auth-token", help="Authentication Token", dest="AUTH_TOKEN", default="mytoken")
parser.add_argument("-v", "--verbose", dest="verbose_count", action="count", default=0, help="increases log verbosity for each occurence.")

args = parser.parse_args()

log = logging.getLogger()

logging.basicConfig(stream=sys.stderr, level=logging.WARNING, format='%(name)s (%(levelname)s): %(message)s')
log.setLevel(max(3 - args.verbose_count, 0) * 10)

def _dayToStr(date):
    return date.strftime("%d/%m/%Y")

@sleep_and_retry
@limits(calls=MAX_HITS_PER_SECOND, period=ONE_SECOND)
def call_enedis(type=False, usage_point_id=False, start=False, end=False):

    if type not in TYPES:
        print('Error, this type is not supported')
        return  False

    headers = {
        'Content-Type': 'application/json',
        'Authorization': AUTH_TOKEN
    }

    payload = {
        'type': type,
        'usage_point_id': usage_point_id,
        'start': start,
        'end': end
    }

    url = '{}://{}/api'.format(DATAHUB_SCHEME, DATAHUB_HOST)

    r = requests.post(
        url,
        headers=headers,
        data=json.dumps(payload),
    )

    return r.json()

influx_client = InfluxDBClient(
    host=INFLUXDB_HOST,
    port=INFLUXDB_PORT,
    username=INFLUXDB_USERNAME,
    password=INFLUXDB_PASSWORD,
    database=INFLUXDB_DATABASE,
)

jsonInflux = []

type_requested = 'daily_consumption'

result = call_enedis(
    type=type_requested,
    usage_point_id=USAGE_POINT_ID,
    start='2020-01-01',
    end='2020-09-08'
)

if not result:
    print('error')
    sys.exit(2)


if type_requested == 'daily_consumption':

    if 'meter_reading' not in result:
        print('error in result')
        sys.exit(2)

    if 'interval_reading' not in result['meter_reading']:
        print('error in result')
        sys.exit(2)

    for data in result['meter_reading']['interval_reading']:
        date_time_obj = datetime.datetime.strptime(data['date'], '%Y-%m-%d')
        now = datetime.datetime.now()

        jsonInflux.append({
            "measurement": "enedis_consumption_per_day",
            "tags": {
            },
            "time": date_time_obj.strftime('%Y-%m-%dT%H:%M:%S'),
            "fields": {
                "value": int(data['value']),
            }
        })

print('Pushing data to influxdb')
influx_client.write_points(jsonInflux, batch_size=10)
