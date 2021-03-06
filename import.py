#!/usr/bin/env python3

import sys
import requests
import json
from ratelimit import limits, sleep_and_retry
from influxdb import InfluxDBClient
import datetime
import argparse
import logging
import re
import decimal
from decimal import Decimal


# Rate limiting is mandatory
MAX_HITS_PER_SECOND = 5
ONE_SECOND = 1
DATAHUB_HOST = 'enedis.valent1.fr'
DATAHUB_SCHEME = 'https'
TYPES = (
    'consumption_load_curve',
    'daily_consumption_max_power',
    'daily_consumption',
    'production_load_curve',
    'daily_production',
    'identity',
    'contact_data',
    'contracts',
    'addresses',
)

parser = argparse.ArgumentParser()

parser.add_argument("--type", help="Type", dest="TYPE",
                    default="consumption_load_curve", required=True)
parser.add_argument("--influxdb-host", help="InfluxDB host",
                    dest="INFLUXDB_HOST", default="influxdb-api.loc")
parser.add_argument("--influxdb-port", help="InfluxDB port",
                    dest="INFLUXDB_PORT", default=8086)
parser.add_argument("--influxdb-username", help="InfluxDB username",
                    dest="INFLUXDB_USERNAME", default="username")
parser.add_argument("--influxdb-password", help="InfluxDB password",
                    dest="INFLUXDB_PASSWORD", default="password")
parser.add_argument("--influxdb-database", help="InfluxDB database",
                    dest="INFLUXDB_DATABASE", default="enedis")
parser.add_argument("--usage-point-id", help="Your usage point id",
                    dest="USAGE_POINT_ID", default="00000000", required=True)
parser.add_argument("--auth-token", help="Authentication Token",
                    dest="AUTH_TOKEN", default="mytoken", required=True)
parser.add_argument("--start-date", help="Start date",
                    dest="START_DATE", default="2020-09-08", required=True)
parser.add_argument("--end-date", help="End date",
                    dest="END_DATE", default="2020-09-09", required=True)
parser.add_argument("-v", "--verbose", dest="verbose_count", action="count",
                    default=0, help="increases log verbosity")

args = parser.parse_args()
log = logging.getLogger()

logging.basicConfig(stream=sys.stderr, level=logging.WARNING,
                    format='%(name)s (%(levelname)s): %(message)s')
log.setLevel(max(3 - args.verbose_count, 0) * 10)


def _dayToStr(date):
    return date.strftime("%d/%m/%Y")


@sleep_and_retry
@limits(calls=MAX_HITS_PER_SECOND, period=ONE_SECOND)
def call_enedis(type=False, usage_point_id=False, start=False, end=False):

    if type not in TYPES:
        print('Error, this type is not supported')
        return False

    headers = {
        'Content-Type': 'application/json',
        'Authorization': args.AUTH_TOKEN
    }

    payload = {
        'type': type,
        'usage_point_id': usage_point_id,
    }

    if start:
        payload['start'] = start

    if end:
        payload['end'] = end

    log.debug(payload)

    url = '{}://{}/api'.format(DATAHUB_SCHEME, DATAHUB_HOST)

    r = requests.post(
        url,
        headers=headers,
        data=json.dumps(payload),
    )

    if r.status_code != 200:
        log.error(r.text)
        return False

    return r.json()


influx_client = InfluxDBClient(
    host=args.INFLUXDB_HOST,
    port=args.INFLUXDB_PORT,
    username=args.INFLUXDB_USERNAME,
    password=args.INFLUXDB_PASSWORD,
    database=args.INFLUXDB_DATABASE,
    timeout=5,
    retries=2,
)

# Initially, request contracts to have the max allowed consumtpion

contracts = call_enedis(
    type='contracts',
    usage_point_id=args.USAGE_POINT_ID,
)

max_consumption_allowed = 0
for c in contracts['customer']['usage_points']:
    log.debug(c)
    if(c['usage_point']['usage_point_id'] == args.USAGE_POINT_ID):
        # This is the one
        subscribed_power_str = re.search('^([0-9]*) kVA', c['contracts']['subscribed_power'])
        subscribed_power = int(subscribed_power_str.group(1))


jsonInflux = []

type_requested = args.TYPE

result = call_enedis(
    type=type_requested,
    usage_point_id=args.USAGE_POINT_ID,
    start=args.START_DATE,
    end=args.END_DATE
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
        log.debug(date_time_obj.strftime('%Y-%m-%dT%H:%M:%S')  + ' - ' + data['value'])

        jsonInflux.append({
            "measurement": "enedis_consumption_per_day",
            "tags": {
            },
            "time": date_time_obj.strftime('%Y-%m-%dT%H:%M:%S'),
            "fields": {
                "value": int(data['value']),
                "subscribed_power": subscribed_power,
            }
        })

elif type_requested == 'consumption_load_curve':
    # Not working right now

    if 'meter_reading' not in result:
        print('error in result')
        sys.exit(2)

    if 'interval_reading' not in result['meter_reading']:
        print('error in result')
        sys.exit(2)

    for data in result['meter_reading']['interval_reading']:
        log.debug(data)
        date_time_obj = datetime.datetime.strptime(data['date'], '%Y-%m-%d %H:%M:%S') + datetime.timedelta(minutes=1)
        now = datetime.datetime.now()
        # log.debug(date_time_obj.strftime('%Y-%m-%dT%H:%M:%S')  + ' - ' + data['value'])

        value = float(data['value']) /1000
        decimal.getcontext().rounding = decimal.ROUND_HALF_UP
        right_rounding = Decimal(value).quantize(Decimal("1.0"))

        jsonInflux.append({
            "measurement": "consumption_load_curve",
            "tags": {
            },
            "time": date_time_obj.strftime('%Y-%m-%dT%H:%M:%S'),
            "fields": {
                "value": right_rounding,
                "interval_length": str(data['interval_length']),
                "measure_type": str(data['measure_type']),
                "subscribed_power": subscribed_power,
            }
        })


print('Pushing data to influxdb')
influx_client.write_points(jsonInflux, batch_size=10)
