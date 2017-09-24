#!/usr/bin/env python

import io
import os
import sys
import json
import csv
import gzip
import shutil
import threading
import http.client
import urllib
import datetime
import uuid

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer
import boto3
import tempfile



from collections import defaultdict, MutableMapping

s3 = boto3.resource('s3')
logger = singer.get_logger()

TMPDIR = tempfile.mkdtemp()
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
DATE_FMT = "%Y-%m-%d"

REQUIRED_CONFIG_KEYS = [
    'bucket',
    'path',
    'delimiter',
    'quotechar',
]


def dtstr_to_dt(d_str):
    return datetime.datetime.strptime(d_str, DATETIME_FMT)


def datestr_to_dt(d_str):
    return datetime.datetime.strptime(d_str, DATE_FMT)


def now_to_date():
    return datetime.datetime.utcnow().strftime(DATE_FMT)


def ts_to_date(ts):
    return datetime.datetime.fromtimestamp(
        int(ts)).strftime(DATE_FMT)


def ts_to_dt(ts):
    return datetime.datetime.fromtimestamp(
        int(ts)).strftime(DATETIME_FMT)


def write_last_state(states):
    logger.info(
        'Persisted batch of {} records to Stitch'.format(len(states)))
    last_state = None
    for state in reversed(states):
        if state is not None:
            last_state = state
            break
    if last_state:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))

    return dict(items)


def load_json(record):
    try:
        return json.loads(record)
    except json.decoder.JSONDecodeError:
        logger.error("Unable to parse:\n{}".format(record))
        raise

def process_state(message):
    logger.debug('Setting state to {}'.format(message.value))
    return message.value


def process_schema(msg, validators, schemas, key_properties):
    if not msg.stream:
        raise Exception(
            "Line is missing required key 'stream': {}".format(msg))

    stream = msg.stream
    schemas[stream] = msg.schema
    validators[stream] = Draft4Validator(msg.schema)

    if not msg.key_properties:
        raise Exception("key_properties field is required")
    key_properties[stream] = msg.key_properties


def parse_record(msg, schemas, validators):
    """
    Validates record and returns flattened recorded
    """
    if msg.stream not in schemas:
        raise Exception(
            "A record for stream {} was encountered "
            "before a corresponding schema".format(
                msg.stream))

    validators[msg.stream].validate(msg.record)

    return flatten(msg.record)


def create_partition_str(date):
    dt = datestr_to_dt(date)
    year = dt.strftime('%Y')
    month = dt.strftime('%m')
    day = dt.strftime('%d')
    return 'year={}/month={}/day={}'.format(year, month, day)


def create_partitions(partition_date, should_partition=False):
    if should_partition:
        return create_partition_str(partition_date)
    else:
        return create_partition_str(now_to_date())


def write_to_s3(stream, file_path, config, partition_date):
    partitions = create_partitions(partition_date,
                                   stream in config['partition_streams'])

    key_name = '{path}/{integration}/{stream}/{partitions}/{name}'.format(
        path=config['path'],
        integration=config['integration'],
        partitions=partitions,
        stream=stream,
        name=file_path.split('/')[-1]
    )
    return s3.Object(config['bucket'], key_name).upload_file(file_path)


def save_temp_csv(tmpdirname, records, stream, config, date):
    f_date = date if date else now_to_date()

    file_name = '{}/{}_{}_{}.tsv'.format(
        tmpdirname, stream, f_date, str(uuid.uuid4()))

    f = open(file_name, 'a')
    writer = csv.DictWriter(f,
                            sorted(records[stream][0].keys()),
                            extrasaction='ignore',
                            delimiter=config['delimiter'],
                            quotechar=config['quotechar'])

    writer.writeheader()

    for row in records[stream]:
        writer.writerow(row)

    f.close()

    return file_name


def prepare_for_s3(records, config, date):
    for stream in records:

        with tempfile.TemporaryDirectory() as tmpdirname:
            file_name = save_temp_csv(tmpdirname,
                                      records, stream, config, date)

            with open(file_name, 'rb') as f_in, \
                    gzip.open(file_name + '.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

                file_to_save = file_name + '.gz' if \
                    os.stat(file_name + '.gz').st_size else file_name

                write_to_s3(
                    stream, file_to_save, config, date)


def save_records(records, config, state, day_to_save, reason):

    if records:
        logger.info('Saving due to: %s' % reason)
        prepare_for_s3(records,
                       config,
                       day_to_save)

        emit_state(state)

    return defaultdict(list), state


def save_if_large(records, message, config, state, current_day):
    """
    Optimized for Athena file which likes 128 MB files
    """
    file_size = int(128 * 1e6)  # 128 MB
    if sys.getsizeof(records[message.stream]) > file_size:
        return save_records(
            records, config, state, current_day, 'SIZE')
    else:
        return records, state


def process_record(message, schemas, config, state, validators, records, current_day):
    flat_record = parse_record(message, schemas, validators)

    if message.stream in config['partition_streams']:
        record_date = ts_to_date(message.record.get(config['time_key']))

        current_day = record_date if not current_day else current_day

        if record_date != current_day:
            records, _ = save_records(
                records, config, state, current_day, 'NEW_DAY')
            current_day = record_date

    records[message.stream].append(flat_record)

    return records, current_day


def persist_lines(config, lines):
    state = {'bookmarks': {}}
    schemas = {}
    key_properties = {}
    validators = {}
    records = defaultdict(list)
    current_day = None

    for line in lines:
        message = singer.parse_message(line)

        if isinstance(message, singer.RecordMessage):

            records, current_day = process_record(
                message, schemas, config, state,
                validators, records, current_day)

            records, _ = save_if_large(records, message, config,
                                       state, current_day)

        elif isinstance(message, singer.StateMessage):
            state = process_state(message)

        elif isinstance(message, singer.SchemaMessage):
            records, _ = save_records(
                records, config, state, current_day, 'SCHEMA')
            process_schema(message, validators, schemas, key_properties)

    return save_records(records, config, state, current_day, 'END')


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-csv').version
        conn = http.client.HTTPSConnection('collector.stitchdata.com',
                                           timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-s3csv',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():

    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    # if not args.config.get('disable_collection', False):
    #     logger.info('Sending version information to stitchdata.com. ' +
    #                 'To disable sending anonymous usage data, set ' +
    #                 'the config parameter "disable_collection" to true')
    #     threading.Thread(target=send_usage_stats).start()

    lines = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    _, state = persist_lines(args.config, lines)
    write_last_state([state])

    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
