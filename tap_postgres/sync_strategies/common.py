import sys
import simplejson as json
import singer
from singer import  metadata
import tap_postgres.db as post_db


# pylint: disable=invalid-name,missing-function-docstring
def should_sync_column(md_map, field_name):
    field_metadata = md_map.get(('properties', field_name), {})
    return singer.should_sync_field(field_metadata.get('inclusion'),
                                    field_metadata.get('selected'),
                                    True)


def write_schema_message(schema_message):
    sys.stdout.write(json.dumps(schema_message, use_decimal=True) + '\n')
    sys.stdout.flush()


def send_schema_message(stream, bookmark_properties):
    s_md = metadata.to_map(stream['metadata'])
    if s_md.get((), {}).get('is-view'):
        key_properties = s_md.get((), {}).get('view-key-properties', [])
    else:
        key_properties = s_md.get((), {}).get('table-key-properties', [])

    schema_message = {'type' : 'SCHEMA',
                      'stream' : post_db.calculate_destination_stream_name(stream, s_md),
                      'schema' : stream['schema'],
                      'key_properties' : key_properties,
                      'bookmark_properties': bookmark_properties}

    write_schema_message(schema_message)

def size_bytes_to_human(num: int) -> str:
    if not num or num == 0:
        return '0B'

    base = 1

    for unit in ['B', 'Kb', 'Mb']:
        n = num / base

        if n < 9.95 and unit != 'B':
            # Less than 10 then keep 1 decimal place
            value = "{:.1f}{}".format(n, unit)
            return value

        if round(n) < 1000:
            # Less than 4 digits so use this
            value = "{}{}".format(round(n), unit)
            return value

        base *= 1024

    value = "{}{}".format(round(n), unit)
    return value
