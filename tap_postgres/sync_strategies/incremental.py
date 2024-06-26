import copy
import time
import psycopg2
import psycopg2.extras
import singer

from singer import utils
from functools import partial
from tap_postgres.metrics import record_counter_dynamic

import tap_postgres.db as post_db


LOGGER = singer.get_logger('tap_postgres')

UPDATE_BOOKMARK_PERIOD = 1000


# pylint: disable=invalid-name,missing-function-docstring
def fetch_max_replication_key(conn_config, replication_key, schema_name, table_name):
    conn = post_db.open_connection(conn_config, False)

    max_key = None

    with conn.cursor() as cur:
        max_key_sql = f"""
            SELECT max({post_db.prepare_columns_sql(replication_key)})
            FROM {post_db.fully_qualified_table_name(schema_name, table_name)}"""

        LOGGER.info("determine max replication key value: %s", max_key_sql)
        cur.execute(max_key_sql)
        max_key = cur.fetchone()[0]

    post_db.close_connection(conn)

    LOGGER.info("max replication key value: %s", max_key)
    return max_key


# pylint: disable=too-many-locals
def sync_table(conn_info, stream, state, desired_columns, md_map):
    time_extracted = utils.now()

    nascent_stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    # before writing the table version to state, check if we had one to begin with
    if nascent_stream_version is None:
        nascent_stream_version = int(time.time() * 1000)
        first_run = True
    else:
        first_run = False

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  nascent_stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    full_stream_name = post_db.calculate_destination_stream_name(stream, md_map)
    schema_name = md_map.get(()).get('schema-name')

    escaped_columns = map(partial(post_db.prepare_columns_for_select_sql, md_map=md_map), desired_columns)

    replication_key = md_map.get((), {}).get('replication-key')
    replication_key_value = singer.get_bookmark(state, stream['tap_stream_id'], 'replication_key_value')
    replication_key_sql_datatype = md_map.get(('properties', replication_key)).get('sql-datatype')
    exclude_last_replication_key = md_map.get((), {}).get('exclude-last-replication-key', False)

    hstore_available = post_db.hstore_available(conn_info)

    with record_counter_dynamic() as counter:
        if first_run:
            activate_version_message = singer.ActivateVersionMessage(
                stream=full_stream_name,
                version=nascent_stream_version)
            LOGGER.info("ACTIVATE VERSION: %s", nascent_stream_version)
            singer.write_message(activate_version_message)
            counter.increment(endpoint=stream['tap_stream_id'], metric_type="truncated")

        conn = post_db.open_connection(conn_info)

        # Client side character encoding defaults to the value in postgresql.conf under client_encoding.
        # The server / db can also have its own configured encoding.
        with conn.cursor() as cur:
            cur.execute("show server_encoding")
            LOGGER.info("Current Server Encoding: %s", cur.fetchone()[0])
            cur.execute("show client_encoding")
            LOGGER.info("Current Client Encoding: %s", cur.fetchone()[0])

        if hstore_available:
            LOGGER.info("hstore is available")
            psycopg2.extras.register_hstore(conn)
        else:
            LOGGER.info("hstore is UNavailable")

        namespace = post_db.get_namespace(conn_info)

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name=namespace) as cur:
            cur.itersize = post_db.CURSOR_ITER_SIZE
            LOGGER.info("Beginning new incremental replication sync %s", nascent_stream_version)
            select_sql = _get_select_sql({
                "escaped_columns": escaped_columns,
                "replication_key": replication_key,
                "replication_key_sql_datatype": replication_key_sql_datatype,
                "replication_key_value": replication_key_value,
                "schema_name": schema_name,
                "table_name": stream['table_name'],
                "limit": conn_info['limit'],
                "exclude_last_replication_key": exclude_last_replication_key,
            })
            LOGGER.info('select statement: %s with itersize %s', select_sql, cur.itersize)
            cur.execute(select_sql)

            rows_saved = 0

            for rec in cur:
                record_message = post_db.selected_row_to_singer_message(stream,
                                                                        rec,
                                                                        nascent_stream_version,
                                                                        desired_columns,
                                                                        time_extracted,
                                                                        md_map)
                singer.write_message(record_message)
                rows_saved += 1
                counter.increment(endpoint=stream['tap_stream_id'])
                counter.increment(endpoint=stream['tap_stream_id'], metric_type="inserted")

                # Picking a replication_key with NULL values will result in it ALWAYS been synced which is not great
                # event worse would be allowing the NULL value to enter into the state
                if record_message.record[replication_key] is not None:
                    state = singer.write_bookmark(state,
                                                    stream['tap_stream_id'],
                                                    'replication_key_value',
                                                    record_message.record[replication_key])

                if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        post_db.close_connection(conn)

    return state


def _get_select_sql(params):
    escaped_columns = params['escaped_columns']
    replication_key = post_db.prepare_columns_sql(params['replication_key'])
    replication_key_sql_datatype = params['replication_key_sql_datatype']
    exclude_last_replication_key = params['exclude_last_replication_key']
    replication_key_value = params['replication_key_value']
    schema_name = params['schema_name']
    table_name = params['table_name']

    limit_statement = f'LIMIT {params["limit"]}' if params["limit"] else ''
    where_condition = ">" if exclude_last_replication_key else ">="
    where_statement = f"WHERE {replication_key} {where_condition} '{replication_key_value}'::{replication_key_sql_datatype}" \
        if replication_key_value else ""

    select_sql = f"""
    SELECT {','.join(escaped_columns)}
    FROM (
        SELECT *
        FROM {post_db.fully_qualified_table_name(schema_name, table_name)} 
        {where_statement}
        ORDER BY {replication_key} ASC {limit_statement}
    ) pg_speedup_trick;"""

    return select_sql
