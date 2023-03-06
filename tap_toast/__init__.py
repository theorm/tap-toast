#!/usr/bin/env python3
import json
import sys
import singer
from singer import metadata
from tap_toast.toast import Toast
from tap_toast.discover import discover_streams
from tap_toast.sync import sync_stream
from tap_toast.streams import STREAMS
from tap_toast.context import Context


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "client_id",
    "client_secret",
    "location_guid",
    "start_date",
    "management_group_guid",
    "auth_with_login"
]


def do_discover(client):
    LOGGER.info("Starting discover")
    catalog = {"streams": discover_streams(client)}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def get_selected_streams(catalog):
    selected_stream_names = []
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        if stream_is_selected(mdata):
            selected_stream_names.append(stream.tap_stream_id)
    return selected_stream_names


class DependencyException(Exception):
    pass


def populate_class_schemas(catalog, selected_stream_names):
    for stream in catalog.streams:
        if stream.tap_stream_id in selected_stream_names:
            STREAMS[stream.tap_stream_id].stream = stream


def ensure_credentials_are_authorized(client):
    client.is_authorized()


def do_sync(client, catalog, state):
    ensure_credentials_are_authorized(client)
    selected_stream_names = get_selected_streams(catalog)

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id

        mdata = metadata.to_map(stream.metadata)

        if stream_name not in selected_stream_names:
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        instance = STREAMS[stream_name](client)
        instance.stream = stream
        counter_value = sync_stream(state, instance)
        singer.write_state(state)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    singer.write_state(state)
    LOGGER.info("Finished sync")


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    creds = {
        "client_id": parsed_args.config['client_id'],
        "client_secret": parsed_args.config['client_secret'],
        "location_guid": parsed_args.config['location_guid'],
        "start_date": parsed_args.config['start_date'],
        "management_group_guid": parsed_args.config['management_group_guid'],
        "auth_with_login": parsed_args.config.get('auth_with_login', True)
    }

    client = Toast(**creds)
    Context.config = parsed_args.config

    if parsed_args.discover:
        do_discover(client)
    elif parsed_args.catalog:
        state = parsed_args.state or {}
        do_sync(client, parsed_args.catalog, state)
