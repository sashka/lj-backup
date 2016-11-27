# encoding: utf-8

import datetime
import uuid
import json
import decimal
import xmlrpclib


class BetterJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, xmlrpclib.Binary):
            return obj.data
        elif isinstance(obj, uuid.UUID):
            return obj.hex
        elif isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, (set, frozenset)):
            return list(obj)
        return super(BetterJSONEncoder, self).default(obj)


def better_json_encode(value, pretty_print=False):
    indent = None
    sort_keys = False
    separators = (',', ':')

    if pretty_print:
        indent = 4
        sort_keys = True
        separators = (', ', ': ')

    return json.dumps(value, separators=separators, cls=BetterJSONEncoder, indent=indent, sort_keys=sort_keys, ensure_ascii=True)
