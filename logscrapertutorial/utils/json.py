"""
Adds :class:`datetime.date`  and :class:`datetime.datetime` encoding to json.

Alternatively, the fake data factories could just return dates as strings but this shows how you
can easily add functionality to builtins.
"""
import json
from datetime import date, datetime


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime) or isinstance(o, date):
            return o.isoformat()
        return super(o)


#  Monkey patch json to use our date object capable encoder.
json._default_encoder = DateTimeEncoder(
    skipkeys=False,
    ensure_ascii=True,
    check_circular=True,
    allow_nan=True,
    indent=None,
    separators=None,
    default=None,
)
