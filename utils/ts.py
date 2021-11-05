import datetime


def timestamp(time_str: str) -> int:
    ts = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S").timestamp() * 1000
    return int(round(ts))
