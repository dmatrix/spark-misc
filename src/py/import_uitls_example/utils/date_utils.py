import datetime

def get_current_date():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def get_current_time():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_current_datetime():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_current_timestamp():
    return datetime.datetime.now().timestamp()

def get_current_timestamp_ms():
    return int(datetime.datetime.now().timestamp() * 1000)

def get_current_timestamp_us():
    return int(datetime.datetime.now().timestamp() * 1000000)

def get_current_timestamp_ns():
    return int(datetime.datetime.now().timestamp() * 1000000000)

def get_current_timestamp_ms_str():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def get_current_timestamp_us_str():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-6]

def get_current_timestamp_ns_str():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")