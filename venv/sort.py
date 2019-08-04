from parsezeeklogs import ParseZeekLogs
from datetime import datetime, timedelta

LOG_FILE_NAME_LIST = ["conn_http.log","conn_syn_5.log","conn_udp_3.log","conn_youtube_10.log"]

ts_index_list = []

def conv_raw_record_to_datetime(raw):
    raw = raw[1:-1]
    if(len(raw) < 1):
        return datetime.now()
    ret = datetime.fromtimestamp((float)(raw))
    return ret

with open('out.csv',"w") as outfile:
    for log_record in ParseZeekLogs("conn.log", output_format="csv", safe_headers=False, fields=["ts","id.orig_h","id.orig_p","id.resp_h","id.resp_p"]):
        if log_record is not None:
            log_record_list = log_record.split(',')
            log_record_datetime = conv_raw_record_to_datetime(log_record_list[0])
            ts_index_list.append((log_record_datetime,log_record_list))

    ts_index_list.sort()

