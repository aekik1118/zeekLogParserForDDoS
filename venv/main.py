from parsezeeklogs import ParseZeekLogs
from datetime import datetime
import time

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
            feature_record_list = []

            #log의 timestamp 를 datetime으로 변환
            log_record_datetime = conv_raw_record_to_datetime(log_record_list[0])

            #time window의 시작 로그인지 체크



            #추출한 feature를 저장
            feature_record_list.append(log_record_datetime.strftime('%F %H:%M:%S:%f'))
            feature_record = ','.join(feature_record_list)
            outfile.write(feature_record + "\n")
