from parsezeeklogs import ParseZeekLogs
from datetime import datetime, timedelta
import time

def conv_raw_record_to_datetime(raw):
    raw = raw[1:-1]
    if(len(raw) < 1):
        return datetime.now()
    ret = datetime.fromtimestamp((float)(raw))
    return ret

flag_is_first = True
log_start_flag = True
log_record_flag = False
ex_datetime = datetime.fromtimestamp(100000.1)
feature_record_list = []

with open('out.csv',"w") as outfile:
    for log_record in ParseZeekLogs("conn.log", output_format="csv", safe_headers=False, fields=["ts","id.orig_h","id.orig_p","id.resp_h","id.resp_p"]):
        if log_record is not None:
            log_record_list = log_record.split(',')

            #log의 timestamp 를 datetime으로 변환
            log_record_datetime = conv_raw_record_to_datetime(log_record_list[0])

            if flag_is_first:
                ex_datetime = log_record_datetime
                flag_is_first = False
                log_start_flag = False
                continue

            #time window의 시작 로그인지 체크
            if (ex_datetime + timedelta(seconds=10)) < log_record_datetime:
                log_start_flag = True
                log_record_flag = True

            #추출한 feature를 저장
            if log_record_flag:
                log_record_flag = False
                feature_record_list.append(ex_datetime.strftime('%F %H:%M:%S:%f'))
                feature_record = ','.join(feature_record_list)
                outfile.write(feature_record + "\n")
                feature_record_list.clear()

            #시작 로그면 ex_datetime update
            if log_start_flag:
                log_start_flag = False
                ex_datetime = log_record_datetime



    #마지막 feature 저장
    feature_record_list.append(ex_datetime.strftime('%F %H:%M:%S:%f'))
    feature_record = ','.join(feature_record_list)
    outfile.write(feature_record + "\n")



