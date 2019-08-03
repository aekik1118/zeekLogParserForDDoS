from parsezeeklogs import ParseZeekLogs
from datetime import datetime, timedelta
import time

flag_is_first = True
log_start_flag = True
log_record_flag = False
ex_datetime = datetime.fromtimestamp(100000.1)
feature_record_list = []
TIME_WINDOW_SIZE = 5
feature_record_name = ["timestamp","tcp_cnt","udp_cnt","other_cnt","size_under_50_bytes","size_under_100_bytes","size_under_1200_bytes","size_over_1200_bytes"]

def conv_raw_record_to_datetime(raw):
    raw = raw[1:-1]
    if(len(raw) < 1):
        return datetime.now()
    ret = datetime.fromtimestamp((float)(raw))
    return ret

def feature_record(outfile, tcp_cnt, udp_cnt, other_cnt,size_under_50_cnt,size_under_100_cnt,size_under_1200_cnt,size_over_1200_cnt):
    #append ts
    feature_record_list.append(ex_datetime.strftime('%F %H:%M:%S:%f'))

    #append tcp_cnt
    feature_record_list.append((str)(tcp_cnt))
    feature_record_list.append((str)(udp_cnt))
    feature_record_list.append((str)(other_cnt))

    feature_record_list.append((str)(size_under_50_cnt))
    feature_record_list.append((str)(size_under_100_cnt))
    feature_record_list.append((str)(size_under_1200_cnt))
    feature_record_list.append((str)(size_over_1200_cnt))

    feature_record = ','.join(feature_record_list)
    outfile.write(feature_record + "\n")
    feature_record_list.clear()


with open('out_youtube_10.csv',"w") as outfile:
    tcp_cnt = 0
    udp_cnt = 0
    other_cnt = 0
    size_under_50_cnt = 0
    size_under_100_cnt = 0
    size_under_1200_cnt = 0
    size_over_1200_cnt = 0


    tmp = ','.join(feature_record_name)
    outfile.write(tmp + "\n")

    for log_record in ParseZeekLogs("conn_youtube_10.log", output_format="csv", safe_headers=False, fields=["ts","proto","orig_bytes"]):
        if len(log_record) > 0:
            log_record_list = log_record.split(',')

            #log의 timestamp 를 datetime으로 변환
            log_record_datetime = conv_raw_record_to_datetime(log_record_list[0])

            #cnt
            proto_filed = log_record_list[1][1:-1]
            if proto_filed == 'tcp':
                tcp_cnt += 1
            elif proto_filed == 'udp':
                udp_cnt += 1
            else:
                other_cnt += 1

            #packet size cnt
            size_filed = 0

            if len(log_record_list)>2 and log_record_list[2][1:-1].isdigit():
                size_filed = (int)(log_record_list[2][1:-1])

            if size_filed <= 50:
                size_under_50_cnt += 1
            elif size_filed <= 100:
                size_under_100_cnt += 1
            elif size_filed <= 1200:
                size_under_1200_cnt += 1
            else:
                size_over_1200_cnt += 1


            #맨 처음 시작 로그이면
            if flag_is_first:
                ex_datetime = log_record_datetime
                flag_is_first = False
                log_start_flag = False
                #특징 추출
                continue

            #time window의 시작 로그인지 체크
            if (ex_datetime + timedelta(seconds=TIME_WINDOW_SIZE)) < log_record_datetime:
                log_start_flag = True
                log_record_flag = True

            #추출한 feature를 저장
            if log_record_flag:
                log_record_flag = False
                feature_record(outfile, tcp_cnt, udp_cnt, other_cnt,size_under_50_cnt,size_under_100_cnt,size_under_1200_cnt,size_over_1200_cnt)

                #cnt 변수 초기화
                tcp_cnt = 0
                udp_cnt = 0
                other_cnt = 0
                size_under_50_cnt = 0
                size_under_100_cnt = 0
                size_under_1200_cnt = 0
                size_over_1200_cnt = 0


            #시작 로그면 ex_datetime update
            if log_start_flag:
                log_start_flag = False
                ex_datetime = log_record_datetime

    #마지막 feature 저장
    feature_record(outfile, tcp_cnt, udp_cnt, other_cnt,size_under_50_cnt,size_under_100_cnt,size_under_1200_cnt,size_over_1200_cnt)


