from parsezeeklogs import ParseZeekLogs
from datetime import datetime, timedelta
import time


feature_record_list = []
TIME_WINDOW_SIZE = 10
feature_record_name = ["timestamp","conn number","tcp_ratio","udp_ratio","other_ratio","size_under_50_bytes","size_under_100_bytes","size_under_1200_bytes","size_over_1200_bytes","S0_ratio","RSTO_ratio","OTH_ratio","SHR_ratio","interval_under_50ms_ratio"]
LOG_FILE_NAME_LIST = ["conn_http.log","conn_syn_5.log","conn_udp_3.log","conn_youtube_10.log"]

def conv_raw_record_to_datetime(raw):
    raw = raw[1:-1]
    if(len(raw) < 1):
        return datetime.now()
    ret = datetime.fromtimestamp((float)(raw))
    return ret

def feature_record(outfile,ex_datetime, tcp_cnt, udp_cnt, other_cnt,size_under_50_cnt,size_under_100_cnt,size_under_1200_cnt,size_over_1200_cnt,packet_cnt, S0_cnt, RSTO_cnt, OTH_cnt,SHR_cnt,interval_under_50ms_cnt):
    if packet_cnt < 1:
        return

    #append ts
    feature_record_list.append(ex_datetime.strftime('%F %H:%M:%S:%f'))

    feature_record_list.append((str)(packet_cnt))

    #append tcp_cnt
    feature_record_list.append((str)((tcp_cnt/packet_cnt)*100))
    feature_record_list.append((str)((udp_cnt/packet_cnt)*100))
    feature_record_list.append((str)((other_cnt/packet_cnt)*100))

    feature_record_list.append((str)((size_under_50_cnt/packet_cnt)*100))
    feature_record_list.append((str)((size_under_100_cnt/packet_cnt)*100))
    feature_record_list.append((str)((size_under_1200_cnt/packet_cnt)*100))
    feature_record_list.append((str)((size_over_1200_cnt/packet_cnt)*100))

    feature_record_list.append((str)((S0_cnt/packet_cnt)*100))
    feature_record_list.append((str)((RSTO_cnt/packet_cnt)*100))
    feature_record_list.append((str)((OTH_cnt/packet_cnt)*100))
    feature_record_list.append((str)((SHR_cnt/packet_cnt)*100))

    feature_record_list.append((str)((interval_under_50ms_cnt/packet_cnt)*100))

    feature_record = ','.join(feature_record_list)
    outfile.write(feature_record + "\n")
    feature_record_list.clear()


def start_parse(file_name):
    flag_is_first = True
    log_start_flag = True
    log_record_flag = False
    ts_record_list = []


    with open('out_' + file_name + '.csv', "w") as outfile:
        packet_cnt = 0
        tcp_cnt = 0
        udp_cnt = 0
        other_cnt = 0
        size_under_50_cnt = 0
        size_under_100_cnt = 0
        size_under_1200_cnt = 0
        size_over_1200_cnt = 0

        S0_cnt = 0
        RSTO_cnt = 0
        OTH_cnt = 0
        SHR_cnt = 0

        ex_datetime = datetime.fromtimestamp(100000.1)
        before_datetime = datetime.fromtimestamp(100000.1)

        interval_under_50ms_cnt = 0

        tmp = ','.join(feature_record_name)
        outfile.write(tmp + "\n")

        for log_record in ParseZeekLogs(file_name, output_format="csv", safe_headers=False,
                                        fields=["ts", "proto", "orig_bytes", "resp_bytes", "conn_state", "orig_ip_bytes", "resp_ip_bytes"]):
            if log_record is not None:
                log_record_list = log_record.split(',')
                log_record_datetime = conv_raw_record_to_datetime(log_record_list[0])
                ts_record_list.append((log_record_datetime, log_record_list))

        ts_record_list.sort()

        for log_record_datetime, log_record_list in ts_record_list:
            if len(log_record_list) > 1:
                packet_cnt += 1

                # log의 timestamp 를 datetime으로 변환
                # log_record_datetime = conv_raw_record_to_datetime(log_record_list[0])

                # 맨 처음 시작 로그이면
                if flag_is_first:
                    ex_datetime = log_record_datetime
                    before_datetime = log_record_datetime
                    flag_is_first = False
                    log_start_flag = False
                    # 특징 추출
                    continue

                # cnt
                proto_filed = log_record_list[1][1:-1]
                if proto_filed == 'tcp':
                    tcp_cnt += 1
                elif proto_filed == 'udp':
                    udp_cnt += 1
                else:
                    other_cnt += 1

                if (before_datetime > log_record_datetime - timedelta(milliseconds=50)) and (before_datetime < log_record_datetime + timedelta(milliseconds=50)):
                    interval_under_50ms_cnt += 1

                size_filed = 0

                for i in log_record_list:
                    if i[1:-1].isdigit():
                        size_filed = max(size_filed, (int)(i[1:-1]))
                    elif i[1:-1] == "S0":
                        S0_cnt += 1
                    elif i[1:-1] == "RSTO":
                        RSTO_cnt += 1
                    elif i[1:-1] == "OTH":
                        OTH_cnt += 1
                    elif i[1:-1] == "SHR":
                        SHR_cnt += 1


                if size_filed <= 50:
                    size_under_50_cnt += 1
                elif size_filed <= 100:
                    size_under_100_cnt += 1
                elif size_filed <= 1200:
                    size_under_1200_cnt += 1
                else:
                    size_over_1200_cnt += 1

                before_datetime = log_record_datetime

                # time window의 시작 로그인지 체크
                if (ex_datetime + timedelta(seconds=TIME_WINDOW_SIZE)) < log_record_datetime:
                    log_start_flag = True
                    log_record_flag = True

                # 추출한 feature를 저장
                if log_record_flag:
                    log_record_flag = False
                    feature_record(outfile,ex_datetime, tcp_cnt, udp_cnt, other_cnt, size_under_50_cnt, size_under_100_cnt,
                                   size_under_1200_cnt, size_over_1200_cnt, packet_cnt, S0_cnt, RSTO_cnt, OTH_cnt,SHR_cnt,interval_under_50ms_cnt)

                    # cnt 변수 초기화
                    packet_cnt = 0
                    tcp_cnt = 0
                    udp_cnt = 0
                    other_cnt = 0
                    size_under_50_cnt = 0
                    size_under_100_cnt = 0
                    size_under_1200_cnt = 0
                    size_over_1200_cnt = 0
                    S0_cnt = 0
                    RSTO_cnt = 0
                    OTH_cnt = 0
                    SHR_cnt = 0
                    interval_under_50ms_cnt = 0

                # 시작 로그면 ex_datetime update
                if log_start_flag:
                    log_start_flag = False
                    ex_datetime = log_record_datetime

        # 마지막 feature 저장
        feature_record(outfile,ex_datetime, tcp_cnt, udp_cnt, other_cnt, size_under_50_cnt, size_under_100_cnt, size_under_1200_cnt,
                       size_over_1200_cnt, packet_cnt, S0_cnt, RSTO_cnt, OTH_cnt,SHR_cnt,interval_under_50ms_cnt)


for i in LOG_FILE_NAME_LIST:
    start_parse(i)
