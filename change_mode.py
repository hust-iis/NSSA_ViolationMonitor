import time
from threading import Thread
import flask
import pymysql
import yaml
from flask import request, jsonify
from kafka import KafkaConsumer
import pickle
import dynamic_record

LEARNING_MODE = 0
INTERCEPT_MODE = 1
now_mode = 0
REQUEST_PACKET = 0
RESPONSE_PACKET = 1
server = flask.Flask(__name__)


def learning_mode_for_http(pkt):
    print("this is learning mode for http")
    if dynamic_record.judge_request_or_response(pkt) != 0:
        return

    request_src = pkt.ip.src
    request_full_uri = pkt.http.request_full_uri
    cursor = db_connector.cursor()
    sql = 'select count(*) from user_ip_dynamic_table where user_ip = \'%s\'' % request_src
    cursor.execute(sql)
    result = cursor.fetchone()
    if result[0] < 1:
        return
    sql = 'select count(*) from ip_uri_static_table where request_ip = \'%s\' and request_uri = \'%s\'' \
          % (request_src, request_full_uri)
    cursor.execute(sql)
    result = cursor.fetchone()
    if result[0] >= 1:
        return
    sql = 'insert into ip_uri_static_table (request_ip, request_uri) values (\'%s\', \'%s\')' \
          % (request_src, request_full_uri)
    cursor.execute(sql)
    db_connector.commit()
    # 将结果存入第三个表
    sql = 'select * from user_ip_dynamic_table where user_ip = \'%s\'' % request_src
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        sql = 'select count(*) from user_uri_white_table where user_name = \'%s\' and user_uri = \'%s\'' \
              % (row[0], request_full_uri)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result[0] >= 1:
            continue
        sql = 'insert into user_uri_white_table (user_name, user_uri) values (\'%s\',\'%s\')' \
              % (row[0], request_full_uri)
        cursor.execute(sql)
        db_connector.commit()


def intercept_mode_for_http(pkt):
    print("this is intercept mode for http")
    if dynamic_record.judge_request_or_response(pkt) != 0:
        return
    request_src = pkt.ip.src
    request_full_uri = pkt.http.request_full_uri
    cursor = db_connector.cursor()
    sql = 'select * from user_ip_dynamic_table where user_ip = \'%s\'' % request_src
    cursor.execute(sql)
    row_count = cursor.rowcount
    if row_count < 1:
        print("ip拦截")
        sql = 'insert into disable_access_log_table (user_name, user_ip, user_uri) values (\'%s\',\'%s\',\'%s\')' \
              % ('unknown', request_src, request_full_uri)
        cursor.execute(sql)
        db_connector.commit()
        return
    result = cursor.fetchone()
    sql = 'select * from user_uri_white_table where user_name = \'%s\' and user_uri = \'%s\'' \
          % (result[0], request_full_uri)
    cursor.execute(sql)
    if cursor.rowcount < 1:
        print("用户拦截")
        sql = 'insert into disable_access_log_table (user_name, user_ip, user_uri) values (\'%s\',\'%s\',\'%s\')' \
              % (result[0], request_src, request_full_uri)
        cursor.execute(sql)
        db_connector.commit()
    else:
        print("in white table")


@server.route('/change-mode', methods=['post'])
def change_mode():
    req = request.get_json()
    global now_mode
    if int(req['status']) == LEARNING_MODE:
        now_mode = LEARNING_MODE
    elif int(req['status']) == INTERCEPT_MODE:
        now_mode = INTERCEPT_MODE
    else:
        return jsonify(
            {
                'code': 1000,
                'msg': 'parameters error'
            })
    return jsonify(
        {
            'code': 200,
            'msg': 'success',
            'data': None
        }
    )


def judge_mode():
    while 1:
        packets = consumer.poll(max_records=1)
        for bytes_stream in packets.values():
            pkt = pickle.loads(bytes_stream[0].value)
            if now_mode == LEARNING_MODE:
                if dynamic_record.judge_http(pkt):
                    learning_mode_for_http(pkt)
            if now_mode == INTERCEPT_MODE:
                if dynamic_record.judge_http(pkt):
                    intercept_mode_for_http(pkt)
            time.sleep(5)


def init_config(config_file):
    with open(config_file, 'r') as f:
        config = yaml.load(f, Loader=yaml.Loader)
        return config


if __name__ == '__main__':
    args_config = init_config('./config.yaml')
    consumer = KafkaConsumer(args_config['mq']['traffic_topic'], group_id='group2',
                             bootstrap_servers=args_config['mq']['server'])
    db_connector = pymysql.connect(host=args_config['mysql']['host'],
                                   port=int(args_config['mysql']['port']),
                                   user=args_config['mysql']['user'],
                                   password=args_config['mysql']['passwd'],
                                   database=args_config['mysql']['db_name'])
    t1 = Thread(target=judge_mode)
    t1.start()
    server.run()
