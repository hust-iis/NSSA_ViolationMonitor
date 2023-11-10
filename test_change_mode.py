import time
from threading import Thread
import flask
import pymysql
import yaml
from kafka import KafkaConsumer
import pickle
import test_consumer

LEARNING_MODE = 0
INTERCEPT_MODE = 1
now_mode = 0
REQUEST_PACKET = 0
RESPONSE_PACKET = 1
server = flask.Flask(__name__)


def learning_mode_for_http():
    print("this is learning mode")
    packets = consumer.poll(max_records=1)
    for bytes_stream in packets.values():
        pkt = pickle.loads(bytes_stream[0].value)

        if test_consumer.judge_request_or_response(pkt) != 0:
            return

        request_src = pkt.ip.src
        request_full_uri = pkt.http.request_full_uri
        cursor = db_connector.cursor()
        sql = 'select count(*) from user_ip_dynamic_table where user_ip = \'%s\'' % request_src
        cursor.execute(sql)
        result = cursor.fetchone()
        if result[0] < 1:
            return
        sql = 'select count(*) from ip_uri_static_table where request_ip = \'%s\' and request_uri = \'%s\''\
              % (request_src, request_full_uri)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result[0] >= 1:
            return
        sql = 'insert into ip_uri_static_table (request_ip, request_uri) values (\'%s\', \'%s\')' \
              % (request_src, request_full_uri)
        cursor.execute(sql)
        db_connector.commit()


def intercept_mode_for_http():
    print("this is intercept mode")
    # bytes_stream = consumer.poll(timeout_ms=100)
    # pkt = pickle.loads(bytes_stream)


@server.route('/learn', methods=['get', 'post'])
def change_learn_to_intercept():
    global now_mode
    now_mode = INTERCEPT_MODE
    return '200'


@server.route('/intercept', methods=['get', 'post'])
def change_intercept_to_learn():
    global now_mode
    now_mode = LEARNING_MODE
    return '200'


def judge():
    while 1:
        if now_mode == LEARNING_MODE:
            learning_mode_for_http()
        if now_mode == INTERCEPT_MODE:
            intercept_mode_for_http()
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
    t1 = Thread(target=judge)
    t1.start()
    server.run()
