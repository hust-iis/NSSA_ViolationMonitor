import yaml
from kafka import KafkaConsumer
import pickle
import pymysql

RESPONSE_PACKET = 1
REQUEST_PACKET = 0


# 判断流量包是否是http包
def judge_http(pkt):
    try:
        if pkt.http:
            print("This packet is HTTP packet")
        return True
    except AttributeError:
        print("This packet is not HTTP packet")
        return False


# 判断流量包是http请求包还是http回包
def judge_request_or_response(pkt):
    try:
        if pkt.http.response_code:
            print("This is response http packet")
        return RESPONSE_PACKET
    except AttributeError:
        print("This is request http packet")
        return REQUEST_PACKET


# 处理http登录包
def handle_http_for_login(pkt):
    # 如果是登录包则记录到字典中，等到收到登录成功到回包后将，user_name 和 ip
    # 存储到数据库中
    if judge_request_or_response(pkt) == REQUEST_PACKET:
        if pkt.http.request_uri != '/user/login':
            return
        request_src = pkt.ip.src
        request_dst = pkt.ip.dst
        request_usr_name = pkt['json'].get_field('value_string').all_fields[0].showname_value
        user_dict[(request_usr_name, request_src, request_dst)] = request_src
    else:
        if pkt.http.response_for_uri[-11:] != '/user/login':
            return
        response_src = pkt.ip.src
        response_dst = pkt.ip.dst
        response_response_code_desc = pkt.http.response_code_desc
        response_usr_name = pkt['json'].get_field('value_string').all_fields[2].showname_value
        # 如果登录成功则存储到数据库中，并将元组从字典中删除
        if response_response_code_desc == 'OK' \
                and user_dict.get((response_usr_name, response_dst, response_src)) is not None:
            cursor = db_connector.cursor()
            sql = "select count(*) from user_ip_dynamic_table where user_name = \'%s\' and user_ip = \'%s\'" \
                  % (response_usr_name, response_dst)
            cursor.execute(sql)
            result = cursor.fetchone()
            if result[0] >= 1:
                return
            sql = "insert into user_ip_dynamic_table (user_name,user_ip) values (\'%s\',\'%s\')" \
                  % (response_usr_name, response_dst)
            # 执行sql语句
            cursor.execute(sql)
            # 提交到数据库执行
            db_connector.commit()
            # 记录成功后删除字典
            del user_dict[(response_usr_name, response_dst, response_src)]


def init_config(config_file):
    with open(config_file, 'r') as f:
        config = yaml.load(f, Loader=yaml.Loader)
        return config


# 持续监听并记录user_name 和 ip
def record_user_ip():
    consumer = KafkaConsumer(args_config['mq']['traffic_topic'], group_id='group1',
                             bootstrap_servers=args_config['mq']['server'])
    for bytes_stream in consumer:
        pkt = pickle.loads(bytes_stream.value)
        if judge_http(pkt):
            handle_http_for_login(pkt)


if __name__ == '__main__':
    args_config = init_config('./config.yaml')
    db_connector = pymysql.connect(host=args_config['mysql']['host'],
                                   port=int(args_config['mysql']['port']),
                                   user=args_config['mysql']['user'],
                                   password=args_config['mysql']['passwd'],
                                   database=args_config['mysql']['db_name'])
    user_dict = {}
    record_user_ip()
    db_connector.close()
