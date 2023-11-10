import yaml
from kafka import KafkaConsumer
import pickle
import pymysql

RESPONSE_PACKET = 1
REQUEST_PACKET = 0


def judge_http(pkt):
    try:
        if pkt.http:
            print("This packet is HTTP packet")
        return True
    except AttributeError:
        print("This packet is not HTTP packet")
        return False


def judge_request_or_response(pkt):
    try:
        if pkt.http.response_code:
            print("This is response http packet")
        return RESPONSE_PACKET
    except AttributeError:
        print("This is request http packet")
        return REQUEST_PACKET


def handle_http_for_login(pkt):
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
        if response_response_code_desc == 'OK' \
                and user_dict.get((response_usr_name, response_dst, response_src)) is not None:
            cursor = db_connector.cursor()
            sql = "select count(*) from user_ip_dynamic_table where user_name = \'%s\' and user_ip = \'%s\'" \
                  % (response_usr_name, response_dst)
            cursor.execute(sql)
            result = cursor.fetchone()
            if result[0] >= 1:
                return
            sql = "insert into user_ip_dynamic_table (user_name,user_ip) values (\'%s\',\'%s\')"\
                  % (response_usr_name, response_dst)
            # 执行sql语句
            cursor.execute(sql)
            # 提交到数据库执行
            db_connector.commit()


def init_config(config_file):
    with open(config_file, 'r') as f:
        config = yaml.load(f, Loader=yaml.Loader)
        return config


def record_user_ip():
    consumer = KafkaConsumer(args_config['mq']['traffic_topic'], group_id='group1', bootstrap_servers=args_config['mq']['server'])
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

# print(pkt['http'])
# print(type(pkt['json']))
# print(pkt['json'].field_names)
# print(pkt['json'].get_field('value_string').all_fields)
# print(pkt['json'].get_field('value_string').all_fields[0].showname_value)
# try:
#     print(pkt['http'])
# except AttributeError as e:
#     print(1)
# print(pkt.http)
# print(pkt)
# protocol = pkt.transport_layer
# src_addr = pkt.ip.src
# src_port = pkt[pkt.transport_layer].srcport
# dst_addr = pkt.ip.dst
# dst_port = pkt[pkt.transport_layer].dstport
# print('%s  %s:%s --> %s:%s' % (protocol, src_addr, src_port, dst_addr, dst_port))
# print(http_layer_pkt)
# print(http_layer_pkt.host)
# print(http_layer_pkt.request_method)
# print(http_layer_pkt.request_uri)
# print(http_layer_pkt.request_version)
# print(http_layer_pkt.request_full_uri)
# print(http_layer_pkt.user_agent)
# print(http_layer_pkt.referer)
