from kafka import KafkaConsumer
import pickle

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


def handle_http(pkt):
    if judge_request_or_response(pkt) == REQUEST_PACKET:
        request_src = pkt.ip.src
        request_dst = pkt.ip.dst
        request_method = pkt.http.request_method
        request_full_uri = pkt.http.request_full_uri
        request_usr_name = pkt['json'].get_field('value_string').all_fields[0].showname_value

        print('%s %s ----> %s' % (request_method, request_src, request_dst))
        print('%s %s' % (request_full_uri, request_usr_name))
        print('---------------------')
    else:
        response_src = pkt.ip.src
        response_dst = pkt.ip.dst
        response_response_code = pkt.http.response_code
        response_response_code_desc = pkt.http.response_code_desc
        response_usr_name = pkt['json'].get_field('value_string').all_fields[2].showname_value
        print('%s %s %s ----> %s' % (response_response_code, response_response_code_desc, response_src, response_dst))
        print(response_usr_name)


def main():
    consumer = KafkaConsumer('net-traffic', bootstrap_servers=['localhost:9092'])
    for bytes_stream in consumer:
        pkt = pickle.loads(bytes_stream.value)
        if judge_http(pkt):
            handle_http(pkt)


if __name__ == '__main__':
    main()

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
