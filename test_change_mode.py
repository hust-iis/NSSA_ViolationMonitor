import time
from threading import Thread
import flask

LEARNING_MODE = 0
INTERCEPT_MODE = 1
now_mode = 1
server = flask.Flask(__name__)


def learning_mode():
    print("this is learning mode")


def intercept_mode():
    print("this is intercept mode")


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
            learning_mode()
        if now_mode == INTERCEPT_MODE:
            intercept_mode()
        time.sleep(5)


if __name__ == '__main__':
    t1 = Thread(target=judge)
    t1.start()
    server.run()