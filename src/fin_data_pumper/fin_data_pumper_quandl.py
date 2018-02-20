import quandl
import abstract_msg_broker
import msg_broker_rabbitmq
import fin_data_pumper2
import json
import datetime


quandl.ApiConfig.api_key = "oJSdyTHy_foE-UjEPz3x"


class FinDataPumperQuandl(fin_data_pumper2.FinDataPumper2):

    def __init__(self, msg_broker):
        super(FinDataPumperQuandl, self).__init__(msg_broker)

    def process_msg(self, msg):

        if msg is None:
            return None

        try:
            j = json.loads(msg)
        except ValueError:
            return None

        command = j['command']
        if command == 'pull_historical':
            big_data_filename = '/tmp/hist_data_' + j['feedcode'] + '_' + '_' + datetime.datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S.%f')[:-3]
            self.pull_historical(j['feedcode'], big_data_filename)
        else:
            return None
            # not supported by quandl

        result = '{"filename":"' + big_data_filename + '"}'
        return result

    def pull_historical(self, feedcode, filename):
        quandl.get_table('WIKI/PRICES', ticker=feedcode).to_csv(filename)


import unittest
import abstract_msg_broker
import msg_broker_rabbitmq
import threading
import pika
import queue


class TestCoreConsumerMock(threading.Thread):

    def __init__(self):
        super(TestCoreConsumerMock, self).__init__()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.basic_consume(self.on_msg, queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_CORE, no_ack=False)
        self.channel.queue_declare(queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_CORE)
        self.queue = queue.Queue()

    def on_msg(self, unused_channel, method, properties, body):
        self.queue.put(body)
        self.channel.basic_ack(method.delivery_tag)

    def run(self):
        self.channel.start_consuming()

    def stop(self):
        self.channel.stop_consuming()
        self.connection.close()
        self.join()

    def get_result(self):
        item = self.queue.get()
        self.queue.task_done()
        return item


class TestFinDataPumperQuandl(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.consumer = TestCoreConsumerMock()
        cls.consumer.start()
        cls.msg_broker = msg_broker_rabbitmq.MsgBrokerRabbitMQ()
        cls.msg_broker.reset_queues()
        cls.msg_broker.start()
        cls.fin_data_pumper = FinDataPumperQuandl(cls.msg_broker)
        cls.fin_data_pumper.start()

    @classmethod
    def tearDownClass(cls):
        cls.fin_data_pumper.stop()
        cls.msg_broker.stop()
        cls.consumer.stop()

    def test_pull_historical(self):
        request = '{ "command":"pull_historical", "feedcode":"NVDA" }'
        self.msg_broker.put_msg_for(request, abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        result = self.consumer.get_result()
        print("got result:")
        print(result)

"""
if __name__ == "__main__":

    msg_broker = msg_broker_rabbitmq.MsgBrokerRabbitMQ()
    msg_broker.reset_queues()
    msg_broker.start()

    fin_data_pumper = FinDataPumperQuandl(msg_broker)
    msg = '{ "command":"pull_historical", "feedcode":"NVDA" }'
    fin_data_pumper.process_msg(msg)

    msg_broker.stop()
"""

if __name__ == '__main__':
    unittest.main()

