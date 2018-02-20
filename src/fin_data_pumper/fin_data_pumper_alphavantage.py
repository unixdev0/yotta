import fin_data_pumper2
import requests
import json
import datetime

# for alphavantage:
# https://www.alphavantage.co/documentation/


class FinDataPumperAlphaVantage(fin_data_pumper2.FinDataPumper2):
    query_url_ = 'https://www.alphavantage.co/query?function='
    intraday_str_ = 'TIME_SERIES_INTRADAY'
    daily_str_ = 'TIME_SERIES_DAILY'
    weekly_str_ = 'TIME_SERIES_WEEKLY'
    monthly_str_ = 'TIME_SERIES_MONTHLY'
    batch_stock_quotes_str_ = 'BATCH_STOCK_QUOTES'
    currency_exchange_rate_str_ = 'CURRENCY_EXCHANGE_RATE'
    alphavantage_apikey_ = '3GCBTCH75I2PCR1X'
    """alphavantage_apikey_ = 'demo'"""
    symbols_set_comma_ = None

    def __init__(self, msg_broker):
        super(FinDataPumperAlphaVantage, self).__init__(msg_broker)

    def pull_bbo(self, symbol):
        ccy1, ccy2 = symbol.split("/")
        if ccy1 is None or ccy2 is None:
            return None
        url = self.query_url_ + self.currency_exchange_rate_str_ + '&from_currency=' + ccy1 + '&to_currency=' + ccy2 + '&apikey=' + self.alphavantage_apikey_
        return self.sync_request(url)

    def pull_historical(self, feedcode, frequency, interval):
        time_series_freq = self.time_series_freq(frequency)
        if time_series_freq is None:
            return
        interval_str = self.interval_string(frequency, interval)
        url = self.query_url_ + self.time_series_freq(frequency) + '&symbol=' + feedcode + interval_str + '&apikey=' + self.alphavantage_apikey_
        return self.sync_request(url)

    def process_msg(self, msg):
        """
        incoming command in the msg
        :param msg: command message in json format
        :return: None or message in json format to reply to the core

        not sure about the message protocol, let it be like:
        { "command":"pull_bbo", "feedcode":"EUR/USD" }
        { "command":"pull_historical", "feedcode":"EUR/USD", "frequency":"daily", "interval":"1" }
        """
        if msg is None:
            return None

        result = None
        big_data_filename = None

        try:
            j = json.loads(msg)
        except ValueError:
            return None

        command = j['command']
        if command == 'pull_bbo':
            result = self.pull_bbo(j['feedcode'])
        elif j['command'] == 'pull_historical':
            result = self.pull_historical(j['feedcode'], j['frequency'], j['interval'])
            big_data_filename = '/tmp/hist_data_' + j['feedcode'] + '_' + j['frequency'] + '_' + j['interval'] + '_' + datetime.datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S.%f')[:-3]
        elif j['command'] == 'pull_batch_stock_quotes':
            result = self.pump_batch_stock_quotes(j['stocks'])

        if result is None:
            return None

        if big_data_filename:
            bd_file = open(big_data_filename, "w")
            bd_file.write(result)
            bd_file.close()
            result = '{ "filename":"' + big_data_filename + '"}'

        return result

    def make_query_url(self, bar):
        """

        :type bar: Bar
        """
        time_series_freq = self.time_series_freq(bar["freq"])
        if time_series_freq is None:
            return None
        interval_str = self.interval_string(bar["freq"], bar["interval"])
        url = self.query_url_ + self.time_series_freq(bar["freq"]) + '&symbol=' + bar["feedcode"] + interval_str + '&apikey=' + self.alphavantage_apikey_
        return url

    def time_series_freq(self, freq):
        if freq == 'minute':
            return self.intraday_str_
        elif freq == 'day':
            return self.daily_str_
        elif freq == 'week':
            return self.weekly_str_
        elif freq == 'month':
            return self.monthly_str_
        return None

    def interval_string(self, freq, interval):
        if freq == 'minute':
            return '&interval=' + interval + '&min'
        return ''

    def pump_market_data(self, fin_data_consumer):
        url = self.query_url_ + self.batch_stock_quotes_str_ + '&symbols=' + self.symbols_set_comma_ + '&apikey=' + self.alphavantage_apikey_
        response = self.sync_request(url)
        fin_data_consumer.consume(response)

    @staticmethod
    def sync_request(url):
        try:
            print('requesting ', url, ' ------->')
            r = requests.get(url)
            return r.text
        except requests.RequestException as e:
            return e.message

        return 'unknown exception'


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


def json_formed_well(json_data):
    try:
        json.loads(json_data)
        return True
    except ValueError:
        return False


class TestFinDataPumperAlhavantage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.consumer = TestCoreConsumerMock()
        cls.consumer.start()
        cls.msg_broker = msg_broker_rabbitmq.MsgBrokerRabbitMQ()
        cls.msg_broker.reset_queues()
        cls.msg_broker.start()
        cls.fin_data_pumper = FinDataPumperAlphaVantage(cls.msg_broker)
        cls.fin_data_pumper.start()

    @classmethod
    def tearDownClass(cls):
        cls.fin_data_pumper.stop()
        cls.msg_broker.stop()
        cls.consumer.stop()

    def test_pull_bbo(self):
        req_bbo_eurusd_data = '{"command":"pull_bbo", "feedcode":"EUR/USD"}'
        self.msg_broker.put_msg_for(req_bbo_eurusd_data, abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        result = self.consumer.get_result()
        print("got bbo result:")
        print(result)
        self.assertTrue(json_formed_well(result))

    def test_pull_historical(self):
        req_hist_data = '{"command":"pull_historical", "feedcode":"MSFT", "frequency":"week", "interval":"1"}'
        self.msg_broker.put_msg_for(req_hist_data, abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        result = self.consumer.get_result()
        print('got historical result:')
        print(result)
        self.assertTrue(json_formed_well(result))


if __name__ == '__main__':
    unittest.main()
