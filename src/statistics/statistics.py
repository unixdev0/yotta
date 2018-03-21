import json
import sqlite3
import pandas as pd
import numpy as np
import math
import time
import sys
import threading
import pika
import queue

sys.path.append('../fin_data_pumper')
import abstract_msg_broker
import msg_broker_rabbitmq


class Statistics(threading.Thread):

    def __init__(self, msg_broker):
        super(Statistics, self).__init__()
        self.msg_broker = msg_broker
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.isSet():
            msg = self.msg_broker.get_next_msg_for(abstract_msg_broker.AbstractMsgBroker.QUEUE_STATISTICS)
            print("Statistics.run: got msg", msg)
            if msg is not None:
                result_tables = self.process_msg(msg)
                if result_tables is not None:
                    json_data = {"tables": result_tables}
                    out_str_msg = json.dumps(json_data)
                    #print("Statistics.run: put msg for QUEUE_CORE", out_str_msg)
                    self.msg_broker.put_msg_for(out_str_msg, abstract_msg_broker.AbstractMsgBroker.QUEUE_CORE)

    def stop(self):
        self._stop_event.set()
        self.join()

    @staticmethod
    def to_db_table(self, stats, instrument, df, dbconn):
        tbl_name = stats + '_' + instrument + '_' + str(int(round(time.time() * 1000)))
        df.to_sql(tbl_name, dbconn, if_exists="replace")
        return tbl_name

    def process_msg(self, msg):

        if msg is None:
            return None

        try:
            j = json.loads(msg)
            stats = (j['statistics']).lower()
            dbpath = j['dbpath']
            instrument = j['instrument']
        except (ValueError, KeyError):
            return None

        dbconn = sqlite3.connect(dbpath)
        result_tables = []

        try:
            if stats == 'sma':
                column = j['column']
                period = j['period']
                df = self.sma_db(dbpath, instrument, column, period)
                tbl_name = 'sma_' + instrument + '_' + str(int(round(time.time() * 1000)))
                Statistics.to_db_table(tbl_name, stats, instrument, df, dbconn)
                result_tables.append(tbl_name)
            elif stats == 'ema':
                column = j['column']
                period = j['period']
                df = self.ema_db(dbpath, instrument, column, period)
                tbl_name = 'ema_' + instrument + '_' + str(int(round(time.time() * 1000)))
                Statistics.to_db_table(tbl_name, stats, instrument, df, dbconn)
                result_tables.append(tbl_name)
            elif stats == 'rsi':
                column = j['column']
                period = j['period']
                df = self.rsi_db(dbpath, instrument, column, period)
                tbl_name = 'rsi_' + instrument + '_' + str(int(round(time.time() * 1000)))
                Statistics.to_db_table(tbl_name, stats, instrument, df, dbconn)
                result_tables.append(tbl_name)
            elif stats == 'macd':
                n_fast = j['n_fast']
                n_slow=j['n_slow']
                n_sign = j['n_sign']
                column=j['column']
                mcd, sign, diff = self.macd_db(dbpath, instrument, n_fast, n_slow, n_sign, column)
                tbl_name = 'mcd_' + instrument + '_' + str(int(round(time.time() * 1000)))
                Statistics.to_db_table(tbl_name, 'mcd', instrument, mcd, dbconn)
                result_tables.append(tbl_name)
                tbl_name = 'sign_' + instrument + '_' + str(int(round(time.time() * 1000)))
                Statistics.to_db_table(tbl_name, 'sign', instrument, sign, dbconn)
                result_tables.append(tbl_name)
                tbl_name = 'diff_' + instrument + '_' + str(int(round(time.time() * 1000)))
                Statistics.to_db_table(tbl_name, 'diff', instrument, diff, dbconn)
                result_tables.append(tbl_name)
            elif stats == 'bollinger_band':
                column = j['column']
                period = j['period']
                up, down = self.bollinger_band_db(dbpath, instrument, column, period)
                df = pd.merge(up.to_frame(), down.to_frame(), left_index=True, right_index=True)
                tbl_name = 'bollinger_band_' + instrument + '_' + str(int(round(time.time() * 1000)))
                Statistics.to_db_table(tbl_name, 'bollinger_band', instrument, df, dbconn)
                result_tables.append(tbl_name)
            elif stats == "historical_volatility":
                column = j['column']
                basic_period = j['basic_period']
                period = j['period']
                annual_period = j['annual_period']
                hvol = self.hist_volatility_db(dbpath, instrument, column, basic_period, period, annual_period)
                tbl_name = 'hist_volatility_' + instrument + '_' + str(int(round(time.time() * 1000)))
                Statistics.to_db_table(tbl_name, 'hist_volatility', instrument, hvol, dbconn)
                result_tables.append(tbl_name)

        except KeyError:
            return None

        dbconn.close()
        return result_tables

    @staticmethod
    def read_from_db(dbpath, instrument):
        result = None
        dbconn = sqlite3.connect(dbpath)
        query = "select * from " + instrument + ";"
        try:
            result = pd.read_sql_query(query, dbconn)
        except:
            pass
        dbconn.close()
        return result

    @staticmethod
    def set_datetime(df):
        df['date'] = df['date'].apply(pd.to_datetime)
        df.set_index('date', inplace=True)
        return df

    # day to day return in case len(xarr) = 2
    @staticmethod
    def continuously_compounded_return(xarr):
        if len(xarr) < 2:
            return np.NaN
        last_idx = len(xarr) - 1
        if xarr[0] == 0 or xarr[0] is None:
            return np.NaN
        return math.log(xarr[last_idx] / xarr[0])

    # period = how many basic_period in it
    @staticmethod
    def hist_volatility(df, column="close", basic_period=1, period=21, annual_period=252):
        Statistics.set_datetime(df)
        cc_ret = df[column].rolling(window=basic_period+1).apply(Statistics.continuously_compounded_return)
        std = cc_ret.rolling(window=period, min_periods=period-1).std()
        sq = math.sqrt(annual_period)
        hvol = std.apply(lambda x: x*sq)
        return hvol

    @staticmethod
    def hist_volatility_db(dbpath, instrument, column="close", basic_period=1, period=21, annual_period=252):
        df = Statistics.read_from_db(dbpath, instrument)
        return Statistics.hist_volatility(df, column, basic_period, period, annual_period)

    @staticmethod
    def sma(df, column="close", period=20):
        if df is None:
            return None
        Statistics.set_datetime(df)
        return df[column].rolling(window=period, min_periods=period - 1).mean()

    @staticmethod
    def sma_db(dbpath, instrument, column="close", period=20):
        df = Statistics.read_from_db(dbpath, instrument)
        return Statistics.sma(df, column, period)

    @staticmethod
    def ema(df, column="close", period=20):
        if df is None:
            return None
        Statistics.set_datetime(df)
        return df[column].ewm(span=period, min_periods=period - 1).mean()

    @staticmethod
    def ema_db(dbpath, instrument, column="close", period=20):
        df = Statistics.read_from_db(dbpath, instrument)
        return Statistics.ema(df, column, period)

    @staticmethod
    def rsi(df, column="close", period=14):
        if df is None:
            return None
        # wilder RSI
        Statistics.set_datetime(df)
        delta = df[column].diff()
        up, down = delta.copy(), delta.copy()

        up[up < 0] = 0
        down[down > 0] = 0

        rUp = up.ewm(span=period, adjust=False, min_periods=period - 1).mean()
        rDown = down.ewm(span=period, adjust=False, min_periods=period - 1).mean().abs()

        return 100 - 100 / (1 + rUp / rDown)

    @staticmethod
    def rsi_db(dbpath, instrument, column="close", period=14):
        df = Statistics.read_from_db(dbpath, instrument)
        return Statistics.rsi(df, column, period)

    # MACD, MACD Signal and MACD difference
    @staticmethod
    def macd(df, n_fast=12, n_slow=26, n_sign=9, column="close"):
        if df is None:
            return None, None, None
        Statistics.set_datetime(df)
        fast = df[column].ewm(span=n_fast, min_periods=n_fast - 1).mean()
        slow = df[column].ewm(span=n_slow, min_periods=n_slow - 1).mean()
        mcd = fast - slow
        sign = mcd.ewm(span=n_sign, min_periods=n_sign - 1).mean()
        diff = mcd - sign

        return mcd, sign, diff

    @staticmethod
    def macd_db(dbpath, instrument, n_fast=12, n_slow=26, n_sign=9, column="close"):
        df = Statistics.read_from_db(dbpath, instrument)
        return Statistics.macd(df, n_fast, n_slow, n_sign, column)

    @staticmethod
    def bollinger_band(df, column="close", period=20):
        if df is None:
            return None
        Statistics.set_datetime(df)
        sma = df[column].rolling(window=period, min_periods=period - 1).mean()
        std = df[column].rolling(window=period, min_periods=period - 1).std()
        up = (sma + (std * 2))
        down = (sma - (std * 2))
        return up, down

    @staticmethod
    def bollinger_band_db(dbpath, instrument, column="close", period=20):
        df = Statistics.read_from_db(dbpath, instrument)
        return Statistics.bollinger_band(df, column, period)


import matplotlib.pyplot as plt


class TestStatisticsConsumerMock(threading.Thread):

    def __init__(self):
        super(TestStatisticsConsumerMock, self).__init__()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_CORE)
        self.channel.basic_consume(self.on_msg, queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_CORE, no_ack=False)
        self.queue = queue.Queue()

    def on_msg(self, unused_channel, method, properties, body):
        #print("TestStatisticsConsumerMock: on_msg", body)
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


def display_tables(dbpath, tables):
    dbconn = sqlite3.connect(dbpath)
    plt.figure()
    for tbl in tables:
        df = pd.read_sql_query("select * from " + tbl + ";", dbconn)
        Statistics.set_datetime(df)
        df.plot(title=tbl, grid=True, figsize=(16,8), label=tbl)
    plt.legend()
    plt.show()
    dbconn.close()


def add_to_tables_to_drop(tables, json_str):
    j = json.loads(json_str)
    tbls = j["tables"]
    for tbl in tbls:
        tables.append(tbl)


def drop_tables(dbpath, tables):
    dbconn = sqlite3.connect(dbpath)
    cursor = dbconn.cursor()
    for tbl in tables:
        cursor.execute('drop table ' + tbl + ';')
    dbconn.commit()
    dbconn.close()


if __name__ == "__main__":

    #"""
    consumer = TestStatisticsConsumerMock()
    consumer.start()
    msg_broker = msg_broker_rabbitmq.MsgBrokerRabbitMQ()
    statistics = Statistics(msg_broker)
    statistics.start()
    msg_broker.reset_queues()
    msg_broker.start()

    tables_to_drop = []
    dbpath = "../../db/yotta.sqlite"

    request = '{"statistics":"sma","dbpath":"../../db/yotta.sqlite","instrument":"MSFT","column":"close","period":20}'
    msg_broker.put_msg_for(request, abstract_msg_broker.AbstractMsgBroker.QUEUE_STATISTICS)
    print("main: put request for QUEUE_STATISTICS: ", request)
    result = consumer.get_result()
    print("got result:")
    print(result)
    add_to_tables_to_drop(tables_to_drop, result)

    request = '{"statistics":"ema","dbpath":"../../db/yotta.sqlite","instrument":"MSFT","column":"close","period":20}'
    msg_broker.put_msg_for(request, abstract_msg_broker.AbstractMsgBroker.QUEUE_STATISTICS)
    print("main: put request for QUEUE_STATISTICS: ", request)
    result = consumer.get_result()
    print("got result:")
    print(result)
    add_to_tables_to_drop(tables_to_drop, result)

    request = '{"statistics":"bollinger_band","dbpath":"../../db/yotta.sqlite","instrument":"MSFT","column":"close","period":20}'
    msg_broker.put_msg_for(request, abstract_msg_broker.AbstractMsgBroker.QUEUE_STATISTICS)
    print("main: put request for QUEUE_STATISTICS: ", request)
    result = consumer.get_result()
    print("got result:")
    print(result)
    add_to_tables_to_drop(tables_to_drop, result)

    request = '{"statistics":"rsi","dbpath":"../../db/yotta.sqlite","instrument":"MSFT","column":"close","period":14}'
    msg_broker.put_msg_for(request, abstract_msg_broker.AbstractMsgBroker.QUEUE_STATISTICS)
    print("main: put request for QUEUE_STATISTICS: ", request)
    result = consumer.get_result()
    print("got result:")
    print(result)
    add_to_tables_to_drop(tables_to_drop, result)

    request = '{"statistics":"historical_volatility","dbpath":"../../db/yotta.sqlite","instrument":"MSFT","column":"close","basic_period":1,"period":21,"annual_period":252}'
    msg_broker.put_msg_for(request, abstract_msg_broker.AbstractMsgBroker.QUEUE_STATISTICS)
    print("main: put request for QUEUE_STATISTICS: ", request)
    result = consumer.get_result()
    print("got result:")
    print(result)
    add_to_tables_to_drop(tables_to_drop, result)

    while True:
        try:
            time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            print("Exiting...")
            break

    msg_broker.stop()
    statistics.stop()
    consumer.stop()

    display_tables(dbpath, tables_to_drop)
    drop_tables(dbpath, tables_to_drop)

    print("Done.")
    #"""


    """
    if len(sys.argv) != 3:
        print("usage: python3.6 statistics.py <dbpath> <instrument>")
        sys.exit(-1)

    dbpath = sys.argv[1]
    instrument = sys.argv[2]

    sma = Statistics.sma_db(dbpath, instrument)
    ema = Statistics.ema_db(dbpath, instrument)

    plt.figure()

    if sma is not None:
        sma.plot(title=instrument, grid=True, figsize=(16,8), label='SMA')
    if ema is not None:
        ema.plot(title=instrument, grid=True, figsize=(16,8), label='EMA')

    plt.legend()
    plt.show()

    plt.figure()

    rsi = Statistics.rsi_db(dbpath, instrument)
    if rsi is not None:
        rsi.plot(title=instrument, grid=True, figsize=(16,8),label='RSI')

    plt.legend()
    plt.show()

    plt.figure()

    macd, sign, hist = Statistics.macd_db(dbpath, instrument)

    if macd is not None and sign is not None and hist is not None:
        macd.plot(title=instrument, grid=True, figsize=(16, 8), label='MACD')
        sign.plot(title=instrument, grid=True, figsize=(16, 8), label='EMA9')
        hist.plot(title=instrument, grid=True, figsize=(16, 8), label='DIFF')

    plt.legend()
    plt.show()

    plt.figure()
    up, down = Statistics.bollinger_band_db(dbpath, instrument)
    title = instrument + " Bollinger Band"
    up.plot(title=title, grid=True, figsize=(16,8), label='UP')
    down.plot(title=title, grid=True, figsize=(16,8), label='DOWN')
    plt.legend()
    plt.show()

    plt.figure()
    hvol = Statistics.hist_volatility_db(dbpath, instrument)
    hvol.plot(title=instrument, grid=True, figsize=(16,8), label='HISTORICAL VOLATILITY')
    plt.legend()
    plt.show()
    """
