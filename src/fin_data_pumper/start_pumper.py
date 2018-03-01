import sys
import time
import datetime
import threading
import sqlite3
import abstract_msg_broker
import msg_broker_rabbitmq
import pika
import fin_data_pumper_quandl
import pandas as pd

DB_SCHEMA_VERSION = 1
DB_CHECK_TIMEOUT_IN_SECONDS = 10


class DummyScheduler(threading.Thread):

    def __init__(self, dbname, msg_broker):
        super(DummyScheduler, self).__init__()
        self.dbname = dbname
        self.msg_broker = msg_broker
        self.db_conn = None
        self.db_cursor = None
        self._stop_event = threading.Event()

    def get_db_schema_version(self):
        self.db_cursor.execute('select version from schema_version;')
        return self.db_cursor.fetchone()[0]

    def get_instruments_to_subscribe(self):
        self.db_cursor.execute('select feedcode from instruments;')
        return self.db_cursor.fetchall()

    def assure_table_exists(self, instrument):
        self.db_cursor.execute(
            """create table if not exists %s (
                date      DATETIME PRIMARY KEY,
                open      FLOAT,
                high      FLOAT,
                low       FLOAT,
                close     FLOAT,
                volume    FLOAT,
                adj_open  FLOAT,
                adj_high  FLOAT,
                adj_low   FLOAT,
                adj_close FLOAT,
                adj_volume FLOAT
            );"""
            % instrument)

    def get_last_db_datetime(self, instrument):
        self.db_cursor.execute('select MAX(date) from %s;' % instrument)
        row = self.db_cursor.fetchone()
        if row is None:
            return None
        return pd.Timestamp(row[0])

    def pump_everything(self, instrument):
        msg = '{"command":"pump_to_db_full", "dbname":"' + self.dbname + '", "feedcode":"' + instrument + '"}'
        print(msg)
        self.msg_broker.put_msg_for(msg, abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)

    def pump_incremental(self, instrument, last_date):
        print("pump incremental *********")
        last_date_str = last_date.strftime('%Y-%m-%d')
        msg = '{"command":"pump_to_db_incremental", "dbname":"' + self.dbname + '", "feedcode":"' + instrument + '", "start_date":"' + last_date_str + '"}'
        print(msg)
        self.msg_broker.put_msg_for(msg, abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)

    def pump_market_data(self, instrument):
        self.assure_table_exists(instrument)
        last_db_datetime = self.get_last_db_datetime(instrument)
        last_working_datetime = (pd.datetime.today() - pd.tseries.offsets.BDay(1))
        last_working_datetime = last_working_datetime.floor('D')

        print('last_db_datetime of %s is ' % instrument, last_db_datetime)
        print('last_working_datetime is ', last_working_datetime)

        if last_db_datetime is pd.NaT:
            print("pump everything.......", instrument)
            self.pump_everything(instrument)
        elif last_db_datetime < last_working_datetime:
            print("pump incremental from ", last_db_datetime, instrument)
            self.pump_incremental(instrument, last_db_datetime)
        else:
            print("no pumping............", instrument)

    def run(self):
        self.db_conn = sqlite3.connect(self.dbname)
        self.db_cursor = self.db_conn.cursor()

        ver = self.get_db_schema_version()
        if ver != DB_SCHEMA_VERSION:
            print('db schema version mismatch: expected:', DB_SCHEMA_VERSION, '; actual:', ver)
            sys.exit(-1)

        epoch_time = int(time.time())
        while not self._stop_event.isSet():
            if epoch_time % DB_CHECK_TIMEOUT_IN_SECONDS == 0:
                rows = self.get_instruments_to_subscribe()
                for row in rows:
                    instrument = row[0]
                    self.pump_market_data(instrument)
                    #print('instrument->', instrument)
                #print('')
                #print('')
                #print('')
                #print('------------------')
                #print('------------------')
            time.sleep(1)
            epoch_time += 1

        self.db_conn.close()

    def stop(self):
        self._stop_event.set()
        self.join()


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print('usage: python3.6 start_pumper.py <sqlite database>')
        sys.exit(-1)

    db_name = sys.argv[1]
    print('sqlite database: ', db_name)

    msg_broker = msg_broker_rabbitmq.MsgBrokerRabbitMQ()
    msg_broker.reset_queues()
    msg_broker.start()

    fin_data_pumper = fin_data_pumper_quandl.FinDataPumperQuandl(msg_broker)
    fin_data_pumper.start()

    scheduler = DummyScheduler(db_name, msg_broker)
    scheduler.start()

    while True:
        try:
            time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            print('Exiting...')
            break

    scheduler.stop()
    fin_data_pumper.stop()
    msg_broker.stop()

    print('Done')
