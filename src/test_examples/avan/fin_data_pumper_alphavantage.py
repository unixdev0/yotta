import fin_data_pumper
import requests

# pip install requests for requests

# for alphavantage:
# https://www.alphavantage.co/documentation/

class FinDataPumperAlphaVantage(fin_data_pumper.FinDataPumper):
    #symbol_set_ = []
    config_bars_ = []
    query_url_ = 'https://www.alphavantage.co/query?function='
    intraday_str_ = 'TIME_SERIES_INTRADAY'
    daily_str_ = 'TIME_SERIES_DAILY'
    weekly_str_ = 'TIME_SERIES_WEEKLY'
    monthly_str_ = 'TIME_SERIES_MONTHLY'
    batch_stock_quotes_str_ = 'BATCH_STOCK_QUOTES'
    alphavantage_apikey_ = '3GCBTCH75I2PCR1X'
    #alphavantage_apikey_ = 'demo'
    symbols_set_comma_ = None

    def __init__(self, config_bars):
        #self.symbol_set_ = list(symbol_set)
        self.config_bars_ = config_bars
        self.make_symbol_set_comma()

    def make_symbol_set_comma(self):
        self.symbols_set_comma_ = '';
        first = True
        for bar in self.config_bars_:
            if first:
                self.symbols_set_comma_ = bar["feedcode"]
                first = False
            else:
                self.symbols_set_comma_ += ',' + bar["feedcode"]

    def pump_init_info(self, fin_data_consumer):
        '''
        Here we just get time series per a symbol for
        1. daily for today
        2. monthly for this month
        '''
        for bar in self.config_bars_:

            url = self.make_query_url(bar)
            if url is None:
                print 'config bar configured incorrectly:'
                print bar
            else:
                response = self.sync_request(url)
                fin_data_consumer.consume(response)

            '''
            url = self.query_url_ + self.daily_str_ + '&symbol=' + symbol + '&apikey=' + self.alphavantage_apikey_
            response = self.sync_request(url)
            fin_data_consumer.consume(response)

            url = self.query_url_ + self.monthly_str_ + '&symbol=' + symbol + '&apikey=' + self.alphavantage_apikey_
            response = self.sync_request(url)
            fin_data_consumer.consume(response)
            '''

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
            return '&interval=' + interval + 'min'
        return ''

    def pump_market_data(self, fin_data_consumer):
        url = self.query_url_ + self.batch_stock_quotes_str_ + '&symbols=' + self.symbols_set_comma_ + '&apikey=' + self.alphavantage_apikey_
        response = self.sync_request(url)
        fin_data_consumer.consume(response)

    def sync_request(self, url):
        try:
            print 'requesting ' + url + ' ------->'
            r = requests.get(url)
            #print 'got response:'
            return r.text
        except requests.RequestException as e:
            return e.message

        return 'unknown exception'
