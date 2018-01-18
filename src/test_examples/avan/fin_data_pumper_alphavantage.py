import fin_data_pumper
import requests

# pip install requests for requests

# for alphavantage:
# https://www.alphavantage.co/documentation/

class FinDataPumperAlphaVantage(fin_data_pumper.FinDataPumper):
    symbol_set_ = []
    query_url_ = 'https://www.alphavantage.co/query?function='
    daily_str_ = 'TIME_SERIES_DAILY'
    monthly_str_ = 'TIME_SERIES_MONTHLY'
    batch_stock_quotes_str_ = 'BATCH_STOCK_QUOTES'
    alphavantage_apikey_ = '3GCBTCH75I2PCR1X'
    #alphavantage_apikey_ = 'demo'
    symbols_set_comma_ = None

    def __init__(self, symbol_set):
        self.symbol_set_ = list(symbol_set)
        self.make_symbol_set_comma()

    def make_symbol_set_comma(self):
        self.symbols_set_comma_ = '';
        first = True
        for symbol  in self.symbol_set_:
            if first:
                self.symbols_set_comma_ = symbol
                first = False
            else:
                self.symbols_set_comma_ += ',' + symbol

    def pump_init_info(self, fin_data_consumer):
        '''
        Here we just get time series per a symbol for
        1. daily for today
        2. monthly for this month
        '''
        for symbol in self.symbol_set_:

            url = self.query_url_ + self.daily_str_ + '&symbol=' + symbol + '&apikey=' + self.alphavantage_apikey_
            response = self.sync_request(url)
            fin_data_consumer.consume(response)

            url = self.query_url_ + self.monthly_str_ + '&symbol=' + symbol + '&apikey=' + self.alphavantage_apikey_
            response = self.sync_request(url)
            fin_data_consumer.consume(response)

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
