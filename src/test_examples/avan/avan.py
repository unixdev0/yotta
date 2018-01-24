import sys
import json
import fin_data_consumer
import fin_data_pumper_alphavantage
import scheduler_dummy

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python avan.py <json configuration file>"
        print "exiting..."
        sys.exit(-1)

    try:
        config_data = json.load(open(sys.argv[1]));
        config_historical_bars = config_data["bars"]
    except Exception as e:
        print "Error: " + e.message
        sys.exit(0)

    print config_historical_bars

    fin_data_consumer = fin_data_consumer.PrintFinDataConsumer()
    fin_data_pumper = fin_data_pumper_alphavantage.FinDataPumperAlphaVantage(config_historical_bars)
    scheduler = scheduler_dummy.SchedulerDummy(fin_data_pumper, fin_data_consumer)

    # should be in a thread, so far running in place
    scheduler.loop_fun()
