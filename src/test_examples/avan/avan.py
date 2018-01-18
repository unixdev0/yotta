import sys
import symbol_set
import fin_data_consumer
import fin_data_pumper_alphavantage
import scheduler_dummy

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "symbols separated by space expected to be input parameters"
        print "exiting..."
        sys.exit(-1)

    print "got the following instruments to analyze:"
    for symbol in sys.argv:
        print symbol

    symbol_names_list = list(sys.argv[1:])
    symbol_set = symbol_set.SymbolSet(symbol_names_list)
    fin_data_consumer = fin_data_consumer.PrintFinDataConsumer()
    fin_data_pumper = fin_data_pumper_alphavantage.FinDataPumperAlphaVantage(symbol_set.symbol_name_list_)
    scheduler = scheduler_dummy.SchedulerDummy(fin_data_pumper, fin_data_consumer)

    # should be in a thread, so far running in place
    scheduler.loop_fun()
