import scheduler
import time


class SchedulerDummy(scheduler.Scheduler):

    def __init__(self, fin_data_pumper, fin_data_consumer):
        super(SchedulerDummy, self).__init__(fin_data_pumper, fin_data_consumer)

    def loop_fun(self):

        # init instruments
        self.fin_data_pumper_.pump_init_info(self.fin_data_consumer_)

        time.sleep(1)

        count = 0
        while count < 5:
            self.fin_data_pumper_.pump_market_data(self.fin_data_consumer_)
            count += 1
            time.sleep(5)
