class Scheduler(object):
    fin_data_pumper_ = None
    fin_data_consumer_ = None

    def __init__(self, fin_data_pumper, fin_data_consumer):
        self.fin_data_pumper_ = fin_data_pumper
        self.fin_data_consumer_ = fin_data_consumer

    def loop_fun(self):
        raise NotImplementedError('not implemented')