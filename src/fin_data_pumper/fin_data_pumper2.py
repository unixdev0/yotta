import abstract_msg_broker
import threading


class FinDataPumper2(threading.Thread):

    def __init__(self, msg_broker):
        super(FinDataPumper2, self).__init__()
        self.msg_broker = msg_broker
        self._stop_event = threading.Event()

    def process_msg(self, msg):
        raise NotImplementedError('not implemented')

    def run(self):
        while not self._stop_event.isSet():
            msg = self.msg_broker.get_next_msg_for(abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
            if msg is None:
                continue
            result = self.process_msg(msg)
            if result is not None:
                self.msg_broker.put_msg_for(result, abstract_msg_broker.AbstractMsgBroker.QUEUE_CORE)

    def stop(self):
        self._stop_event.set()
        self.join()

