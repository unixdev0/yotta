class AbstractMsgBroker(object):

    # destinations for queues
    # queue destination
    QUEUE_PUMPER = 'dest_pumper'
    QUEUE_CORE = 'dest_core'

    def __init__(self, name):
        self.name = name

    def start(self):
        raise NotImplementedError('not implemented')

    def stop(self):
        raise NotImplementedError('not implemented')

    def get_next_msg_for(self, queue_destination):
        raise NotImplementedError('not implemented')

    def put_msg_for(self, msg, queue_destination):
        raise NotImplementedError('not implemented')
