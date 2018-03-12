import unittest
import abstract_msg_broker
import pika
import queue
import threading


class MsgBrokerRabbitMQThreadHelper(threading.Thread):

    def __init__(self, channel):
        super(MsgBrokerRabbitMQThreadHelper, self).__init__()
        self.channel = channel

    def run(self):
        self.channel.start_consuming()

    def stop(self):
        self.channel.stop_consuming()


class MsgBrokerRabbitMQ(abstract_msg_broker.AbstractMsgBroker):

    def __init__(self):
        super(MsgBrokerRabbitMQ, self).__init__('rabbitmq')
        self.msg_queue = queue.Queue()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        self.channel.queue_declare(queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_CORE)
        self.channel.queue_declare(queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_STATISTICS)
        self.channel.basic_consume(self.on_message, queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER, no_ack=False)
        self.channel.basic_consume(self.on_message, queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_STATISTICS, no_ack=False)
        self.thr = MsgBrokerRabbitMQThreadHelper(self.channel)

    def reset_queues(self):
        self.channel.queue_purge(queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        self.channel.queue_purge(queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_CORE)
        self.channel.queue_purge(queue=abstract_msg_broker.AbstractMsgBroker.QUEUE_STATISTICS)

    def start(self):
        self.thr.start()

    def stop(self):
        self.thr.stop()

        try:
            self.connection.close()
        except AttributeError:
            pass

        self.thr.join()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        # put body into the local queue
        self.msg_queue.put(body)
        self.channel.basic_ack(basic_deliver.delivery_tag)

    def get_next_msg_for(self, queue_destination):
        try:
            item = self.msg_queue.get(True, 1)
            self.msg_queue.task_done()
        except queue.Empty:
            item = None
        return item

    def put_msg_for(self, msg, queue_destination):
        self.channel.basic_publish(exchange='', routing_key=queue_destination, body=msg)


class TesMsgBrokerRabbitMQ(unittest.TestCase):

    def setUp(self):
        self.mbroker = MsgBrokerRabbitMQ()
        self.mbroker.reset_queues()
        self.mbroker.start()

    def tearDown(self):
        self.mbroker.stop()

    def test_one_basic_in_out_queue(self):
        msg_1 = 'first'
        msg_2 = 'second'
        msg_3 = 'third'
        self.mbroker.put_msg_for(msg_1, abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        self.mbroker.put_msg_for(msg_2, abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        self.mbroker.put_msg_for(msg_3, abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        got_msg_1 = self.mbroker.get_next_msg_for(abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        got_msg_2 = self.mbroker.get_next_msg_for(abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        got_msg_3 = self.mbroker.get_next_msg_for(abstract_msg_broker.AbstractMsgBroker.QUEUE_PUMPER)
        self.assertEquals(msg_1, got_msg_1)
        self.assertEquals(msg_2, got_msg_2)
        self.assertEquals(msg_3, got_msg_3)
        self.mbroker.stop()
        print('Done')


if __name__ == '__main__':
    unittest.main()
