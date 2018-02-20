import pika
import threading
import time
import datetime


class Sender(threading.Thread):

    def __init__(self):
        super(Sender, self).__init__()
        self._stop_event = threading.Event()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='hello')

    def run(self):
        while not self._stop_event.isSet():
            t = datetime.datetime.now().time()
            msg = 'now is: ' + t.strftime("%H:%M:%S")
            self.channel.basic_publish(exchange='', routing_key='hello', body=msg)
            print (" [x] Sent ---> " + msg)
            time.sleep(1)

    def stop(self):
        self._stop_event.set()
        self.join()
        self.connection.close()


class Receiver(threading.Thread):

    def __init__(self):
        super(Receiver, self).__init__()
        self._stop_event = threading.Event()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='hello')
        self.channel.basic_consume(self.on_msg, queue='hello', no_ack=False)

    def on_msg(self, unused_channel, method, properties, body):
        print ("<---- Received [x] : " + body)
        self.channel.basic_ack(method.delivery_tag)

    def run(self):
        self.channel.start_consuming()

    def stop(self):
        self.channel.stop_consuming()
        self.connection.close()


if __name__ == '__main__':

    sender = Sender()
    sender.start()

    receiver = Receiver()
    receiver.start()

    while True:
        try:
            time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            print "Exiting..."
            break

    sender.stop()
    receiver.stop()
    print 'Done'
