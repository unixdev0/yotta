class FinDataConsumer:
    def consume(self, data):
        raise NotImplementedError("not implemented")


class PrintFinDataConsumer(FinDataConsumer):
    def consume(self, data):
        print data
