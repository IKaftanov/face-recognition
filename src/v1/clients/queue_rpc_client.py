import pika
import uuid
import pickle


class RpcClient:
    def __init__(self, host, client_name, queue_name):
        self.host = host
        self.client_name = client_name
        self.queue_name = queue_name

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, data):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=pickle.dumps(data))
        while self.response is None:
            self.connection.process_data_events()
        return pickle.loads(self.response)

