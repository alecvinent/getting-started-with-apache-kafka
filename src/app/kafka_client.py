import logging
from typing import List, Any

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import threading
import queue


class KafkaClient:
    def __init__(self, brokers, topic):
        self._brokers = brokers
        self._topic = topic
        self._producer = KafkaProducer(bootstrap_servers=self._brokers)
        self._admin_client = KafkaAdminClient(bootstrap_servers=self._brokers)

        self._create_topic()

        self._messages = queue.Queue()
        self._consumer_thread = threading.Thread(target=self.consume_messages)
        self._consumer_thread.start()

    def send_message(self, message) -> bool:
        logging.info("sending messages")
        self._producer.send(self._topic, message.encode('utf-8'))
        return True

    def read_messages(self) -> List[Any]:
        logging.info("reading messages")
        messages = []
        while not self._messages.empty():
            messages.append(self._messages.get())
        return messages

    def consume_messages(self) -> None:
        logging.info("consuming messages")
        consumer = KafkaConsumer(self._topic, bootstrap_servers=self._brokers)
        for message in consumer:
            self._messages.put(message.value.decode('utf-8'))

    def _create_topic(self, num_partitions=1, replication_factor=1) -> bool:
        logging.info("creating topics: %s" % self._topic)
        try:
            topic_list = [
                NewTopic(
                    name=self._topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor
                )
            ]
            self._admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except Exception as ex:
            logging.warning(ex)
        return True
