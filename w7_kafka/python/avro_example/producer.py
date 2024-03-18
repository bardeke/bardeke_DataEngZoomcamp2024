import os
import csv
from time import sleep
from typing import Dict

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from ride_record_key import RideRecordKey, ride_record_key_to_dict
from ride_record import RideRecord, ride_record_to_dict
from settings import RIDE_KEY_SCHEMA_PATH, RIDE_VALUE_SCHEMA_PATH, \
    SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideAvroProducer:
    def __init__(self, props: Dict):
        # Schema Registry and Serializer-Deserializer Configurations
        key_schema_str = self.load_schema(props['schema.key'])
        value_schema_str = self.load_schema(props['schema.value'])
        schema_registry_props = {'url': props['schema_registry.url']}
        schema_registry_client = SchemaRegistryClient(schema_registry_props)
        self.key_serializer = AvroSerializer(schema_registry_client, key_schema_str, ride_record_key_to_dict)
        self.value_serializer = AvroSerializer(schema_registry_client, value_schema_str, ride_record_to_dict)

        # Producer Configuration
        producer_props = {'bootstrap.servers': props['bootstrap.servers']}
        self.producer = Producer(producer_props)

    @staticmethod
    def load_schema(schema_path: str):
        path = os.path.realpath(os.path.dirname(__file__))
        with open(f"{path}/{schema_path}") as f:
            schema_str = f.read()
        return schema_str

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print("Delivery failed for record {}: {}".format(msg.key(), err))
            return
        print('Record {} successfully produced to {} [{}] at offset {}'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()))

    @staticmethod
    def read_records(resource_path: str):
        ride_records, ride_keys = [], []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                ride_records.append(RideRecord(arr=[row[0], row[3], row[4], row[9], row[16]]))
                ride_keys.append(RideRecordKey(vendor_id=int(row[0])))
        return zip(ride_keys, ride_records)

    def publish(self, topic: str, records: [RideRecordKey, RideRecord]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.produce(topic=topic,
                                      key=self.key_serializer(key, SerializationContext(topic=topic,
                                                                                        field=MessageField.KEY)),
                                      value=self.value_serializer(value, SerializationContext(topic=topic,
                                                                                              field=MessageField.VALUE)),
                                      on_delivery=delivery_report)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'schema_registry.url': SCHEMA_REGISTRY_URL,
        'schema.key': RIDE_KEY_SCHEMA_PATH,
        'schema.value': RIDE_VALUE_SCHEMA_PATH
    }
    producer = RideAvroProducer(props=config)
    ride_records = producer.read_records(resource_path=INPUT_DATA_PATH)
    producer.publish(topic=KAFKA_TOPIC, records=ride_records)


    '''
    The `RideRecordKey` and `RideRecord` classes, along with their associated functions (`ride_record_key_to_dict`, `dict_to_ride_record_key`, `ride_record_to_dict`, and `dict_to_ride_record`), are used in the `producer.py` script for a very specific purpose in the context of working with Apache Kafka and Avro serialization. Here's a breakdown of their usage:

### Usage of `RideRecordKey` and `RideRecord`
- These classes represent the data structure of Kafka message keys (`RideRecordKey`) and values (`RideRecord`) that are to be produced to a Kafka topic.
- In this context, `RideRecordKey` might represent a unique identifier for each ride (e.g., based on the vendor ID), while `RideRecord` holds detailed information about each ride.

### Serialization for Kafka Production
- The `producer.py` script is designed to send messages to a Kafka topic. Messages in Kafka consist of keys and values, both of which can be serialized (converted into a format suitable for transmission over the network) in various ways. Here, Avro serialization is used.
- Avro is a binary serialization format that requires a schema for data encoding and decoding. It's often used in Kafka to ensure that the structure of produced and consumed messages is clear and agreed upon.

### How `RideRecordKey` and `RideRecord` are Integrated into Kafka Messages
- The `RideAvroProducer` class is defined to handle the production of messages to a Kafka topic. It initializes serializers for both the key and the value parts of the messages using Avro schemas.
- `key_serializer` is set up with the schema for the message key and is told how to convert a `RideRecordKey` object into a dictionary format (`ride_record_key_to_dict`) that matches the Avro schema.
- `value_serializer` does the same for `RideRecord` objects, using `ride_record_to_dict` to convert the object into a dictionary format that matches its corresponding Avro schema.

### Publishing Messages to Kafka
- The `publish` method of `RideAvroProducer` reads records (using `read_records`), which essentially reads data from a CSV file and converts each row into a `RideRecord` and its associated `RideRecordKey`.
- It then loops through these records, serializes the keys and values using the configured serializers, and produces them to the specified Kafka topic.
- The `produce` method of the `Producer` class from `confluent_kafka` is used to send these serialized messages to Kafka, with `delivery_report` being called back for each message to notify whether its delivery was successful or failed.

In summary, `RideRecordKey` and `RideRecord` serve as structured representations of the data that will be produced to a Kafka topic. Their integration with Avro serialization allows for efficient, schema-validated transmission of data within a Kafka ecosystem.
    '''
