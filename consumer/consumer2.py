import time
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiver, ServiceBusMessageBatch

from dotenv import load_dotenv
import os

load_dotenv()
connection_string = os.environ["SERVICE_BUS_CONNECTION_STRING_NORMAL_QUEUE"]
queue_name = os.environ["QUEUE_NAME"]


client = ServiceBusClient.from_connection_string(conn_str=connection_string, logging_enable=True)

receiver = client.get_queue_receiver(queue_name=queue_name, max_weight_time=5)


while True:
    
    messages = receiver.receive_messages(max_message_count = 1, max_wait_time = 5)

    for msg in messages:
        print("Consumer 2")
        print(f"MessageId : {msg.message_id}")
        print(f"SequenceNo : {msg.sequence_number}")
        print("Received: " + str(msg))
        receiver.complete_message(msg)
        print("Completed: " + str(msg))
        
    time.sleep(8)