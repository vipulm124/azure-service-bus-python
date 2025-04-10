from azure.servicebus import ServiceBusMessage, ServiceBusSender, ServiceBusClient
from dotenv import load_dotenv
import os

load_dotenv()
connection_string = os.environ["SERVICE_BUS_CONNECTION_STRING_NORMAL_QUEUE"]
queue_name = os.environ["QUEUE_NAME"]


client = ServiceBusClient.from_connection_string(conn_str=connection_string, logging_enable=True)


def get_sender_client():
    return client.get_queue_sender(queue_name=queue_name)


def send_messages(message):
    sender_client  = get_sender_client()
    with sender_client:
        message = ServiceBusMessage(message)
        sender_client.send_messages(message)
        print("Message sent successfully!")


for i in range(1, 11):
    """Creating 10 messages for sampling"""
    message = f"{i} - This is a message from me!"
    send_messages(message)