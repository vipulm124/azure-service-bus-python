from dotenv import load_dotenv
from azure.servicebus import ServiceBusMessage, ServiceBusSender, ServiceBusClient
import os

load_dotenv()
connection_string = os.environ["SERVICE_BUS_CONNECTION_STRING_SESSION_QUEUE"]
queue_name = os.environ["QUEUE_NAME_WITH_SESSION_ENABLED"]


client = ServiceBusClient.from_connection_string(conn_str=connection_string)
sender = client.get_queue_sender(queue_name=queue_name)

customers = ["CustA", "CustB", "CustC", "CustD", "CustE", "CustF"]


# Session feature is only avaialble in Azure starting from Standard Pricing Tier
# and above. It is not available in Basic Pricing Tier.
with sender:
    for customer in customers:
        for i in range(1,4):
            message = ServiceBusMessage(
                body=f"Step {i} - This is a message from {customer}!",
                session_id=customer
            )
            sender.send_messages(message)
            print(f"Message sent successfully! {message}")
