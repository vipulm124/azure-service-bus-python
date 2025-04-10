from dotenv import load_dotenv
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import NEXT_AVAILABLE_SESSION
from azure.servicebus.exceptions import ServiceBusError
import os
import asyncio

load_dotenv()
connection_string = os.environ["SERVICE_BUS_CONNECTION_STRING_SESSION_QUEUE"]
queue_name = os.environ["QUEUE_NAME_WITH_SESSION_ENABLED"]

async def process_chat(session):
    async with session:
        session_id = session.session_id
        print(f"📥 Started session: {session_id}")
        try:
            async for msg in session:
                print(f"[{session_id}] Received: {msg.body.decode()}")
                await session.complete_message(msg)
        except ServiceBusError as e:
            print(f"❌ Error in session {session_id}: {e}")
        finally:
            print(f"✅ Finished session: {session_id}")

async def main():
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_string)

    async with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(
            queue_name=queue_name,
            session_id=NEXT_AVAILABLE_SESSION
        )

        # Process multiple sessions concurrently
        async with receiver:
            await receiver.session.set_state("OPEN")
            messages = await receiver.receive_messages(max_message_count=10, max_wait_time=5)
            for msg in messages:
                print(f"Messages: {msg}")
                await receiver.complete_message(msg)

            await receiver.session.set_state("CLOSED")
            

while True:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")
        break
    except Exception as e:
        print(f"Error: {e}")