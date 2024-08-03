from aiokafka import AIOKafkaProducer
import asyncio

async def send_one():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:29092')
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait("testing", b"send message 1")
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

asyncio.run(send_one())