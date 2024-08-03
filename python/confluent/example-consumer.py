from confluent_kafka import Consumer

conf = {'bootstrap.servers': 'localhost:29092',
        'group.id':'foo',
        'auto.offset.reset': 'smallest'
    }

c = Consumer(conf)
c.subscribe(['testing'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        c.close()

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()