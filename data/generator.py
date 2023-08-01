import json
from datetime import datetime, timedelta, timezone
from random import randint, randrange
from time import sleep
from uuid import uuid4

from kafka import KafkaProducer


def now():
    return datetime.now(timezone(timedelta(hours=7))).isoformat(timespec="seconds")


users = [
    dict(user_id="1", full_name="Cordell Caldwell"),
    dict(user_id="2", full_name="Mike Fisher"),
    dict(user_id="3", full_name="Lea Bush"),
    dict(user_id="4", full_name="Gonzalo Dodson"),
    dict(user_id="5", full_name="Giuseppe Carson"),
    dict(user_id="6", full_name="Hershel Jefferson"),
    dict(user_id="7", full_name="Delia Mcintyre"),
    dict(user_id="8", full_name="Kareem Mcintosh"),
    dict(user_id="9", full_name="Yvonne Mata"),
    dict(user_id="10", full_name="Eloy Browning"),
]
products = [
    dict(product_id="1", product_name="Indomie Goreng", price=2950.0),
    dict(product_id="2", product_name="Indomie Soto", price=2700.0),
    dict(product_id="3", product_name="Indomie Ayam Bawang", price=2700.0),
    dict(product_id="4", product_name="Telor", price=30000.0),
    dict(product_id="5", product_name="Kecap Bango 210 ML", price=10300.0),
    dict(product_id="6", product_name="Aqua 1500 ML", price=5700.0),
    dict(product_id="7", product_name="Pisang Cavendish 1 KG", price=22500.0),
]


def generate_trx_details(trx_id):
    n = len(products)
    for i in {randrange(n) for j in range(n)}:
        product = products[i]
        yield dict(
            trx_id=trx_id,
            product_id=product["product_id"],
            qty=randint(1, 10),
            price=product["price"],
        )


def generate_trx():
    user = users[randrange(len(users))]
    return dict(
        trx_id=str(uuid4()),
        user_id=user["user_id"],
        created_date=now(),
    )


producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    trx = generate_trx()
    trx_details = generate_trx_details(trx["trx_id"])
    producer.send(topic="transactions", value=trx)
    for td in trx_details:
        producer.send(topic="transaction-details", value=td)
    producer.flush()
    sleep(randrange(5))
