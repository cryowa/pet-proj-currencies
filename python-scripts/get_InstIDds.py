import requests
import json
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = 'okx_InstIDs'# Kafka config
# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

url = "https://www.okx.com/api/v5/market/tickers?instType="
TYPES = ['SPOT', 'SWAP', 'FUTURES', 'OPTION']

def getInstIDs(url,TYPES):
    instIDs = []
    for type in TYPES:
        try:
            response = requests.get(url+type)
            if response:
                raw_data = response.json()['data']
                for item in raw_data:
                    instIDs.append({'instType':item['instType'],'instId':item['instId']})
        except:
            print(response)
    return instIDs

# def send_to_kafka(topic, message):
#     producer.send(topic, value=message)
#     producer.flush()

data = getInstIDs(url,TYPES)

print(*data,sep='\n')

