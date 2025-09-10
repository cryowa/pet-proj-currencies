import requests
import json
#import datetime
import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "test-topic"
start_url = "https://www.okx.com"
middle_part_url = "/api/v5/market/tickers"
params_url = "?instType=SPOT&uly=BTC-USD"
url = f"{start_url}{middle_part_url}{params_url}"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def get_okx_crypto_price_data():
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        producer.send(TOPIC,value=data)
        producer.flush()
        #dir_name = params_url[params_url.find("=")+1:]
        #with open(f'CryptoCcys/{dir_name}/okx_{dir_name}_{int(datetime.datetime.now().timestamp()*1000)}.json', 'w', encoding='utf-8') as f:
        #    json.dump(data, f, ensure_ascii=False, indent=4)
        print("Message delivered to Kafka",datetime.datetime.now())
    else:
        print(f"Error: {response.status_code}")
    

while True:
    get_okx_crypto_price_data()
    


