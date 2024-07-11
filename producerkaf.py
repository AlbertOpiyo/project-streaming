import requests
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import time
from kafka import KafkaProducer


url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
api_key = 'ce291ebc-18d1-4113-92d1-d9a9319667c2'
parameters = {
  'start':'1',
  'limit':'5',
  'convert':'USD'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': api_key,
}

#configure kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8')
                        )


session = Session()
session.headers.update(headers)

while True:                         
    try:
        response = session.get(url, params=parameters)
        response.raise_for_status()
        data = response.json()
        print(data)

        # send data to kafka
        producer.send('coinmarket', value=data)
        # producer.flush()
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(f"Error occurred: {e}")
    
    time.sleep(60)