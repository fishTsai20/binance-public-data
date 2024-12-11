import json
import sys

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import logging
from functools import wraps

from confluent_kafka import Producer

from verify_send import delivery_report
from utility import get_oklink_usdt_parser

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = 'https://www.oklink.com/api/v5/explorer/stablecoin/printing-record'


def retry_decorator(max_retries=5, base_delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        logging.error(f"Max retries reached. Last error: {str(e)}")
                        raise
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"Attempt {attempt + 1} failed. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)

        return wrapper

    return decorator


@retry_decorator(max_retries=5, base_delay=1)
def make_api_request(params, headers):
    response = requests.get(BASE_URL, params=params, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()


def fetch_usdt_issuance(start_time, end_time, apiKey, kafka_servers, kafka_topic, network='all'):
    # 初始化 Kafka Producer
    options = {
        'bootstrap.servers': kafka_servers[0],
        'max.in.flight.requests.per.connection': 1,
        'enable.idempotence': True,
        'linger.ms': 1000,  # 可以根据需求调整，设置消息批量发送的延迟
        'queue.buffering.max.messages': 2147483647,
        'queue.buffering.max.kbytes': 2147483647,
        'message.max.bytes': 15728640,
    }
    producer = Producer(**options)

    sink_start_time = datetime.now()
    logging.info("Start sending")
    send_bytes_count = 0

    params = {
        'stablecoinName': 'USDT',
        'network': network,
        'type': 'printing',
        'limit': '100',  # Maximum allowed
        'startTime': start_time,
        'endTime': end_time
    }
    headers = {'Ok-Access-Key': apiKey}

    all_records = []
    page = 1
    maxTransactionTime = 0

    while True:
        params['page'] = str(page)
        logging.info(f"Fetching page {page}...")

        try:
            data = make_api_request(params, headers)['data'][0]
            records = data['recordList']
            for record in records:
                if int(record['transactionTime']) > maxTransactionTime:
                    maxTransactionTime = int(record['transactionTime'])
                message_byte = json.dumps(record)
                send_bytes_count += len(message_byte)
                producer.produce(
                    topic=kafka_topic,
                    value=message_byte,
                    callback=delivery_report
                )
            all_records.extend(records)

            logging.info(f"Fetched {len(records)} records from page {page}")

            if len(records) < 100 or int(data['page']) >= int(data['totalPage']):
                return all_records

            page += 1
            delay = random.uniform(1, 3)
            logging.info(f"Waiting for {delay:.2f} seconds before next request...")
            time.sleep(delay)

        except Exception as e:
            logging.error(f"Error fetching data: {str(e)}")
            return all_records
        finally:
            # 确保消息都发送到 Kafka
            producer.flush(timeout=30)  # 一次性确保消息发送完成
            logging.info(f"End of sending {datetime.now() - sink_start_time}, {send_bytes_count} Bytes")
    logging.info(f"Latest Transaction Time:{pd.to_datetime(maxTransactionTime, unit='ms')}")

def main(start, end, apiKey, kafka_topic, kafka_servers):
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = int((datetime.now() - timedelta(days=start)).timestamp() * 1000)

    logging.info("Starting to fetch USDT issuance records for the {} days - {} days ago...".format(start, end))
    records = fetch_usdt_issuance(start_time, end_time, apiKey, kafka_servers, kafka_topic)

    if records:
        logging.info(f"Total records fetched: {len(records)}，latest state is ")
    else:
        logging.warning("No records fetched or an error occurred.")


if __name__ == "__main__":
    parser = get_oklink_usdt_parser()
    args = parser.parse_args(sys.argv[1:])
    main(args.start, args.end, args.apiKey, args.kafkaTopic, args.kafkaServers)
