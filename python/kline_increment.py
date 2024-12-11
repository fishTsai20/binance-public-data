import asyncio
import logging
import sys

import aiohttp
from confluent_kafka import Producer
from kafka import KafkaProducer
import json
from datetime import datetime, timedelta

from python.utility import get_parser, get_kline_increment_parser
from python.verify_send import parse_line_to_json, delivery_report

logger = logging.getLogger(__name__)

# Base URLs 和符号列表
BASE_URLS = [
    "https://api.binance.com",
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com"
]
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]  # 示例符号列表


async def fetch_kline(session, base_url, symbol, start_time):
    """发送请求以获取K线数据"""
    url = f"{base_url}/api/v3/klines?symbol={symbol}&interval=1m&startTime={start_time}"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                print(f"Success: {symbol} from {base_url}")
                return data
            else:
                print(f"Failed: {symbol} from {base_url} with status {response.status}")
                raise Exception(f"HTTP error: {response.status}")
    except Exception as e:
        print(f"Error fetching {symbol} from {base_url}: {e}")
        raise


async def fetch_with_fallback(symbol, start_time):
    """尝试从 base_url 列表中获取数据，失败时切换 URL"""
    for base_url in BASE_URLS:
        async with aiohttp.ClientSession() as session:
            try:
                return await fetch_kline(session, base_url, symbol, start_time)
            except Exception as e:
                print(f"Switching URL for {symbol}: {e}")
                continue
    print(f"All URLs failed for {symbol}")
    return None


async def process_symbols(symbols, kafka_servers, kafka_topic, start_time, csv_path):
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
    logger.info("Start sending")
    send_bytes_count = 0
    try:
        """并发处理符号列表"""
        tasks = [fetch_with_fallback(symbol, start_time) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        for symbol, result in zip(SYMBOLS, results):
            if result:
                for sublist in result:
                    try:
                        json_list = parse_line_to_json(symbol, ",".join(map(str, sublist)), csv_path)
                        for json_data in json_list:
                            message_byte = json.dumps(json_data)
                            send_bytes_count += len(message_byte)
                            producer.produce(
                                topic=kafka_topic,
                                value=message_byte,
                                callback=delivery_report
                            )

                    except ValueError as e:
                        print(f"Skipping line due to error: {e}")
            else:
                print(f"No data for {symbol}")
    except Exception as e:
        print(f"Error while processing zip file: {e}")
        return False
    finally:
        # 确保消息都发送到 Kafka
        producer.flush(timeout=30)  # 一次性确保消息发送完成
        logger.info(f"End of sending {datetime.now() - sink_start_time}, {send_bytes_count} Bytes")


async def main(symbols, interval, range, kafka_servers, kafka_topic, csv_path):
    """主循环任务"""
    while True:
        # 当前时间减去 10 分钟的时间戳，精确到毫秒
        current_time = datetime.utcnow()
        start_time = int((current_time - timedelta(seconds=range)).timestamp() * 1000)
        print(f"Start Time: {start_time}")

        # 处理符号
        await process_symbols(symbols, kafka_servers, kafka_topic, start_time, csv_path)

        # 等待 5 分钟
        print("Sleeping for {} seconds...".format(interval))
        await asyncio.sleep(interval)


if __name__ == "__main__":
    parser = get_kline_increment_parser()
    args = parser.parse_args(sys.argv[1:])
    try:
        asyncio.run(main(args.symbols, args.interval, args.range, args.kafkaServers, args.kafkaTopic, args.csvPath))
    except KeyboardInterrupt:
        print("Shutting down...")
