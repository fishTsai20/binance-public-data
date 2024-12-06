import hashlib
import zipfile
import json
from confluent_kafka import Producer
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def calculate_checksum(file_path, hash_algorithm="sha256"):
    """计算文件的校验值。"""
    hash_func = hashlib.new(hash_algorithm)
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def verify_checksum(zip_path, checksum_path):
    """验证 ZIP 文件的校验值是否正确。"""
    with open(checksum_path, "r") as f:
        expected_checksum = f.read().strip().split(' ')[0]

    actual_checksum = calculate_checksum(zip_path)
    return actual_checksum == expected_checksum, expected_checksum, actual_checksum


def parse_line_to_json(line):
    """
    将一行数据解析为 JSON 对象，带字段类型转换。
    :param line: 单行数据字符串
    :return: JSON 对象
    """
    # 定义列名和字段类型
    field_names = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ]
    field_types = [
        int, float, float, float, float, float,
        int, float, int,
        float, float, int
    ]

    # 分割数据
    values = line.split(',')
    if len(values) != len(field_names):
        raise ValueError(f"Unexpected number of fields: {len(values)}. Expected: {len(field_names)}")

    # 类型转换并生成 JSON
    json_data = {name: field_type(value) for name, field_type, value in zip(field_names, field_types, values)}
    return json_data


def delivery_report(err, msg):
    """
    回调函数，用于处理每条消息的发送状态
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def parse_zip_and_send_to_kafka(zip_path, kafka_topic, kafka_servers):
    """
    解析 ZIP 文件内容，将其转换为 JSON 格式并批量发送到 Kafka。
    :param zip_path: ZIP 文件路径
    :param kafka_topic: Kafka 主题名称
    :param kafka_servers: Kafka 服务器地址列表
    """
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

    start_time = datetime.now()
    logger.info("Start sending")
    send_bytes_count = 0
    try:
        # 解压并读取文件
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                with zip_ref.open(file_name) as file:
                    # 解析文件内容
                    lines = file.read().decode('utf-8').strip().split('\n')
                    for line in lines:
                        try:
                            json_data = parse_line_to_json(line)
                            message_byte = json.dumps(json_data)
                            send_bytes_count += len(message_byte)
                            producer.produce(
                                topic=kafka_topic,
                                value=message_byte,
                                callback=delivery_report
                            )
                        except ValueError as e:
                            print(f"Skipping line due to error: {e}")
    except Exception as e:
        print(f"Error while processing zip file: {e}")
        return False
    finally:
        # 确保消息都发送到 Kafka
        producer.flush(timeout=30)  # 一次性确保消息发送完成
        logger.info(f"End of sending {datetime.now() - start_time}, {send_bytes_count} Bytes")

    return True  # 成功返回 True