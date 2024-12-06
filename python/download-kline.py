#!/usr/bin/env python

"""
  script to download klines.
  set the absolute path destination folder for STORE_DIRECTORY, and run

  e.g. STORE_DIRECTORY=/data/ ./download-kline.py

"""
import os
import sys
from datetime import *
import pandas as pd
from enums import *
from utility import download_file, get_all_symbols, get_parser, get_start_end_date_objects, convert_to_date_object, \
    get_path, get_destination_dir
from verify_send import verify_checksum, parse_zip_and_send_to_kafka


def download_monthly_klines(trading_type, symbols, num_symbols, intervals, years, months, start_date, end_date, folder,
                            checksum, kafka_topic, kafka_servers):
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    print("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        print("[{}/{}] - start download monthly {} klines ".format(current + 1, num_symbols, symbol))
        for interval in intervals:
            for year in years:
                for month in months:
                    current_date = convert_to_date_object('{}-{}-01'.format(year, month))
                    if current_date >= start_date and current_date <= end_date:
                        path = get_path(trading_type, "klines", "monthly", symbol, interval)
                        file_name = "{}-{}-{}-{}.zip".format(symbol.upper(), interval, year, '{:02d}'.format(month))
                        download_file(path, file_name, date_range, folder)

                        if checksum == 1:
                            checksum_path = get_path(trading_type, "klines", "monthly", symbol, interval)
                            checksum_file_name = "{}-{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, year,
                                                                                   '{:02d}'.format(month))
                            download_file(checksum_path, checksum_file_name, date_range, folder)

                            save_checksum_path = get_destination_dir(os.path.join(checksum_path, checksum_file_name),
                                                                     folder)
                            save_zip_path = get_destination_dir(os.path.join(path, file_name), folder)
                            is_valid, expected_checksum, actual_checksum = verify_checksum(save_zip_path,
                                                                                           save_checksum_path)
                            if not is_valid:
                                print(f"Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}")
                            else:
                                print("Checksum is valid. Proceeding with file parsing...")
                                # 调用函数解析 ZIP 并发送到 Kafka
                                parse_zip_and_send_to_kafka(save_zip_path, kafka_topic, kafka_servers)

        current += 1


def download_daily_klines(trading_type, symbols, num_symbols, intervals, dates, start_date, end_date, folder, checksum,
                          kafka_topic, kafka_servers):
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    # Get valid intervals for daily
    intervals = list(set(intervals) & set(DAILY_INTERVALS))
    print("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        print("[{}/{}] - start download daily {} klines ".format(current + 1, num_symbols, symbol))
        for interval in intervals:
            for date in dates:
                current_date = convert_to_date_object(date)
                if current_date >= start_date and current_date <= end_date:
                    path = get_path(trading_type, "klines", "daily", symbol, interval)
                    file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, date)
                    download_success = download_file(path, file_name, date_range, folder)
                    if download_success:
                        if checksum == 1:
                            checksum_path = get_path(trading_type, "klines", "daily", symbol, interval)
                            checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, date)
                            download_file(checksum_path, checksum_file_name, date_range, folder)

                            save_checksum_path = get_destination_dir(os.path.join(checksum_path, checksum_file_name),
                                                                     folder)
                            save_zip_path = get_destination_dir(os.path.join(path, file_name), folder)
                            is_valid, expected_checksum, actual_checksum = verify_checksum(save_zip_path,
                                                                                           save_checksum_path)
                            if not is_valid:
                                print(f"Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}")
                            else:
                                print("Checksum is valid. Proceeding with file parsing...")
                                # 调用函数解析 ZIP 并发送到 Kafka
                                send_success = parse_zip_and_send_to_kafka(save_zip_path, kafka_topic, kafka_servers)
                                if send_success:
                                    """
                                        删除文件
                                        :param zip_path: ZIP 文件路径
                                        """
                                    try:
                                        os.remove(save_zip_path)
                                        print(f"Deleted zip file: {save_zip_path}")
                                    except Exception as e:
                                        print(f"Failed to delete zip file {save_zip_path}: {e}")

                                    try:
                                        os.remove(save_checksum_path)
                                        print(f"Deleted checksum file: {save_checksum_path}")
                                    except Exception as e:
                                        print(f"Failed to delete checksum file {save_checksum_path}: {e}")
        current += 1


if __name__ == "__main__":
    parser = get_parser('klines')
    args = parser.parse_args(sys.argv[1:])

    if not args.symbols:
        print("fetching all symbols from exchange")
        symbols = get_all_symbols(args.type)
        num_symbols = len(symbols)
    else:
        symbols = args.symbols
        num_symbols = len(symbols)

    if args.dates:
        dates = args.dates
    else:
        period = convert_to_date_object(datetime.today().strftime('%Y-%m-%d')) - convert_to_date_object(
            PERIOD_START_DATE)
        dates = pd.date_range(end=datetime.today(), periods=period.days + 1).to_pydatetime().tolist()
        dates = [date.strftime("%Y-%m-%d") for date in dates]
        if args.skip_monthly == 0:
            download_monthly_klines(args.type, symbols, num_symbols, args.intervals, args.years, args.months,
                                    args.startDate, args.endDate, args.folder, args.checksum, args.kafkaTopic,
                                    args.kafkaServers)
    if args.skip_daily == 0:
        download_daily_klines(args.type, symbols, num_symbols, args.intervals, dates, args.startDate, args.endDate,
                              args.folder, args.checksum, args.kafkaTopic, args.kafkaServers)
