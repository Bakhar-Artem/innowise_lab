import logging
import sys

import pandas as pd
from watchdog.observers import Observer
from rule.impl.bundle_per_hour_rule import BundlePerHourRule
from rule.impl.minute_rule import MinuteRule
from watchdog.events import FileSystemEventHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)


class AddFileEventHandler(FileSystemEventHandler):
    """
    Class to monitor files adding to the dir
    """

    def on_created(self, event):
        logging.getLogger(name='console').info(f'created file {event.src_path}\nstarting processing')
        process_csv(event.src_path)


def read_df(path):
    """
    method to read pandas dataframe from csv by chunksize = 10**5
    :param path: path to csv file
    :return: pandas dataframe iterator
    """
    chunksize = 10 ** 5
    for chunk in pd.read_csv(path, chunksize=chunksize):
        yield chunk


def prepare_df(dataframe):
    """
    method to prepare dataframe to the next processing by rules:
        changing column names,
        cutting time till minutes,
        deleting success row
    :param dataframe: pandas dataframe
    :return: prepared pandas dataframe
    """
    column_names = ['error_code', 'error_message', 'severity', 'log_location', 'mode', 'model', 'graphics',
                    'session_id',
                    'sdkv', 'test_mode', 'flow_id', 'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id',
                    'appv',
                    'language', 'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date']
    dataframe.columns = column_names
    dataframe['date'] = pd.to_datetime(dataframe['date'], unit='s').dt.strftime('%Y-%m-%d %H:%M')
    dataframe = dataframe[dataframe['severity'] != 'Success']
    return dataframe


def get_df_chunk(path):
    for df in read_df(path):
        yield prepare_df(df)


def process_csv(src_path):
    """
    method to process csv by rules
    :param src_path: src_path to csv file
    :return:none
    """
    rules = [BundlePerHourRule(), MinuteRule()]
    for chunk in get_df_chunk(src_path):
        for i, row in chunk.iterrows():
            for rule in rules:
                rule.process_rule(row)
    for rule in rules:
        rule.analyse_errors()
    logging.getLogger(name='console').info('Processing is finished')


if __name__ == '__main__':
    event_handler = AddFileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path='./date_to_process', recursive=True)
    logging.getLogger(name='console').info("Monitoring started")
    observer.start()
    try:
        while True:
            pass

    except Exception as exception:
        logging.getLogger().error('file problem')
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
    finally:
        observer.stop()
        observer.join()
