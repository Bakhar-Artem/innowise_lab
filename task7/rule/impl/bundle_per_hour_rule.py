import logging
import sys

import pandas as pd

from rule.custom_rule import CustomRule


class BundlePerHourRule(CustomRule):
    """
    Implication of CustomRule
    rule: error by (bundle_id,hour)
    """

    def __init__(self) -> None:
        super().__init__()

    def process_rule(self, row):
        """
        add to dict
            key: (bundle_id,time),
            value: error counter
        time is cut till hours
        :param row: row from dataframe
        :return:none
        """
        if (row['bundle_id'], pd.to_datetime(row['date']).strftime('%Y-%m-%d %H')) in self._error_dict:
            self._error_dict[(row['bundle_id'], pd.to_datetime(row['date']).strftime('%Y-%m-%d %H'))] += 1
        else:
            self._error_dict[(row['bundle_id'], pd.to_datetime(row['date']).strftime('%Y-%m-%d %H'))] = 0

    def analyse_errors(self):
        """
        log errors from dict

        :return: none
        """
        logger = logging.getLogger(name='console')
        for k, v in self._error_dict.items():
            if v > 10:
                logger.info(f'for bundle_id: {k} errors: {v}')
