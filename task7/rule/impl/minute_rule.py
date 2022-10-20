import logging
import sys

from rule.custom_rule import CustomRule

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)


class MinuteRule(CustomRule):
    """
    Implication of CustomRule
    rule: error by minute
    """

    def __init__(self) -> None:
        super().__init__()

    def process_rule(self, row):
        """
        add to dict
            key: time,
            value: error counter
        time is cut till minutes
        :param row: row from dataframe
        :return:none
        """
        if row['date'] in self._error_dict:
            self._error_dict[row['date']] += 1
        else:
            self._error_dict[row['date']] = 0

    def analyse_errors(self):
        """
        log errors from dict

        :return: none
        """
        logger = logging.getLogger(name='console')
        for k, v in self._error_dict.items():
            if v > 10:
                logger.info(f'for minute: {k} errors: {v}')
