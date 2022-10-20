from abc import abstractmethod


class CustomRule:
    """
    Base class for processing and analyse errors by rule
    """

    def __init__(self) -> None:
        self._error_dict = dict()

    @abstractmethod
    def process_rule(self, row):
        """
        abstract method to process each row from dataframe
        :param row: row from dataframe
        :return: none
        """
        pass

    @abstractmethod
    def analyse_errors(self):
        """
        abstract method to analyse error dict and log errors
        :return: none
        """
        pass
