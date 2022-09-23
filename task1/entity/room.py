class Room:
    def __init__(self, id_, name_):
        self.__id = id_
        self.__name = name_

    @property
    def id(self):
        return self.__id

    @id.setter
    def id(self, id_):
        self.__id = id_

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, name_):
        self.__name = name_


