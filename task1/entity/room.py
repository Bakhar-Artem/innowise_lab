from json import JSONEncoder


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


class RoomJSONEncoder(JSONEncoder):
    def default(self, obj):
        return obj.__dict__
