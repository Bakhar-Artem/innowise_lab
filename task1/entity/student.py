from json import JSONEncoder


class Student:
    def __init__(self, birthday_, id_, name_, room_, sex_):
        self.__id = id_
        self.__name = name_
        self.__birthday = birthday_
        self.__room = room_
        self.__sex = sex_

    @property
    def id(self):
        return self.__id

    @id.setter
    def id(self, id_):
        self.__id = id_

    @id.deleter
    def id(self):
        del id

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, name_):
        self.__name = name_

    @property
    def birthday(self):
        return self.__birthday

    @birthday.setter
    def birthday(self, birthday_):
        self.__birthday = birthday_

    @property
    def room(self):
        return self.__room

    @room.setter
    def room(self, room_):
        self.__room = room_

    @property
    def sex(self):
        return self.__sex

    @sex.setter
    def sex(self, sex_):
        self.__sex = sex_


class StudentJSONEncoder(JSONEncoder):
    def default(self, obj):
        return obj.__dict__
