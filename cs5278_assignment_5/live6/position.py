from abc import ABC, abstractmethod


class Position(ABC):
    @staticmethod
    def with_coordinates(lat: float, lon: float):
        class PositionInstance(Position):
            @staticmethod
            def get_latitude() -> float:
                return lat

            @staticmethod
            def get_longitude() -> float:
                return lon

        return PositionInstance()

    @staticmethod
    @abstractmethod
    def get_latitude() -> float:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def get_longitude() -> float:
        raise NotImplementedError
