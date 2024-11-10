from abc import abstractmethod
from typing import TypeVar, Generic

from cs5278_assignment_5.live6.position import Position

T = TypeVar("T")


class DataAndPosition(Position, Generic[T]):
    """
    DataAndPosition binds a latitude / longitude location
    to a specific piece of data, such as a Building object,
    str, RealEstateListing object, dict, etc.

    positionedMap: DataAndPosition[Dict[str, str]] = DataAndPosition.with_coordinates(0, 0, {})

    class Foo():
        pass

    positionedFoo: DataAndPosition[Foo] = DataAndPosition.with_coordinates(1, 1, Foo())
    """

    @staticmethod
    def with_coordinates(lat: float, lon: float, data: [T] = None):
        if data:
            class DataAndPositionInstance(DataAndPosition):
                @staticmethod
                def get_latitude() -> float:
                    return lat

                @staticmethod
                def get_longitude() -> float:
                    return lon

                @staticmethod
                def get_data() -> [T]:
                    return data

            return DataAndPositionInstance()
        else:
            return super(DataAndPosition, DataAndPosition).with_coordinates(lat, lon)

    @staticmethod
    @abstractmethod
    def get_data() -> [T]:
        raise NotImplementedError
