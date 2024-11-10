from abc import ABC, abstractmethod
from collections.abc import Collection
from typing import TypeVar, Generic, List
import math

from multimethod import multimethod

from cs5278_assignment_5.live6.data_and_position import DataAndPosition
from cs5278_assignment_5.live6.position import Position
from cs5278_assignment_5.live6.geo_db_factory import GeoDBFactory

T = TypeVar("T")


class ProximityDB(ABC, Generic[T]):
    """
    Create an implementation of the ProximityDB.

    The ProximityDB maps data items to coordinates and
    allows searching for nearby data items using geohashes.

    You are free to adapt your GeoDB implementation. However, please make
    sure that your implementation of ProximityDB is in this package so that
    it is easy for your peers and the instructor to find.

    There will be ambiguous requirements. Use your best judgement in determining
    an appropriate interpretation. Keep these ambiguities in mind when you code
    review others' solutions later in class.

    The type parameter T is the type of data stored in the database. For example,
    to map strings to coordinates, you would use something like the below code:
    ProximityDBImpl(lat, lon, "string"): ProximityDBImpl[str]

    See the example package for a sample application
    that stores Building objects in the database
    """

    @abstractmethod
    def insert(self, data: DataAndPosition[T]) -> None:
        """
        Inserts a data item into the database at the specified location.
        You SHOULD preserve duplicate data items.
        """

        raise NotImplementedError

    @abstractmethod
    @multimethod
    def delete(self, pos: Position) -> Collection[DataAndPosition[T]]:
        """
        Deletes the all data items at the specified
        location from the database.

        Returns the list of data items that were deleted.
        """

        raise NotImplementedError

    @abstractmethod
    @multimethod
    def delete(self, pos: Position, bits_of_precision: int) -> Collection[DataAndPosition[T]]:
        """
        Deletes all data items from the database that
        match the provided latitude and longitude
        up to the specified number of bits of precision
        in their geohashes.

        Returns the list of deleted data items.
        """

        raise NotImplementedError

    @abstractmethod
    def contains(self, pos: Position, bits_of_precision: int) -> bool:
        """
        Returns true if the database contains at least one data item that
        matches the provided latitude and longitude
        up to the specified number of bits of precision
        in its geohash.
        """

        raise NotImplementedError

    @abstractmethod
    def nearby(self, pos: Position, bits_of_precision: int) -> Collection[DataAndPosition[T]]:
        """
        Returns all data items in the database that
        match the provided latitude and longitude
        up to the specified number of bits of precision
        in their geohashes.
        """

        raise NotImplementedError

class ProximityDBImplementation(ProximityDB, Generic[T]):

    positionedMap: list[DataAndPosition[T]]

    def __init__(self, bits: int):
        self.positionedMap = []
        self.geo = GeoDBFactory.new_database(bits)


    def insert(self, data: DataAndPosition[T]) -> None:
        """
        Inserts a data item into the database at the specified location.
        You SHOULD preserve duplicate data items.
        BUT geo does not care bec it is not used for that only for the nearby stuff.
        """
        self.positionedMap.append(data)
        self.geo.insert(data.get_latitude(), data.get_longitude())
    

    @multimethod
    def delete(self, pos: Position) -> Collection[DataAndPosition[T]]:
        returnList: list[DataAndPosition[T]] = []
        lat = pos.get_latitude()
        long = pos.get_longitude()

        for i in range(len(self.positionedMap)):
            dp = self.positionedMap[i]
            if ((dp.get_latitude() == lat) and (dp.get_longitude() == long)):
                returnList.append(dp)
        for i in range(len(returnList)):
            self.positionedMap.remove(returnList[i])
        self.geo.delete(lat, long)
        return returnList

        """
        Deletes the all data items at the specified
        location from the database.

        Returns the list of data items that were deleted.
        """

    @multimethod
    def delete(self, pos: Position, bits_of_precision: int) -> Collection[DataAndPosition[T]]:
        """
        Deletes all data items from the database that
        match the provided latitude and longitude
        up to the specified number of bits of precision
        in their geohashes.

        Returns the list of deleted data items.

        Idea is we get each lat, long pair form the geo hash which is what it returns and feed it to the delete above
        """
        returnList: list[DataAndPosition[T]] = []
        lat = pos.get_latitude()
        long = pos.get_longitude()
        coordinate_list = self.geo.delete_all(lat, long, bits_of_precision)
        for i in range(len(coordinate_list)):
            # Even though we delete here the delete there is ok bec it will just not be able to find it which is fine
            # if not just make flag
            #self.delete(Position.with_coordinates(coordinate_list[i][0], coordinate_list[i][1]))
            lat = coordinate_list[i][0]
            long = coordinate_list[i][1]
            for j in range(len(self.positionedMap)):
                dp = self.positionedMap[j]
                if ((dp.get_latitude() == lat) and (dp.get_longitude() == long)):
                    returnList.append(dp)
        for i in range(len(returnList)):
            self.positionedMap.remove(returnList[i])
        return returnList


    def contains(self, pos: Position, bits_of_precision: int) -> bool:
        """
        Returns true if the database contains at least one data item that
        matches the provided latitude and longitude
        up to the specified number of bits of precision
        in its geohash.
        """
        lat = pos.get_latitude()
        long = pos.get_longitude()
        return self.geo.contains(lat, long, bits_of_precision)


    def nearby(self, pos: Position, bits_of_precision: int) -> Collection[DataAndPosition[T]]:
        """
        Returns all data items in the database that
        match the provided latitude and longitude
        up to the specified number of bits of precision
        in their geohashes.
        """
        lat = pos.get_latitude()
        long = pos.get_longitude()
        coordinate_list = self.geo.nearby(lat, long, bits_of_precision)
        returnList: list[DataAndPosition[T]] = []
        for i in range(len(coordinate_list)):
            print("Expcetd: " + str(coordinate_list[i][0]) + "," + str(coordinate_list[i][1]))
            # Even though we delete here the delete there is ok bec it will just not be able to find it which is fine
            # if not just make flag
            #self.delete(Position.with_coordinates(coordinate_list[i][0], coordinate_list[i][1]))
            for j in range(len(self.positionedMap)):
                dp = self.positionedMap[j]
                print("" + str(dp.get_latitude()) + "," + str(dp.get_longitude()))
                if (math.isclose(dp.get_latitude(), coordinate_list[i][0]) and math.isclose(dp.get_longitude(), coordinate_list[i][1])):
                    returnList.append(dp)
        return returnList
