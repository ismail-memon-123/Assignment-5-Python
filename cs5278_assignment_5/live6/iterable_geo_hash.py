from abc import ABC, abstractmethod
from typing import Iterable, Self, List, Iterator

from cs5278_assignment_5.live6.geo_hash import GeoHash


class IterableGeoHash(ABC, Iterable[bool]):
    @abstractmethod
    def __init__(self) -> None:
        self.bits: List[bool] = None

    @abstractmethod
    def bits_of_precision(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def prefix(self, n: int) -> Self:
        """
        Similar to "substring" on Strings. This method should
        return the first n bits of the GeoHash as a new GeoHash.
        """

        raise NotImplementedError

    # Bonus Points: Implementation of this is not required, but is a nice challenge for bonus points.
    @abstractmethod
    def north_neighbor(self) -> Self:
        raise NotImplementedError

    # Bonus Points: Implementation of this is not required, but is a nice challenge for bonus points.
    @abstractmethod
    def south_neighbor(self) -> Self:
        raise NotImplementedError

    # Bonus Points: Implementation of this is not required, but is a nice challenge for bonus points.
    @abstractmethod
    def west_neighbor(self) -> Self:
        raise NotImplementedError

    # Bonus Points: Implementation of this is not required, but is a nice challenge for bonus points.
    @abstractmethod
    def east_neighbor(self) -> Self:
        raise NotImplementedError

class IterableGeoHashImplementation(IterableGeoHash):

    def __init__(self, bits: List[bool]):
        self.bits: List[bool] = bits if bits else []

    def bits_of_precision(self) -> int:
        return len(self.bits)

    def prefix(self, n: int) -> Self:
        """
        Similar to "substring" on Strings. This method should
        return the first n bits of the GeoHash as a new GeoHash.
        """
        return self.__class__(self.bits[:n])
    
    def __iter__(self) -> Iterator[bool]:
        return iter(self.bits)