from abc import ABC, abstractmethod

from cs5278_assignment_5.live6.iterable_geo_hash import IterableGeoHash
from cs5278_assignment_5.live6.iterable_geo_hash import IterableGeoHashImplementation
from cs5278_assignment_5.live6.geo_hash import GeoHash


class IterableGeoHashFactory(ABC):
    """
    @TODO

    You will need to create an implementation of this
    interface that knows how to create your new GeoHash objects.
    """

    @staticmethod
    @abstractmethod
    def with_coordinates(lat: float, lon: float, bits_of_precision: int) -> IterableGeoHash:
        # first calculate geo hash and then put in this
        raise NotImplementedError

class IterableGeoHashConcreteFactory(IterableGeoHashFactory):
    def create() -> IterableGeoHash:
        return IterableGeoHashImplementation()
