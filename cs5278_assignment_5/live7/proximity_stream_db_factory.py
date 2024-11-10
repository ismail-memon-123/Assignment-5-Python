from typing import TypeVar, Generic

from cs5278_assignment_5.live7.attributes_strategy import AttributesStrategy
from cs5278_assignment_5.live7.proximity_stream_db import ProximityStreamDB
from cs5278_assignment_5.live7.proximity_stream_db import ProximityStreamDBImplementation
from cs5278_assignment_5.live6.iterable_geo_hash_factory import IterableGeoHashFactory

T = TypeVar("T")


class ProximityStreamDBFactory(Generic[T]):
    @staticmethod
    def create(strat: AttributesStrategy[T], bits: int,
               hash_factory: IterableGeoHashFactory) -> ProximityStreamDB[T]:
        """
        

        Fill in with your ProximityStreamDB implementation.
        """

        return ProximityStreamDBImplementation(strat, bits, hash_factory)
