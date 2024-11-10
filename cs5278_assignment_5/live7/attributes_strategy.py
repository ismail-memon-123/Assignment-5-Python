from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Collection

from cs5278_assignment_5.live7.attribute import Attribute

T = TypeVar("T")


class AttributesStrategy(ABC, Generic[T]):
    @staticmethod
    @abstractmethod
    def get_attributes(data: [T]) -> Collection[Attribute]:
        """
        Given a data item of type T, returns
        the attributes of this piece of data.

        An attribute can be thought of as a key/value pair that is
        associated with the data. Each attribute has a name, type, and value.

        Another way of thinking of attribute is that they represent
        "columns" and the data items are "rows" in a database.
        Since the type of the data items in our database can vary,
        we need a helper class (e.g., strategy) to extract the
        columns present in a data item.
        """
        raise NotImplementedError
        
