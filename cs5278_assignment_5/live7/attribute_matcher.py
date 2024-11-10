from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from cs5278_assignment_5.live7.attribute import Attribute

T = TypeVar("T")


class AttributeMatcher(ABC, Generic[T]):
    @staticmethod
    @abstractmethod
    def matches(attr: Attribute[T]) -> bool:
        raise NotImplementedError
