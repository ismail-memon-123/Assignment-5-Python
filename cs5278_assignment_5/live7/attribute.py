from typing import TypeVar, Generic, Type

T = TypeVar("T")


class Attribute(Generic[T]):
    def __init__(self, name: str, attribute_type: type(T), value: [T]):
        self.name: str = name
        self.type: type(T) = attribute_type
        self.value: [T] = value

    def get_value(self) -> [T]:
        return self.value

    def get_name(self) -> str:
        return self.name

    def get_type(self) -> Type[T]:
        return self.type
