from typing import Self


class Building:
    def __init__(self, name: str, sqft: float, classrooms: float):
        self.name: str = name
        self.sizeInSquareFeet: float = sqft
        self.classRooms: float = classrooms

    def get_name(self) -> str:
        return self.name

    def set_name(self, name: str) -> None:
        self.name = name

    def get_size_in_square_feet(self) -> float:
        return self.sizeInSquareFeet

    def set_size_in_square_feet(self, size_in_square_feet: float) -> None:
        self.sizeInSquareFeet = size_in_square_feet

    def get_classrooms(self) -> float:
        return self.classRooms

    def set_classrooms(self, classrooms: float) -> None:
        self.classRooms = classrooms

    def equals(self, o: type(Self)) -> bool:
        if self == o:
            return True

        if o is None or not isinstance(o, type(self)):
            return False

        return self.name == o.name

    def hash_code(self) -> int:
        return hash(self.name)
