from typing import Collection

from cs5278_assignment_5.live7.attribute import Attribute
from cs5278_assignment_5.live7.attributes_strategy import AttributesStrategy
from cs5278_assignment_5.live7.example.building import Building


class BuildingAttributesStrategy(AttributesStrategy[Building]):
    SIZE_IN_SQUARE_FEET: str = "sizeInSquareFeet"
    CLASSROOMS: str = "classrooms"
    NAME: str = "name"

    @staticmethod
    def get_attributes(data: Building) -> Collection[Attribute]:
        sqft: Attribute = \
            Attribute(BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET, float, data.get_size_in_square_feet())
        classrooms: Attribute = \
            Attribute(BuildingAttributesStrategy.CLASSROOMS, float, data.get_classrooms())
        name: Attribute = \
            Attribute(BuildingAttributesStrategy.NAME, str, data.get_name())

        return [sqft, classrooms, name]
