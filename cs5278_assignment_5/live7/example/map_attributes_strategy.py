from typing import Any, Dict, Collection

from cs5278_assignment_5.live7.attribute import Attribute
from cs5278_assignment_5.live7.attributes_strategy import AttributesStrategy


class MapAttributesStrategy(AttributesStrategy[Dict[str, Any]]):
    """
    This is a sample strategy that converts all the entries in a map to attributes.
    """

    @staticmethod
    def get_attributes(data: Dict[str, Any]) -> Collection[Attribute]:
        return [Attribute(item[0], type(item[1]), item[1]) for item in data.items()]
