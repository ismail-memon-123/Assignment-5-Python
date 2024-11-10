from typing import Collection, Dict

from cs5278_assignment_5.live7.attribute_matcher import AttributeMatcher
from cs5278_assignment_5.live7.proximity_stream_db import ProximityStreamDB
from cs5278_assignment_5.live7.example.building import Building
from cs5278_assignment_5.live7.example.building_attributes_strategy import BuildingAttributesStrategy
from cs5278_assignment_5.live6.data_and_position import DataAndPosition
from cs5278_assignment_5.live6.position import Position

if __name__ == "__main__":
    kirkland_hall: Building = Building("Kirkland Hall", 150000, 5)
    fgh: Building = Building("Featheringill Hall", 95023.4, 38)
    esb: Building = Building("Engineering Sciences Building", 218793.34, 10)

    # Create an instance of your ProximityStreamDB impl
    strmdb: ProximityStreamDB = None

    strmdb.insert(DataAndPosition.with_coordinates(36.145050, 86.803365, fgh))
    strmdb.insert(DataAndPosition.with_coordinates(36.148345, 86.802909, kirkland_hall))
    strmdb.insert(DataAndPosition.with_coordinates(36.143171, 86.805772, esb))

    buildingsNearFgh: Collection[DataAndPosition[Building]] = \
        strmdb.nearby(Position.with_coordinates(36.145050, 86.803365), 28)

    for buildingAndPos in buildingsNearFgh:
        print(f"{buildingAndPos.get_data().get_name()} is located at "
              f"{buildingAndPos.get_latitude()}, {buildingAndPos.get_longitude()}.")

    averageBuildingSqft: float = \
        float(strmdb.average_nearby(
            type("AttributeMatcherImplementation", (AttributeMatcher, object),
                 {"matches": lambda attr: BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET == attr.get_name()})(),
            Position.with_coordinates(36.145050, 86.803365), 28))

    print(f"The average building size near FGH Hall is: {averageBuildingSqft} sqft.")

    buildingSizeHistogram: Dict[object, int] = \
        strmdb.histogram_nearby(
            type("AttributeMatcherImplementation", (AttributeMatcher, object),
                 {"matches": lambda attr: BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET == attr.get_name()})(),
            Position.with_coordinates(36.145050, 86.803365), 16)

    for countForValue in buildingSizeHistogram.items():
        print(f"There are {countForValue[1]} buildings with {countForValue[0]} sqft nearby.")
