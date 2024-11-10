import math
import random
import uuid
from typing import Generic, Dict, List, Set, Iterator, Any, TypeVar

from pyxtension.streams import stream

from cs5278_assignment_5.live6.data_and_position import DataAndPosition
from cs5278_assignment_5.live6.iterable_geo_hash import IterableGeoHash
from cs5278_assignment_5.live6.iterable_geo_hash_factory import IterableGeoHashFactory
from cs5278_assignment_5.live6.position import Position
from cs5278_assignment_5.live7.attribute_matcher import AttributeMatcher
from cs5278_assignment_5.live7.attributes_strategy import AttributesStrategy
from cs5278_assignment_5.live7.example.building import Building
from cs5278_assignment_5.live7.example.building_attributes_strategy import BuildingAttributesStrategy
from cs5278_assignment_5.live7.example.map_attributes_strategy import MapAttributesStrategy
from cs5278_assignment_5.live7.proximity_stream_db import ProximityStreamDB
from cs5278_assignment_5.live7.proximity_stream_db_factory import ProximityStreamDBFactory

T = TypeVar("T")


class TestProximityStreamDB(Generic[T]):
    class FakeIterableGeoHash(IterableGeoHash):
        def __init__(self, bits: List[bool]):
            self.bits: List[bool] = bits if bits else []

        def bits_of_precision(self) -> int:
            return len(self.bits)

        def prefix(self, n) -> IterableGeoHash:
            return TestProximityStreamDB.FakeIterableGeoHash(self.bits[:n])

        def north_neighbor(self) -> IterableGeoHash:
            raise NotImplementedError

        def south_neighbor(self) -> IterableGeoHash:
            raise NotImplementedError

        def west_neighbor(self) -> IterableGeoHash:
            raise NotImplementedError

        def east_neighbor(self) -> IterableGeoHash:
            raise NotImplementedError

        def __iter__(self) -> Iterator[bool]:
            return iter(self.bits)

    @staticmethod
    def new_db(strat: AttributesStrategy[T], hash_dict: Dict[Position,
               List[bool]], bits: int) -> ProximityStreamDB[type(T)]:
        def hash_dict_filter(lat, lon):
            matching_entries = [hash_dict[pos] for pos in hash_dict if
                                pos.get_latitude() == lat and pos.get_longitude() == lon]

            if not len(matching_entries):
                raise KeyError

            assert len(matching_entries) == 1, "Found more than one matching entry in " \
                                               "dictionary! This should not be possible."

            return matching_entries[0]

        return ProximityStreamDBFactory.create(
            strat,
            bits,
            type("IterableGeoHashFactoryImplementation", (IterableGeoHashFactory, object),
                 {"with_coordinates": staticmethod(
                     lambda lat, lon, bs: TestProximityStreamDB.FakeIterableGeoHash(
                         hash_dict_filter(lat, lon)).prefix(bs)
                 )})()
        )

    @staticmethod
    def random_geo_hash(bits: int) -> List[bool]:
        return random.choices([True, False], k=bits)

    @staticmethod
    def random_position() -> Position:
        lat: float = random.uniform(-90, 90)  # generate a random lat between -90 / 90
        lon: float = random.uniform(-180, 180)  # generate a random lon between -180 / 180

        return Position.with_coordinates(lat, lon)

    # This method randomly generates a set of unique Positions and maps them to a specified
    # number of geohashes. The geohashes are completely random and the mapping is random.

    # For example, randomCoordinateHashMappings(16, 100, 12) would generate 100
    # unique Positions and map them to 12 random geohashes of 16 bits each.
    @staticmethod
    def random_coordinate_hash_mappings(bits: int, total: int, groups: int,
                                        shared_prefix_length: int) -> Dict[Position, List[bool]]:
        avg: int = total // groups  # If it doesn't divide evenly, there is a remainder discarded

        mappings: Dict[Position, List[bool]] = {}
        positions: Set[Position] = set()
        prefixes: Set[str] = set()

        # We generate random unique geohash prefixes of length `sharedPrefixLength`
        # so that we can synthesize groups of positions that will match up to a
        # certain number of bits.
        for i in range(groups):
            prefix: List[bool] = TestProximityStreamDB.random_geo_hash(shared_prefix_length)
            while TestProximityStreamDB.to_string(prefix) in prefixes:
                prefix = TestProximityStreamDB.random_geo_hash(shared_prefix_length)

            prefixes.add(TestProximityStreamDB.to_string(prefix))

            # Create `avg` Position objects that are
            # unique and map each one to the random hash.
            prefix_suffixes = set()
            for j in range(avg):
                full_hash: List[bool] = prefix.copy()

                suffix = TestProximityStreamDB.random_geo_hash(bits - shared_prefix_length)
                while TestProximityStreamDB.to_string(suffix) in prefix_suffixes:
                    suffix = TestProximityStreamDB.random_geo_hash(bits - shared_prefix_length)

                pos: Position = TestProximityStreamDB.random_position()
                while pos in positions:
                    pos = TestProximityStreamDB.random_position()

                positions.add(pos)

                mappings[pos] = full_hash

        return mappings

    @staticmethod
    def to_string(data: List[bool]) -> str:
        hash_string: str = ""

        for b in data:
            hash_string += "1" if b else "0"

        return hash_string

    # This is the most comprehensive, but also the most difficult to
    # understand test. I would save this test for last if you are
    # trying to incrementally pass the test methods.
    @staticmethod
    def test_stream_attributes_random() -> None:
        # This test randomly generates a set of positions that are artificially
        # assigned to geohashes. The geohashes are constructed such that the
        # positions are guaranteed to fall into N groups that match K bits of
        # their geohashes. The test checks that all data items in a given group
        # are correctly streamed when any position that is mapped to that group
        # is provided as the nearby search position. The nearby searches are
        # done with K bits to guarantee that all items in the group will have
        # matching geohashes.

        # A synthetic example:

        # groups = 3
        # shared_prefix_length = 2
        # bits = 4
        # buildings = 6

        # random_mappings = {
        #      [(-88.01, 0) 1111]
        #      [(-48.01, 90) 1101]
        #      [(-88.01, 20) 1000]
        #      [(20.01, 0) 1001]
        #      [(118.01, -10) 0110]
        #      [(88.01, 10) 0101]
        # }

        # There are three unique prefixes of length 2.
        # [11, 10, 01]

        # Every position has been mapped to a random
        # geohash that starts with one of these prefixes.

        # For any given prefix, we know in
        # advance what positions will map to it.

        # For each position, we can check that all other locations with
        # a matching prefix are returned when we do a nearby search on
        # that position.

        # Note: the hashes are completely random and unrelated to the
        # actual positions on the earth -- it shouldn't matter to your
        # implementation how the position to geohash translation is done,
        # as long as it is consistent

        max_groups: int = 128
        max_bits: int = 256

        groups: int = random.randint(1, max_groups)
        buildings: int = random.randint(groups, 28 * groups)

        # We have to ensure that we have
        # enough bits in the shared prefix
        # to differentiate all the groups and
        # not take forever to randomly generate
        # the unique shared prefixes
        shared_prefix_length: int = \
            random.randint(math.ceil(math.log2(groups)), max_bits - math.ceil(math.log2(buildings // groups)))
        bits: int = \
            shared_prefix_length + \
            random.randint(math.ceil(math.log2(buildings // groups)), max_bits - shared_prefix_length)

        # For debugging.
        # print(f"Testing {buildings} items with {bits} bit hashes and "
        #       f"{shared_prefix_length} shared bits in {groups} groups")

        random_mappings: Dict[Position, List[bool]] = \
            TestProximityStreamDB.random_coordinate_hash_mappings(bits, buildings, groups, shared_prefix_length)
        db: ProximityStreamDB[Building] = \
            TestProximityStreamDB.new_db(BuildingAttributesStrategy(), random_mappings, bits)

        hash_to_building_name: Dict[str, Set[str]] = {}
        hash_to_sqft: Dict[str, List[float]] = {}
        hash_to_classrooms: Dict[str, List[float]] = {}

        for entry in random_mappings.items():
            pos: Position = entry[0]
            hashstr: str = TestProximityStreamDB.to_string(entry[1])[:shared_prefix_length]
            b: Building = Building(str(uuid.uuid4()), random.random() * 100000, round(random.random() * 25))
            db.insert(DataAndPosition.with_coordinates(pos.get_latitude(), pos.get_longitude(), b))

            existing: Set[str] = hash_to_building_name.get(hashstr, set())
            existing.add(b.get_name())
            hash_to_building_name[hashstr] = existing

            curr: List[float] = hash_to_sqft.get(hashstr, [])
            curr.append(b.get_size_in_square_feet())
            hash_to_sqft[hashstr] = curr

            rooms: List[float] = hash_to_classrooms.get(hashstr, [])
            rooms.append(b.get_classrooms())
            hash_to_classrooms[hashstr] = rooms

        for entry in random_mappings.items():
            pos: Position = entry[0]
            hashstr: str = TestProximityStreamDB.to_string(entry[1])[:shared_prefix_length]
            expected: Set[str] = hash_to_building_name.get(hashstr, {})
            actual: Set[str] = \
                db.stream_nearby(type("AttributeMatcherImplementation", (AttributeMatcher, object), {
                    "matches": staticmethod(lambda a: a.get_name() == BuildingAttributesStrategy.NAME)})(), pos,
                                 shared_prefix_length).map(lambda v: str(v)).toSet()

            assert expected == actual

        for entry in random_mappings.items():
            pos: Position = entry[0]
            hashstr: str = TestProximityStreamDB.to_string(entry[1])[:shared_prefix_length]

            expected_sqft: List[float] = hash_to_sqft.get(hashstr)

            expected_avg: float = float(stream(expected_sqft).map(lambda v: float(v)).mean())

            actual_avg: float = float(db.average_nearby(
                type("AttributeMatcherImplementation", (AttributeMatcher, object), {
                    "matches": staticmethod(
                        lambda a: a.get_name() == BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET)})(),
                pos, shared_prefix_length))

            assert math.isclose(expected_avg, actual_avg, abs_tol=0.1)

            expected_max: float = float(stream(expected_sqft).map(lambda v: float(v)).max())
            actual_max: float = float(db.max_nearby(
                type("AttributeMatcherImplementation", (AttributeMatcher, object), {
                    "matches": staticmethod(
                        lambda a: a.get_name() == BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET)})(),
                pos, shared_prefix_length))

            assert math.isclose(expected_max, actual_max, abs_tol=0.1)

            expected_min: float = float(stream(expected_sqft).map(lambda v: float(v)).min())
            actual_min: float = float(db.min_nearby(
                type("AttributeMatcherImplementation", (AttributeMatcher, object), {
                    "matches": staticmethod(
                        lambda a: a.get_name() == BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET)})(),
                pos, shared_prefix_length))

            assert math.isclose(expected_min, actual_min, abs_tol=0.1)

        for entry in random_mappings.items():
            pos: Position = entry[0]
            hashstr: str = TestProximityStreamDB.to_string(entry[1])[:shared_prefix_length]

            expected_classrooms: List[float] = hash_to_classrooms.get(hashstr)

            expected_avg: float = float(stream(expected_classrooms).map(lambda v: float(v)).mean())
            type("AttributeMatcherImplementation", (AttributeMatcher, object),
                 {"matches": staticmethod(
                     lambda a: a.get_name() == BuildingAttributesStrategy.CLASSROOMS)})()
            actual_avg: float = float(db.average_nearby(
                type("AttributeMatcherImplementation", (AttributeMatcher, object),
                     {"matches": staticmethod(
                         lambda a: a.get_name() == BuildingAttributesStrategy.CLASSROOMS)})(), pos,
                shared_prefix_length))

            assert math.isclose(expected_avg, actual_avg, abs_tol=0.1)

            expected_max: float = float(stream(expected_classrooms).map(lambda v: float(v)).max())
            actual_max: float = float(db.max_nearby(
                type("AttributeMatcherImplementation", (AttributeMatcher, object),
                     {"matches": staticmethod(
                         lambda a: a.get_name() == BuildingAttributesStrategy.CLASSROOMS)})(), pos,
                shared_prefix_length))

            assert math.isclose(expected_max, actual_max, abs_tol=0.1)

            expected_min: float = float(stream(expected_classrooms).map(lambda v: float(v)).min())
            actual_min: float = float(db.min_nearby(
                type("AttributeMatcherImplementation", (AttributeMatcher, object),
                     {"matches": staticmethod(lambda a: a.get_name() == BuildingAttributesStrategy.CLASSROOMS)})(), pos,
                shared_prefix_length))

            assert math.isclose(expected_min, actual_min, abs_tol=0.1)

            hist: Dict[float, int] = db.histogram_nearby(
                type("AttributeMatcherImplementation", (AttributeMatcher, object),
                     {"matches": staticmethod(lambda a: a.get_name() == BuildingAttributesStrategy.CLASSROOMS)})(), pos,
                shared_prefix_length)

            for bucket in hist.items():
                assert stream(expected_classrooms).filter(lambda v: v == bucket[0]).size() == bucket[1]

    @staticmethod
    def test_stream_nearby():
        mapping: Dict[Position, List[bool]] = {Position.with_coordinates(36.145050, 86.803365): [True, True, True],
                                               Position.with_coordinates(36.148345, 86.802909): [True, True, False],
                                               Position.with_coordinates(36.143171, 86.805772): [True, False, False]}

        strmdb: ProximityStreamDB[Building] = \
            TestProximityStreamDB.new_db(BuildingAttributesStrategy(), mapping, 3)

        kirkland_hall: Building = Building("Kirkland Hall", 150000, 5)
        fgh: Building = Building("Featheringill Hall", 95023.4, 38)
        esb: Building = Building("Engineering Sciences Building", 218793.34, 10)

        strmdb.insert(DataAndPosition.with_coordinates(36.145050, 86.803365, fgh))
        strmdb.insert(DataAndPosition.with_coordinates(36.148345, 86.802909, kirkland_hall))
        strmdb.insert(DataAndPosition.with_coordinates(36.143171, 86.805772, esb))

        buildings_near_fgh: Set[Building] = stream(strmdb.nearby(Position.with_coordinates(
            36.145050, 86.803365), 2)).map(lambda dpos: dpos.get_data()).toSet()

        assert 2 == len(buildings_near_fgh)
        assert fgh in buildings_near_fgh
        assert kirkland_hall in buildings_near_fgh
        assert esb not in buildings_near_fgh

    @staticmethod
    def test_average_buildings_nearby():
        mapping: Dict[Position, List[bool]] = {Position.with_coordinates(36.145050, 86.803365): [True, True, True],
                                               Position.with_coordinates(36.148345, 86.802909): [True, True, False],
                                               Position.with_coordinates(36.143171, 86.805772): [True, False, False]}

        strmdb: ProximityStreamDB[Building] = \
            TestProximityStreamDB.new_db(BuildingAttributesStrategy(), mapping, 3)

        kirkland_hall: Building = Building("Kirkland Hall", 150000, 5)
        fgh: Building = Building("Featheringill Hall", 95023.4, 38)
        esb: Building = Building("Engineering Sciences Building", 218793.34, 10)

        strmdb.insert(DataAndPosition.with_coordinates(36.145050, 86.803365, fgh))
        strmdb.insert(DataAndPosition.with_coordinates(36.148345, 86.802909, kirkland_hall))
        strmdb.insert(DataAndPosition.with_coordinates(36.143171, 86.805772, esb))

        average_building_sqft: float = float(strmdb.average_nearby(
            type("AttributeMatcherImplementation", (AttributeMatcher, object),
                 {"matches": staticmethod(lambda a: a.get_name() == BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET)})(),
            Position.with_coordinates(36.145050, 86.803365), 2))

        assert math.isclose(122511.7, average_building_sqft, abs_tol=0.1)

    @staticmethod
    def test_histogram_buildings_nearby():
        mapping: Dict[Position, List[bool]] = {Position.with_coordinates(36.145050, 86.803365): [True, True, True],
                                               Position.with_coordinates(36.148345, 86.802909): [True, True, False],
                                               Position.with_coordinates(36.143171, 86.805772): [True, False, False]}

        strmdb: ProximityStreamDB[Building] = \
            TestProximityStreamDB.new_db(BuildingAttributesStrategy(), mapping, 3)

        kirkland_hall: Building = Building("Kirkland Hall", 150000, 5)
        fgh: Building = Building("Featheringill Hall", 95023.4, 38)
        esb: Building = Building("Engineering Sciences Building", 218793.34, 10)

        strmdb.insert(DataAndPosition.with_coordinates(36.145050, 86.803365, fgh))
        strmdb.insert(DataAndPosition.with_coordinates(36.148345, 86.802909, kirkland_hall))
        strmdb.insert(DataAndPosition.with_coordinates(36.143171, 86.805772, esb))

        building_size_histogram: Dict[object, int] = strmdb.histogram_nearby(
            type("AttributeMatcherImplementation", (AttributeMatcher, object),
                 {"matches": staticmethod(lambda a: a.get_name() == BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET)})(),
            Position.with_coordinates(36.145050, 86.803365), 1)

        assert 1 == building_size_histogram.get(kirkland_hall.get_size_in_square_feet())
        assert 1 == building_size_histogram.get(esb.get_size_in_square_feet())
        assert 1 == building_size_histogram.get(fgh.get_size_in_square_feet())

    @staticmethod
    def test_history():
        groups: int = 32
        shared_prefix_length: int = 8
        bits: int = 16
        buildings: int = 32

        random_mappings: Dict[Position, List[bool]] = \
            TestProximityStreamDB.random_coordinate_hash_mappings(bits, buildings, groups, shared_prefix_length)

        strmdb: ProximityStreamDB[Dict[str, Any]] = \
            TestProximityStreamDB.new_db(MapAttributesStrategy(), random_mappings, bits)

        data: List[Dict[str, float]] = []
        positions: List[Position] = []

        for p in random_mappings.keys():
            random_map: Dict[str, float] = {}
            strmdb.insert(DataAndPosition.with_coordinates(p.get_latitude(), p.get_longitude(), random_map))
            data.append(random_map)
            positions.append(p)

        for p in positions:
            strmdb.delete(p, bits)

        for i in range(len(positions)):
            snapshot: ProximityStreamDB[Dict[str, Any]] = strmdb.database_state_at_time(i + 1)

            assert snapshot.contains(positions[i], bits)

            for j in range(i + 1, len(positions), -1):
                assert not snapshot.contains(positions[j], bits)

            for j in range(i - 1, 0, -1):
                assert snapshot.contains(positions[j], bits)

        for i in range(len(positions)):
            snapshot: ProximityStreamDB[Dict[str, Any]] = strmdb.database_state_at_time(i + len(positions) + 1)

            assert not snapshot.contains(positions[i], bits)

            for j in range(i + 1, len(positions), -1):
                assert snapshot.contains(positions[j], bits)

            for j in range(i - 1, 0, -1):
                assert not snapshot.contains(positions[j], bits)