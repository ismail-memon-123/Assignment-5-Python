class GeoHash:
    """
    This live session will focus on basic Python and some concepts important to
    functional programming, such as recursion.

    This class uses a main() method where we can write our own simple "experiments" to
    test how our code works. You are encouraged to modify the main method to play around
    with your code and understand it. When you have working code, you can extract it
    into a method. When you have working examples with assertions, you can extract them
    into tests.

    I have left some sample experiments in main() to help you understand the geohash
    algorithm.

    This class will provide an implementation of GeoHashes:

    https://www.mapzen.com/blog/geohashes-and-you/
    https://en.wikipedia.org/wiki/Geohash

    GeoHash Spatial Precision:

    https://releases.dataone.org/online/api-documentation-v2.0.1/design/geohash.html
    """

    LATITUDE_RANGE = [-90, 90]
    LONGITUDE_RANGE = [-180, 180]

    @staticmethod

    def geo_hash_helper(value, minimum, maximum):
        median = (minimum + maximum) / 2
        if value >= median:
            return median, maximum, True
        return minimum, median, False

    @staticmethod
    def geo_hash_1d(value_to_hash: float, value_range: list[float], bits_of_precision: int) -> list[bool]:
        current_range = [value_range[0], value_range[1]]
        result = []
        for i in range(bits_of_precision):
            current_range[0], current_range[1], toAppend = GeoHash.geo_hash_helper(value_to_hash, current_range[0], current_range[1])
            result.append(toAppend)
        return result

    @staticmethod
    def geo_hash_2d(v1: float, v1_range: list[float], v2: float,
                    v2_range: list[float], bits_of_precision: int) -> list[bool]:
        result = []
        # Need a 1d of long and 1d of lat. Even is long and odd is lat.
        bits_latitude = []
        bits_longitude = []
        num_bits_latitude = int(bits_of_precision / 2)
        num_bits_longitude = int(bits_of_precision / 2)
        # If bits of precision is 11, then last index is 10 which means longitude has one more bit.
        if (bits_of_precision % 2 == 1):
            num_bits_longitude = num_bits_longitude + 1
        bits_latitude = GeoHash.geo_hash_1d(v2, v2_range, num_bits_latitude)
        bits_longitude = GeoHash.geo_hash_1d(v1, v1_range, num_bits_longitude)
        # Now interweave them.
        for i in range(num_bits_latitude):
            result.append(bits_longitude[i])
            result.append(bits_latitude[i])
        if (bits_of_precision % 2 != 0):
            result.append(bits_longitude[num_bits_longitude - 1])
        return result

    @staticmethod
    def geo_hash(lat: float, lon: float, bits_of_precision: int) -> list[bool]:
        return GeoHash.geo_hash_2d(lat, GeoHash.LATITUDE_RANGE, lon, GeoHash.LONGITUDE_RANGE, bits_of_precision)

    # This is a helper method that will make printing out geohashes easier
    @staticmethod
    def to_hash_string(geohash: list[bool]) -> str:
        hash_string = ""
        for b in geohash:
            hash_string += "1" if b else "0"

        return hash_string

    # This is a convenience method to make it easy to get a string of 1s and 0s for a geohash
    @staticmethod
    def geo_hash_string(value_to_hash: float, value_range: list[float], bits_of_precision: int) -> str:
        return GeoHash.to_hash_string(GeoHash.geo_hash_1d(value_to_hash, value_range, bits_of_precision))

    # Faux testing for now
    @staticmethod
    def assert_equals(v1: str, v2: str) -> None:
        assert v1 == v2

    @staticmethod
    def main() -> None:
        # Example of hand-coding a 3-bit geohash

        # 1st bit of the geohash
        longitude = 0.0
        bounds = [GeoHash.LONGITUDE_RANGE[0], GeoHash.LONGITUDE_RANGE[1]]
        midpoint = (bounds[0] + bounds[1]) / 2

        if longitude >= midpoint:
            bit = True
            bounds[0] = midpoint
        else:
            bit = False
            bounds[1] = midpoint

        # 2nd bit of the geohash
        midpoint = (bounds[0] + bounds[1]) / 2
        if longitude >= midpoint:
            bit2 = True
            bounds[0] = midpoint
        else:
            bit2 = False
            bounds[1] = midpoint

        # 3rd bit of the geohash
        midpoint = (bounds[0] + bounds[1]) / 2
        if longitude >= midpoint:
            bit3 = True
            bounds[0] = midpoint
        else:
            bit3 = False
            bounds[1] = midpoint
        # Continue this process for however many bits of precision we need...

        # Faux testing for now
        GeoHash.assert_equals("100", GeoHash.to_hash_string([bit, bit2, bit3]))

        # If you can get the 1D geohash to pass all of these faux tests, you should be in
        # good shape to complete the 2D version.
        GeoHash.assert_equals("00000", GeoHash.geo_hash_string(GeoHash.LONGITUDE_RANGE[0], GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("00000", GeoHash.geo_hash_string(GeoHash.LATITUDE_RANGE[0], GeoHash.LATITUDE_RANGE, 5))
        GeoHash.assert_equals("11111", GeoHash.geo_hash_string(GeoHash.LONGITUDE_RANGE[1], GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("11111", GeoHash.geo_hash_string(GeoHash.LATITUDE_RANGE[1], GeoHash.LATITUDE_RANGE, 5))
        GeoHash.assert_equals("10000", GeoHash.geo_hash_string(0, GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("11000", GeoHash.geo_hash_string(90.0, GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("11100", GeoHash.geo_hash_string(135.0, GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("11110", GeoHash.geo_hash_string(157.5, GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("11111", GeoHash.geo_hash_string(168.75, GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("01111", GeoHash.geo_hash_string(-1, GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("00111", GeoHash.geo_hash_string(-91.0, GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("00011", GeoHash.geo_hash_string(-136.0, GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("00001", GeoHash.geo_hash_string(-158.5, GeoHash.LONGITUDE_RANGE, 5))
        GeoHash.assert_equals("00000", GeoHash.geo_hash_string(-169.75, GeoHash.LONGITUDE_RANGE, 5))


if __name__ == "__main__":
    GeoHash().main()