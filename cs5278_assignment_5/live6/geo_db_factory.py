from cs5278_assignment_5.live6.geo_db import GeoDB, GeoDBImplementation


class GeoDBFactory:
    @classmethod
    def new_database(cls, bits_of_precision: int) -> GeoDB:
        """
        TODO

        Return an instance of your GeoDB implementation.
        """

        return GeoDBImplementation(bits_of_precision)