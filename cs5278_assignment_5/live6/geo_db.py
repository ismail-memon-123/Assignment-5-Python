from abc import ABC, abstractmethod
from cs5278_assignment_5.live6.geo_hash import GeoHash

class HashNode:
    def __init__(self):
        self.left = None
        self.right = None
        self.isSentinal = False
        self.coordinates = None

class BinaryTrie:
    def __init__(self):
        self.root = HashNode()

    # This takes an array of bools and inserts it where needed in trie.
    def insert(self, bool_array, coordinates):
        print("bool_araay")
        print(bool_array)
        current_node = self.root
        for boolean in bool_array:
            if not boolean:
                # 0 value
                if current_node.left is None:
                    current_node.left = HashNode()
                current_node = current_node.left
            else:
                # 1 value
                if current_node.right is None:
                    current_node.right = HashNode()
                current_node = current_node.right
        # After traversing done we want to mark as sentinal and label coordinates.
        current_node.isSentinal = True
        current_node.coordinates = coordinates
    
    # Searches and returns node of where the specified bool_array is, if not it returns None

    def search(self, bool_array):
        current_node = self.root
        for boolean in bool_array:
            if current_node is None:
                return None
            if not boolean:
                current_node = current_node.left
                print("LEFT")
            else:
                current_node = current_node.right
                print("RIGHT")
        print(current_node.right)
        print(current_node.left)
        return current_node
    
    def contains(self, bool_array):
        # Search and return true if sentinal
        node = self.search(bool_array)
        if node is None:
            return False
        return node.isSentinal

    def cleanTrie(self, startNode, bool_array, iterator):
        if (iterator) == len(bool_array):
            # These changes may allow cleaning if not sentinal which is useful here.
            if not startNode.isSentinal:
                return False
            startNode.isSentinal = False
            return (startNode.left is None) and (startNode.right is None)
        boolean = bool_array[iterator]
        nextNode = startNode.right
        if not boolean:
            nextNode = startNode.left
        if nextNode is None:
            # Cannot proceed to search
            return False
        # each time we go down the path we make sure to call this so when we hit the sentinal we can clean up recursively.
        moreClean = self.cleanTrie(nextNode, bool_array, iterator+1)
        if moreClean:
            if not boolean:
                startNode.left = None
            else:
                startNode.right = None
            return (startNode.left is None) and (startNode.right is None)
        return False

    def delete(self, bool_array):
        # we should have it first and then only we can make sure if deleted
        flag = (self.search(bool_array) is not None)
        self.cleanTrie(self.root, bool_array, 0)
        return ((self.search(bool_array) is None) and flag)
       
    def traverse_helper(self, node, return_value):
        print("HI")
        if node is None:
            return
        if (node.right is None and node.left is None):
            print("HOHOHO")
            return_value.append(node)
            return
        self.traverse_helper(node.left, return_value)
        self.traverse_helper(node.right, return_value)

    # This method returns a list of all the nodes that are sentinals.
    def traverse(self, startNode):
        return_value = []
        if startNode is None:
            return return_value
        self.traverse_helper(startNode, return_value)
        return return_value


class GeoDB(ABC):
    """
    Using your GeoHash implementation, create an implementation of this GeoDB
    interface that allows you to insert, delete, and perform proximity searches
    on latitude, longitude coordinate pairs.
    There are a LOT of ways to implement this. Future assignments will build on
    this implementation and also introduce new or change requirements. You will
    have to maintain this code and refactor it as needed over the course of the
    class.
    Your solution should provide better than O(n) lookup time for nearby(..) and
    contains(..). This means that a list will not work. If your solution uses
    a 'for loop' to iterate through hashes and check for matching prefixes when
    you call nearby(), then it needs to be redesigned.
    You will also need to update the GeoDBFactory.
    There will be ambiguous requirements. Use your best judgement in determining
    an appropriate interpretation. Keep these ambiguities in mind when you code
    review others' solutions later in class.
    """

    @abstractmethod
    def __init__(self):
        """
        Your GeoDB implementation should take the maximum
        number of bits of precision as a constructor parameter.
        """

        raise NotImplementedError

    @abstractmethod
    def insert(self, lat: float, lon: float) -> None:
        """
        When you call this insert method, it should use the
        maximum bits of precision when calculating the geohash
        for the inserted data.
        Inserts a location into the database. No
        duplicates are stored. If the position is already
        present, it should be overwritten.
        """

        raise NotImplementedError

    @abstractmethod
    def delete(self, lat: float, lon: float) -> bool:
        """
        Deletes the specified location from the database.
        Your GeoDB implementation should take the maximum
        number of bits of precision as a constructor parameter.
        When you call this deletion method, it should use the
        maximum bits of precision when calculating the geohash
        to search for to delete the associated location(s).
        Returns True if an item was deleted.
        """

        raise NotImplementedError

    @abstractmethod
    def delete_all(self, lat: float, lon: float, bits_of_precision: int) -> list[list[float]]:
        """
        Deletes all locations from the database that
        match the provided latitude and longitude
        up to the specified number of bits of precision
        in their geohashes.
        For example, if you are using 3 bits of precision,
        then the following two geohashes match:
        0100001 => 010
        0101111 => 010
        With 4 bits of precision, they don't match:
        0100001 => 0100
        0101111 => 0101
        Returns the list of deleted locations.
        If bits_of_precision == 0, then this method should delete everything.
        """

        raise NotImplementedError

    @abstractmethod
    def contains(self, lat: float, lon: float, bits_of_precision: int) -> bool:
        """
        Returns True if the database contains at least one location that
        matches the provided latitude and longitude
        up to the specified number of bits of precision
        in its geohash.
        For example, if you are using 3 bits of precision,
        then the following two geohashes match:
        0100001 => 010
        0101111 => 010
        With 4 bits of precision, they don't match:
        0100001 => 0100
        0101111 => 0101
        If bits_of_precision == 0, then this method should always return True.
        """

        raise NotImplementedError

    @abstractmethod
    def nearby(self, lat: float, lon: float, bits_of_precision: int) -> list[list[float]]:
        """
        Returns all locations in the database that
        match the provided latitude and longitude
        up to the specified number of bits of precision
        in their geohashes.
        For example, if you are using 3 bits of precision,
        then the following two geohashes match:
        0100001 => 010
        0101111 => 010
        With 4 bits of precision, they don't match:
        0100001 => 0100
        0101111 => 0101
        If bits_of_precision == 0, then this method should
        always return everything in the database.
        """

        raise NotImplementedError
    

class GeoDBImplementation(GeoDB):
    """
    Using your GeoHash implementation, create an implementation of this GeoDB
    interface that allows you to insert, delete, and perform proximity searches
    on latitude, longitude coordinate pairs.

    There are a LOT of ways to implement this. Future assignments will build on
    this implementation and also introduce new or change requirements. You will
    have to maintain this code and refactor it as needed over the course of the
    class.

    Your solution should provide better than O(n) lookup time for nearby(..) and
    contains(..). This means that a list will not work. If your solution uses
    a 'for loop' to iterate through hashes and check for matching prefixes when
    you call nearby(), then it needs to be redesigned.

    You will also need to update the GeoDBFactory.

    There will be ambiguous requirements. Use your best judgement in determining
    an appropriate interpretation. Keep these ambiguities in mind when you code
    review others' solutions later in class.
    """
    def __init__(self, maxBits):
        """
        Your GeoDB implementation should take the maximum
        number of bits of precision as a constructor parameter.
        """

        self.maxBits = maxBits
        self.GeoHashTrie = BinaryTrie()

    def insert(self, lat: float, lon: float) -> None:
        
        LATITUDE_RANGE = [-90, 90]
        LONGITUDE_RANGE = [-180, 180]

        geoHashValue = GeoHash.geo_hash(lat, lon, self.maxBits)
        print("geoHashCalcu")
        print(geoHashValue)
        self.GeoHashTrie.insert(geoHashValue, [lat, lon])

    def delete(self, lat: float, lon: float) -> bool:
        LATITUDE_RANGE = [-90, 90]
        LONGITUDE_RANGE = [-180, 180]

        geoHashValue = GeoHash.geo_hash(lat, lon, self.maxBits)
        # Method will return false if not found so this matches behavior.
        return self.GeoHashTrie.delete(geoHashValue)

    def nodeListToCoordinateList(self, nodeList):
        coordinateList = []
        for node in nodeList:
            coordinateList.append(node.coordinates)
        return coordinateList

    def delete_all(self, lat: float, lon: float, bits_of_precision: int) -> list[list[float]]:
        LATITUDE_RANGE = [-90, 90]
        LONGITUDE_RANGE = [-180, 180]

        if bits_of_precision == 0:
            nodeList = self.GeoHashTrie.traverse(self.GeoHashTrie.root)
            coordinate_list = self.nodeListToCoordinateList(nodeList)
            self.GeoHashTrie.root.left = None
            self.GeoHashTrie.root.right = None
            return coordinate_list
        geoHashValue = GeoHash.geo_hash(lat, lon, bits_of_precision)
        startNode = self.GeoHashTrie.search(geoHashValue)
        # startNode may be none if not found else it is node that has that value does not need to be sentinal
        if startNode is None:
            return []
        nodeList = self.GeoHashTrie.traverse(startNode)
        coordinate_list = self.nodeListToCoordinateList(nodeList)
        startNode.left = None
        startNode.right = None
        return coordinate_list

    def contains(self, lat: float, lon: float, bits_of_precision: int) -> bool:
        LATITUDE_RANGE = [-90, 90]
        LONGITUDE_RANGE = [-180, 180]

        geoHashValue = GeoHash.geo_hash(lat, lon, bits_of_precision)
        startNode = self.GeoHashTrie.search(geoHashValue)
        # We need one of the children to exist OR we need the node to be sentianl if we are seraching all
        # the way and only for one value not a bunch.
        if startNode is None:
            return False
        return (startNode.left is not None) or (startNode.right is not None) or (startNode.isSentinal)


    def nearby(self, lat: float, lon: float, bits_of_precision: int) -> list[list[float]]:
        LATITUDE_RANGE = [-90, 90]
        LONGITUDE_RANGE = [-180, 180]

        if bits_of_precision == 0:
            startNode = self.GeoHashTrie.root
        else:
            print("bop")
            print(str(bits_of_precision))
            geoHashValue = GeoHash.geo_hash(lat, lon, bits_of_precision)
            print("geohash val")
            print(geoHashValue)
            startNode = self.GeoHashTrie.search(geoHashValue)
        if startNode is None:
            return []
        list_of_nodes = self.GeoHashTrie.traverse(startNode)
        print("len of nodes")
        print(str(len(list_of_nodes)))
        return self.nodeListToCoordinateList(list_of_nodes)
