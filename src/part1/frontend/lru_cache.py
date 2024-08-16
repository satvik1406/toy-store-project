from collections import OrderedDict
class LRUcache:
    def __init__(self,capacity):
        # Initialize the LRU cache with a specified capacity and an ordered dictionary to maintain item order
        self.cache = OrderedDict()
        self.capacity = capacity

    def put(self,key,value):
        # Add an item to the cache or update an existing item
        if len(self.cache) >= self.capacity:
            # If the cache is at capacity, remove the least recently used (oldest) item
            discarded = next(iter(self.cache))
            del self.cache[discarded]
        # Insert the item as the most recently used (newest) item
        self.cache[key] = value

    def get(self,key):
        # Retrieve an item from the cache
        if key in self.cache:
        # If the item is found, move it to the end to mark it as most recently used
            self.cache.move_to_end(key)
            return self.cache[key]
        else:
            # Return -1 if the item is not found
            return -1
    
    def delete(self, key):
        # Remove an item from the cache
        if key in self.cache:
            # If the item exists in the cache, delete it and return True
            del self.cache[key]
            return True 
        # If the item does not exist, return False
        return False
        
    
        
