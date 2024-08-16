from collections import OrderedDict

class LRUcache:
    def __init__(self,capacity):
        self.cache = OrderedDict()
        self.capacity = capacity

    def put(self,key,value):
        if len(self.cache) >= self.capacity:
            discarded = next(iter(self.cache))
            del self.cache[discarded]
        self.cache[key] = value

    def get(self,key):
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        else:
            return -1
    
    def delete(self, key):
        if key in self.cache:
            del self.cache[key]
            return True 
        return False
        
    
        
