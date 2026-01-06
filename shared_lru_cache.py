import threading


class Node:
    def __init__(self, key, val):
        self.val = val
        self.key = key
        self.next = None
        self.prev = None


class LRUCache:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.mapping = {}
        self.head = Node(0, 0)
        self.tail = Node(0, 0)
        self.tail.prev = self.head
        self.head.next = self.tail
        # for insert/remove
        self.lock = threading.RLock() 

    def _insert(self, node):
        # the caller should acquire locks before calling this func
        prev = self.tail.prev
        self.tail.prev = node
        node.next = self.tail
        prev.next = node
        node.prev = prev
    
    def _remove(self, node):
        # the caller should acquire locks before calling this func
        prev = node.prev
        next_ = node.next
        prev.next = next_
        next_.prev = prev

    def get(self, key: int) -> int:
        with self.lock:
            if key not in self.mapping:
                return -1

            node = self.mapping[key]
            self._remove(node)
            self._insert(node)
            return node.val

    def put(self, key: int, value: int) -> None:
        with self.lock:
            if key in self.mapping:
                self._remove(self.mapping[key])
            
            new_node = Node(key, value)
            self.mapping[key] = new_node
            self._insert(new_node)

            if len(self.mapping) > self.capacity:
                evict = self.head.next
                self._remove(evict)
                # prevent memory leak
                if evict.key in self.mapping:
                    del self.mapping[evict.key]


# obj = LRUCache(capacity)
# param_1 = obj.get(key)
# obj.put(key,value)


class ShardedLRUCache: # approximate LRU for better parallelism
    def __init__(self, total_capacity, num_shards=16):
        self.num_shards = num_shards
        # allocate capacity for each shard
        # TODO: implement LRU for more stable hashing result
        shard_cap = total_capacity // num_shards
        self.shards = [LRUCache(shard_cap) for _ in range(num_shards)]

    def _get_shard(self, key):
        return self.shards[hash(key) % self.num_shards]

    def get(self, key):
        shard = self._get_shard(key)
        return shard.get(key)

    def put(self, key, value):
        shard = self._get_shard(key)
        shard.put(key, value)
