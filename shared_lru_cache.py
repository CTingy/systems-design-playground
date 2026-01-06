import threading
from dataclasses import dataclass
from typing import Dict, Optional, List


@dataclass
class Node:
    key: int
    val: int
    prev: Optional["Node"] = None
    next: Optional["Node"] = None


class LRUCache:
    """
    Thread-safe LRU cache (O(1) get/put) using:
      - dict: key -> node
      - doubly linked list: most-recent at tail, least-recent at head.next
    """

    def __init__(self, capacity: int):
        self.capacity = max(0, int(capacity))
        self.mapping: Dict[int, Node] = {}

        # sentinel nodes
        self.head = Node(-1, -1)
        self.tail = Node(-1, -1)
        self.head.next = self.tail
        self.tail.prev = self.head

        self.lock = threading.RLock()

    # ----- list helpers (caller must hold lock) -----

    def _add_to_tail(self, node: Node) -> None:
        """Insert node right before tail (most recent)."""
        last = self.tail.prev
        assert last is not None  # sentinel always has prev

        last.next = node
        node.prev = last
        node.next = self.tail
        self.tail.prev = node

    def _unlink(self, node: Node) -> None:
        """Remove node from the list (node must be linked)."""
        prev = node.prev
        nxt = node.next
        if prev is None or nxt is None:
            # Defensive: shouldn't happen if used correctly
            return
        prev.next = nxt
        nxt.prev = prev
        node.prev = node.next = None  # optional: helps avoid accidental reuse bugs

    def _touch(self, node: Node) -> None:
        """Mark as most-recent."""
        self._unlink(node)
        self._add_to_tail(node)

    def _evict_lru(self) -> None:
        """Evict least-recently-used node if over capacity."""
        if len(self.mapping) <= self.capacity:
            return
        lru = self.head.next
        if lru is None or lru is self.tail:
            return
        self._unlink(lru)
        self.mapping.pop(lru.key, None)

    # ----- public API -----

    def get(self, key: int) -> int:
        with self.lock:
            node = self.mapping.get(key)
            if node is None:
                return -1
            self._touch(node)
            return node.val

    def put(self, key: int, value: int) -> None:
        if self.capacity == 0:
            return

        with self.lock:
            node = self.mapping.get(key)
            if node is not None:
                # update + mark most-recent
                node.val = value
                self._touch(node)
                return

            node = Node(key, value)
            self.mapping[key] = node
            self._add_to_tail(node)
            self._evict_lru()


class ShardedLRUCache:
    """
    Sharded (approximate) LRU for better parallelism:
      - LRU is per-shard, not global.
      - Capacity is distributed across shards (with remainder spread).
    """

    def __init__(self, total_capacity: int, num_shards: int = 16):
        self.num_shards = max(1, int(num_shards))
        total_capacity = max(0, int(total_capacity))

        base = total_capacity // self.num_shards
        rem = total_capacity % self.num_shards

        # spread the remainder so total capacity matches exactly
        caps: List[int] = [base + (1 if i < rem else 0) for i in range(self.num_shards)]
        self.shards = [LRUCache(c) for c in caps]

    def _get_shard(self, key: int) -> LRUCache:
        # For an in-memory toy cache, Python's hash() is fine.
        # TODO: Use a stable hash for stable sharding across processes
        return self.shards[hash(key) % self.num_shards]

    def get(self, key: int) -> int:
        return self._get_shard(key).get(key)

    def put(self, key: int, value: int) -> None:
        self._get_shard(key).put(key, value)
