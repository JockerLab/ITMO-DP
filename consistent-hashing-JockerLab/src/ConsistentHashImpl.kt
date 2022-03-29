class ConsistentHashImpl<K> : ConsistentHash<K> {
    private var shards: ArrayList<Pair<Int, Shard>> = arrayListOf()

    override fun getShardByKey(key: K): Shard {
        val hash = key.hashCode()
        val pos = lowerBound(hash)
        return shards[pos].second
    }

    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        val result: MutableMap<Shard, Set<HashRange>> = mutableMapOf()
        for (hash in vnodeHashes) {
            if (shards.size == 0) {
                continue
            }
            val pos = lowerBound(hash)
            val prev = (pos + shards.size - 1) % shards.size
            var nextShardHashes = result.getOrDefault(shards[pos].second, setOf())
            nextShardHashes = uniteRanges(nextShardHashes, HashRange(shards[prev].first + 1, hash))
            result[shards[pos].second] = nextShardHashes
        }
        for (hash in vnodeHashes) {
            shards.add(Pair(hash, newShard))
        }
        shards.sortBy { it.first }
        return result
    }

    private fun uniteRanges(setHashRanges: Set<HashRange>, hashRange: HashRange): Set<HashRange> {
        var result: Set<HashRange> = setOf()
        var flag = false
        for (range in setHashRanges) {
            if (range.leftBorder == hashRange.leftBorder) {
                flag = true
                if (hashRange.rightBorder < hashRange.leftBorder) {
                    if (range.rightBorder < range.leftBorder) {
                        if (hashRange.rightBorder > range.rightBorder) {
                            result = result.plus(HashRange(range.leftBorder, hashRange.rightBorder))
                            continue
                        }
                    } else {
                        result = result.plus(HashRange(range.leftBorder, hashRange.rightBorder))
                        continue
                    }
                } else {
                    if (range.rightBorder > range.leftBorder && hashRange.rightBorder > range.rightBorder) {
                        result = result.plus(HashRange(range.leftBorder, hashRange.rightBorder))
                        continue
                    }
                }
            }
            result = result.plus(range)
        }
        if (!flag) {
            result = result.plus(hashRange)
        }
        return result
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        val result: MutableMap<Shard, Set<HashRange>> = mutableMapOf()
        val newShards: ArrayList<Pair<Int, Shard>> = arrayListOf()
        var pos = 0
        for (elem in shards) {
            if (elem.second.shardName == shard.shardName) {
                var prev = -1
                var next = -1
                for (i in pos until shards.size) {
                    if (shards[i].second.shardName != shard.shardName) {
                        prev = i
                        if (next == -1) {
                            next = i
                        }
                    }
                }
                for (i in 0..pos) {
                    if (shards[i].second.shardName != shard.shardName) {
                        prev = i
                        if (next == -1) {
                            next = i
                        }
                    }
                }
                var nextShardHashes = result.getOrDefault(shards[next].second, setOf())
                nextShardHashes = uniteRanges(nextShardHashes, HashRange(shards[prev].first + 1, elem.first))
                result[shards[next].second] = nextShardHashes
            } else {
                newShards.add(elem)
            }
            pos++
        }
        shards = newShards
        return result
    }

    private fun lowerBound(hash: Int): Int {
        var l = -1
        var r = shards.size - 1
        while (l + 1 < r) {
            val m = (l + r) / 2
            if (shards[m].first < hash) {
                l = m
            } else {
                r = m
            }
        }
        if (shards.size == 0 || shards[r].first < hash) {
            r = 0
        }
        return r
    }
}