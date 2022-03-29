import system.MergerEnvironment

class MergerImpl<T : Comparable<T>>(
    private val mergerEnvironment: MergerEnvironment<T>,
    prevStepBatches: Map<Int, List<T>>?
) : Merger<T> {
    private val prevStepBatches = prevStepBatches
    private val currentBatches: MutableMap<Int, List<T>> = mutableMapOf()
    private val set: MutableSet<Pair<T, Int>> = sortedSetOf(compareBy ( {it.first}, {it.second} ))
    private val pointers: MutableList<Int> = mutableListOf()
    private var isBegin: Boolean = true

    override fun mergeStep(): T? {
        if (isBegin) {
            for (i in 0 until mergerEnvironment.dataHoldersCount) {
                if (prevStepBatches == null || prevStepBatches[i] == null) {
                    currentBatches[i] = mergerEnvironment.requestBatch(i)
                }
                else {
                    currentBatches[i] = prevStepBatches[i]!!
                }
                pointers.add(0)
                if (currentBatches[i] != null && currentBatches[i]!!.isNotEmpty()) {
                    pointers[i]++
                    set.add(Pair(currentBatches[i]!![0], i))
                }
            }
            isBegin = false
        }

        return if (set.size > 0) {
            val first = set.first()
            set.remove(first)
            if (pointers[first.second] == currentBatches[first.second]!!.size) {
                currentBatches[first.second] = mergerEnvironment.requestBatch(first.second)
                pointers[first.second] = 0
            }
            if (currentBatches[first.second] != null && currentBatches[first.second]!!.isNotEmpty()) {
                set.add(Pair(currentBatches[first.second]!![pointers[first.second]], first.second))
                pointers[first.second]++
            }
            first.first
        } else {
            null
        }
    }

    override fun getRemainingBatches(): Map<Int, List<T>> {
        var result: MutableMap<Int, MutableList<T>> = mutableMapOf()
        while (set.size > 0) {
            val first = set.first()
            set.remove(first)
            val cur = result.getOrDefault(first.second, mutableListOf())
            cur.add(first.first)
            result[first.second] = cur
        }
        for (i in 0 until mergerEnvironment.dataHoldersCount) {
            if (currentBatches[i] != null && currentBatches[i]!!.isNotEmpty() && pointers[i] < currentBatches[i]!!.size) {
                val cur = result.getOrDefault(i, mutableListOf())
                cur.addAll(currentBatches[i]!!.subList(pointers[i], currentBatches[i]!!.size))
                result[i] = cur
            }
        }
        return result
    }
}