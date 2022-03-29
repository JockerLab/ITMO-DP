import system.DataHolderEnvironment

class DataHolderImpl<T : Comparable<T>>(
    private val keys: List<T>,
    private val dataHolderEnvironment: DataHolderEnvironment
) : DataHolder<T> {
    private var currentPointer: Int = 0
    private var savedPointer: Int = 0

    override fun checkpoint() {
        savedPointer = currentPointer
    }

    override fun rollBack() {
        currentPointer = savedPointer
    }

    override fun getBatch(): List<T> {
        val result = keys.subList(currentPointer, minOf(currentPointer + dataHolderEnvironment.batchSize, keys.size))
        currentPointer = minOf(currentPointer + dataHolderEnvironment.batchSize, keys.size)
        return result
    }
}