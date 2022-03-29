package mutex

/**
 * Distributed mutual exclusion implementation.
 * All functions are called from the single main thread.
 *
 * @author Vsevolod Shaldin
 */
class ProcessImpl(private val env: Environment) : Process {
    private var inCS = false // are we in critical section?
    private var requested = false
    private val queue = ArrayDeque<Int>()
    private val forks = IntArray(env.nProcesses + 1) // 0 - haven't, 1 - have (clean), 2 - have (dirty)

    init {
        for (i in 1..env.nProcesses) {
            if (i > env.processId) {
                forks[i] = 2
            }
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        message.parse {
            when (readEnum<Type>()) {
                Type.REQ -> {
                    if (forks[srcId] == 2 && !inCS) {
                        forks[srcId] = 0
                        env.send(srcId) {
                            writeEnum(Type.OK)
                        }
                        if (requested) {
                            env.send(srcId) {
                                writeEnum(Type.REQ)
                            }
                        }
                    }
                    else {
                        queue.addLast(srcId)
                    }
                }
                Type.OK -> {
                    forks[srcId] = 1
                }
            }
            checkCSEnter()
        }
    }

    private fun checkCSEnter() {
        if (inCS) return
        if (calcForks() != env.nProcesses - 1) return
        inCS = true
        env.locked()
    }

    override fun onLockRequest() {
        requested = true
        if (calcForks() != env.nProcesses - 1) {
            for (i in 1..env.nProcesses) {
                if (i != env.processId && forks[i] == 0) {
                    env.send(i) {
                        writeEnum(Type.REQ)
                    }
                }
            }
        }
        else {
            inCS = true
            env.locked()
        }
    }

    override fun onUnlockRequest() {
        requested = false
        env.unlocked()
        inCS = false
        for (i in 1..env.nProcesses) {
            if (i != env.processId) {
                forks[i] = 2
            }
        }
        while (!queue.isEmpty()) {
            val element = queue.removeFirst()
            forks[element] = 0
            env.send(element) {
                writeEnum(Type.OK)
            }
        }
    }

    private fun calcForks(): Int {
        var count = 0
        for (i in 1..env.nProcesses) {
            if (i != env.processId && forks[i] != 0) {
                count += 1
            }
        }
        return count
    }

    enum class Type { REQ, OK }
}
