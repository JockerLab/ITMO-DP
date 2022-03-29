package dijkstra

import dijkstra.messages.*
import dijkstra.system.environment.Environment

class ProcessImpl(private val environment: Environment) : Process {
    private var d: Long = Long.MAX_VALUE
    private var isRoot: Boolean = false
    private var parentId: Int? = null
    private var childCount: Int = 0
    private var balance: Int = 0

    override fun onMessage(srcId: Int, message: Message) {
        if (message is MessageWithDistance) {
            if (message.data < d) {
                if (parentId != null) {
                    balance++
                    environment.send(parentId!!, RemoveChildMessage)
                }
                parentId = srcId
                balance++
                environment.send(parentId!!, AddChildMessage)
                d = message.data
                sendDistanceToNeighbours()
            }
        }
        if (message is AckMessage) {
            balance--
        }
        if (message is AddChildMessage) {
            childCount++
        }
        if (message is RemoveChildMessage) {
            childCount--
        }
        if (message !is AckMessage) {
            environment.send(srcId, AckMessage)
        }
        if (childCount == 0 && balance == 0) {
            if (isRoot) {
                environment.finishExecution()
            } else if (parentId != null) {
                balance++
                environment.send(parentId!!, RemoveChildMessage)
                parentId = null
            }
        }
    }

    override fun getDistance(): Long? {
        return if (d == Long.MAX_VALUE) {
            null
        } else {
            d
        }
    }

    override fun startComputation() {
        d = 0L
        isRoot = true
        sendDistanceToNeighbours()
        if (childCount == 0 && balance == 0) {
            environment.finishExecution()
        }
    }

    private fun sendDistanceToNeighbours() {
        for ((to, len) in environment.neighbours) {
            if (to != environment.pid) {
                balance++
                environment.send(to, MessageWithDistance(len + d))
            }
        }
    }
}