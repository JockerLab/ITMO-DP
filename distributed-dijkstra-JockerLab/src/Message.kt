package dijkstra.messages

sealed class Message

data class MessageWithDistance(val data: Long) : Message()
object AddChildMessage : Message()
object RemoveChildMessage : Message()
object AckMessage : Message()
