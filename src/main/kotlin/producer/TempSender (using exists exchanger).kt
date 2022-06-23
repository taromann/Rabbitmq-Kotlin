package producer

import com.rabbitmq.client.ConnectionFactory
// Вариант подключения к уже существующему эксченджеру, которы создали заранее.
fun main() {
    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.port = 5672

    factory.newConnection().use {connection ->
        connection.createChannel().use {
            it.basicPublish("hello_exchanger", "java", null, "TempSender".toByteArray())
        }
    }
}