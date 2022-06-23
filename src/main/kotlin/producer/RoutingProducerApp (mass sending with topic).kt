package producer

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.ConnectionFactory
import java.nio.charset.Charset

// Вариант для рассылки одинаковых сообщений на несколько сервисов. Просто рассылает в канал сообщения с тегом
// TOPIC-Exchanger - обмен сообщениями по определенной теме
// TOPIC отличается от FANOUT только тем, что на routingKey не смотрит
private const val EXCHANGE_NAME = "topic_exchange";

fun main() {
    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.port = 5672

    factory.newConnection().use {connection ->
        connection.createChannel().use {
            it.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC)
            val routingKey = "prog.java"; //указываем тему сообщения - ключ
            val message = "topic_exchange"; // само сообщение

            it.basicPublish(EXCHANGE_NAME, routingKey, null, message.toByteArray(Charset.defaultCharset()))
            println(" [x] Sent '$message'")
        }
    }
}