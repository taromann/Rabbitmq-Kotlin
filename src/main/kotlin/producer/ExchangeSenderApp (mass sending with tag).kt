package producer

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.ConnectionFactory
import java.nio.charset.Charset

// Вариант для рассылки одинаковых сообщений на несколько сервисов. Просто рассылает в канал сообщения с тегом
private const val EXCHANGER_NAME = "directExchanger"

fun main() {
    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.port = 5672

    factory.newConnection().use {connection ->
        connection.createChannel().use {
            it.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.DIRECT)
            it.basicPublish(EXCHANGER_NAME, "java", null, "SimpleSenderApp_java".toByteArray(Charset.defaultCharset()))
            it.basicPublish(EXCHANGER_NAME, "php", null, "SimpleSenderApp_php".toByteArray(Charset.defaultCharset()))
            println(" [x] Sent '" + "SimpleSenderApp" + "'")
        }
    }
}