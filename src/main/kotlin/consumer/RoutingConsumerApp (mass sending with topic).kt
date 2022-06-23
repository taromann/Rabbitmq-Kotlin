package consumer

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import java.nio.charset.Charset

// Вариант для рассылки одинаковых сообщений на несколько сервисов. Просто рассылает в канал сообщения с тегом
// TOPIC-Exchanger - обмен сообщениями по определенной теме
private const val EXCHANGE_NAME = "topic_exchange"

fun main() {
    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.port = 5672

    val chanel = factory.newConnection().createChannel()
    chanel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC)

    //создаем временную очередь при запуске, чтоб из нее можно было что-то доставать
    val queueName = chanel.queueDeclare().queue
    println("QUEUE NAME: $queueName")

    // нас интересует тема такая то (ключ): # - любая последовательность символов, * - любое одно слово
    val routingKey = "prog.*"
    chanel.queueBind(queueName, EXCHANGE_NAME, routingKey)
    println(" [*] Waiting for messages with routing key ($routingKey):")

    val deliverCallback = DeliverCallback { consumerTag, delivery ->
        val message = String(delivery.body, Charset.defaultCharset())
        println(" [x] Received '" + delivery.envelope.routingKey + "':'" + message + "'")
    }

    chanel.basicConsume(queueName, true, deliverCallback, CancelCallback { consumerTag: String? -> })

}