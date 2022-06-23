package consumer

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import java.nio.charset.Charset

// Вариант подключения к уже существующей очереди, которую создали заранее.
// Выполнять 1 задачу будет 1 получатель (конкуренция за задачи)
fun main() {
    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.port = 5672

    val channel = factory.newConnection().createChannel()
    val deliverCallback = DeliverCallback { consumerTag, delivery ->
        println(String(delivery.body , Charset.defaultCharset()))
    }
    channel.basicConsume("hello", true, deliverCallback) { consumerTag: String? -> }
}