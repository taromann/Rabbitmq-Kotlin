package consumer

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import java.nio.charset.Charset

// Вариант для рассылки одинаковых сообщений на несколько сервисов.
// Сервисы подключаются к рабиту и создают каждый свою временную очередь, в которую сендер рассылает сообщения (он во все рассылает одно и то же,
// ему не важно сколько сервисов принимают их). При этом receiver получит только те сообщения, которые отправлены после его подключения

private const val EXCHANGER_NAME = "directExchanger"

fun main() {
    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.port = 5672
    val chanel = factory.newConnection().createChannel()
    chanel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.DIRECT)
    // Приложение создает для себя временную очередь
    val queueName = chanel.queueDeclare().queue
    println("My queue name: $queueName")
    // привязываемся к эксченджеру с темой php
    chanel.queueBind(queueName, EXCHANGER_NAME, "php")
    // способ повесить 2 разные темы на одну очередь
    chanel.queueBind(queueName, EXCHANGER_NAME, "java"); //подключаем второй бинд (ключ, по которому будут получаться сообщения)
    println(" [*] Waiting for a messages")
    // Листенер делаем только один раз, даже на несколько биндов
    // Слушатель, не связан с ексченджером, а только с очередью (говорит что когда в очередь что-то попадет, он на это среагирует так то)
    val deliverCallback = DeliverCallback { consumerTag, delivery ->
        val message = String(delivery.body, Charset.defaultCharset())
        println(Thread.currentThread().name)
        println(" [x] Received '$message'")
    }

    chanel.basicConsume(queueName, true, deliverCallback) { consumerTag -> }; // вешаем на очередь basicConsume

}