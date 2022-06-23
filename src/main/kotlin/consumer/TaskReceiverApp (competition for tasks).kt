package consumer

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import java.nio.charset.Charset

private const val TASK_QUEUE_NAME = "task_queue"
private const val TASK_EXCHANGER = "task_exchanger"

fun main() {
    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.port = 5672

    val connection = factory.newConnection()
    val channel = connection.createChannel()

// задекларировать очередь и прибиндить к эксченджеру можем тут, а можем в продюсере
    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null)
    channel.queueBind(TASK_QUEUE_NAME, TASK_EXCHANGER, "")
    println(" [*] Waiting for messages")

    // prefetchCount - предельное количество загрузки задач на одного обработчика. Позволяет равномерно распределить задачи по краулерам
    channel.basicQos(3)

    // краулер вытаскивает пачку сообщений из очереди за раз, чтоб сократить загрузку на сеть. Если такое не надо - настраиваем prefetchCount - предельное количество загрузки задач
    // когда получам сообщение - действуем так то
    val deliverCallback = DeliverCallback { consumerTag, delivery ->
        val message = String(delivery.body, Charset.defaultCharset())
        println(" [x] Received '" + message + "'")

        //  if (1 < 10) throw new RuntimeException("Oops"); // тест того что обработчик упал с эксепшеном
        doWork(message)
        println(" [x] Done")

        // отправляем сигнал, что сообщение обработано
        channel.basicAck(
            delivery.envelope.deliveryTag /*идентификатор сообщение, чтоб понимали какое обработано*/,
            false
        )
    }

    // DeliverCallback вешаем на очередь. В случае указания во второй опции (autoAck автоподтверждение) true -
    // значит что как только сообщение достали из очереди, считаем его автоматом обработанным.
    // В случае false - мы обязательно должны отправить сигнал что сообщение обработано
    channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback) { consumerTag: String? -> }
}

fun doWork(message: String) {
    message.toCharArray().forEach {
        if (it ==".".single()) {
            try {
                Thread.sleep(1000)
            } catch (_ignored: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
    }
}
