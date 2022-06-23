package consumer

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import java.nio.charset.Charset

// Первый вариант, что эксченджер накидывает задачи в очередь, а ресиверы будут выполнять (1 задачу выполнит 1 получатель (конкуренция за задачи))

const val QUEUE_NAME = "hello"

fun main() {
    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.port = 5672

    val connection = factory.newConnection()
    val chanel = connection.createChannel()
    // Создаем листенер и ждем коллбэки.
    // Второй параметр durable - долговечность, значит что при false сли сервер рабит перезапустится, то очереди удаляются. Если tru, то очередь восстановится после перезапуска сервера
    // Третий параметр exclusive: в один момент времени с очередью может работать одно соединение, 2 приложение с такой очередью не смогут работать
    // Четвертый параметр false - autoDelete, значит что очередь живет, пока живет текущее соединение (пока приложение, создавшее очередь, живет)
    // Если очередь с названием hello есть, то подключается к ней, если нет, то создает
    chanel.queueDeclare(QUEUE_NAME, false, false, false, null)
    println(" [*] Waiting for messages")

    //в момент получения данных из очереди выполняем след действие
    val deliverCallback : DeliverCallback =
        DeliverCallback { consumerTag: String , delivery: Delivery ->  //delivery - просто пачка байтов, в которой все что угодно
            val message = String(
                delivery.body,
                Charset.defaultCharset()
            ) // мы знаем что байтовая строка в нашем случае представляет собой обычную строку
            println(" [x] Received '$message'")
        }

    //когда в очередь QUEUE_NAME придет сообщение мы применяем к нему функцию consumerTag
    chanel.basicConsume(
        QUEUE_NAME,
        true,
        deliverCallback
    ) { consumerTag: String? ->  }
}