package producer

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import java.nio.charset.Charset

//вариант с краулерами, которые ждут задачии выполняют их (одна задача выполняется одним краулером)
private const val TASK_EXCHANGER = "task_exchanger";
private const val TASK_QUEUE_NAME = "task_queue"
//task_exchanger связываем в веб-консоли рабита с task_queue, чтоб он в нее отправлял задачи

fun main() {
    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.port = 5672

    factory.newConnection().use { connection ->
        connection.createChannel().use {
            val message = "Task....."

            // задекларировать очередь и прибиндить к эксченджеру можем тут, а можем в ресивере
//            it.queueDeclare(TASK_QUEUE_NAME, true, false, false, null)
//            it.queueBind(TASK_QUEUE_NAME, TASK_EXCHANGER, "")

            it.exchangeDeclare(TASK_EXCHANGER, BuiltinExchangeType.FANOUT)
            for (i in 0..20) {
                it.basicPublish(TASK_EXCHANGER, "", MessageProperties.PERSISTENT_TEXT_PLAIN, (message + i).toByteArray(Charset.defaultCharset()))
                println(" [x] Sent '$message'")
            }
        }
    }
}