package producer

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.ConnectionFactory

// Первый вариант, что эксченджер накидывает задачи в очередь, а ресиверы будут выполнять (1 задачу выполнит 1 получатель (конкуренция за задачи))

const val QUEUE_NAME = "hello"
const val EXCHANGER_NAME = "hello_exchanger"  //создаем обменник

fun main() {
    val factory = ConnectionFactory() //открываем соединение
    factory.host = "localhost"  //подключаемся к рабиту
    factory.port = 5672
//    factory.username = "guest"
//    factory.password = "guest"

    factory.newConnection().use { connection ->
        connection.createChannel().use {//создаем канал связи
            //exchangeDeclare значит что если такой эксченджер есть то трогать не будем, нет - создадим
            it.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.DIRECT) //задаем тип обменника, и говорим что хотим на сервере создать такой эксченджер
            //аналогично создаем очередь
            it.queueDeclare(QUEUE_NAME, false, false, false, null)
            //привязываем эксченджер к очереди (создаем связь с ключем java, т.е. только если будет такой ключ, сообщения будут пролетать )
            it.queueBind(QUEUE_NAME, EXCHANGER_NAME, "java")

            val message = "Hello World!"
            //в канал подключения к рабиту отправляем сообщение
            it.basicPublish(EXCHANGER_NAME, "java", null, message.toByteArray())
            println(" [x] Sent '$message'")
        }
    }
}