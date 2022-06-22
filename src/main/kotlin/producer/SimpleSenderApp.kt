package producer

import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.ConnectionFactory

const val QUEUE_NAME = "hello"
const val EXCHANGER_NAME = "hello_exchanger"  //создаем обменник

// docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3

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