import java.sql.DriverManager
import java.util.Properties

import KafkaProducer.sendData
import kafka.utils.Json
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import yahoofinance.{Stock, YahooFinance}

import scala.util.parsing.json.JSON
import org.json4s._
import org.json4s
import org.json4s.native.JsonMethods

object GoogleStockData {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    val topic = "test"
    props.put("bootstrap.servers", "127.0.0.1:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "1")
    props.put("retries", "3")
    props.put("linger.ms", "1")
    val producer = new KafkaProducer[String, String](props)
    var i = 0

    while (true) {
      val url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AAPL&interval=1min&apikey=***************"
      val result = scala.io.Source.fromURL(url).mkString
      val stock_data = JsonMethods.parse(result)

      val maps = stock_data.values
      val data = new ProducerRecord[String, String](topic, Integer.toString(i), stock_data.toString())
      producer.send(data)
      i += 1

    }



    producer.close()

  }

}

