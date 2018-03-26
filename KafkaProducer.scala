import java.sql.{DriverManager, ResultSet}
import java.util.Properties

/*import com.microsoft.sqlserver.jdbc.SQLServerResultSet*/
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/*class SQLServerConnection {

  val main_ID: Int = 0

}*/

object KafkaProducer {

  var main_ID: Int = 0

  def sendData(rs: ResultSet, producer:KafkaProducer[String,String], topic:String): Unit={
    while ((rs.next())) {
      val id = rs.getInt("id")
      main_ID = id
      val firstName = rs.getString("FirstName")
      val lastName = rs.getString("LastName")
      val age = rs.getInt("Age")
      val salary = rs.getString("Salary")
      val msg = id + "," + firstName + "," + lastName + "," + age + "," + salary
      val data = new ProducerRecord[String, String](topic, Integer.toString(id), msg)
      producer.send(data)

    }
  }

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
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val myUrl = "jdbc:sqlserver://localhost:1433;databaseName=employees"
    val conn = DriverManager.getConnection(myUrl, "sa", "root")

    while (true) {
      val query = "select * from employee where id > " + main_ID
      val statement = conn.createStatement()
      val rs = statement.executeQuery(query)
      /*print("Hi " + main_ID)*/
      sendData(rs, producer, topic)
    }
    /*for(i <- Range(0, 50)){
      val msg = "Message with id:" + Integer.toString(i)
      val data = new ProducerRecord[String, String](topic, Integer.toString(i), msg)
      producer.send(data)
    }*/
    producer.close()

  }
}
