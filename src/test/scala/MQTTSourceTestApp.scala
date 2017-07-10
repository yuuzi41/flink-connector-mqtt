import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.mqtt
import org.apache.flink.streaming.connectors.mqtt.{MQTTSource, MQTTSourceConfig}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object MQTTSourceTestApp {

  def main(args: Array[String]): Unit = {
    val uri = "tcp://test.mosquitto.org:1883"

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val config = new mqtt.MQTTSourceConfig.Builder[String]()
      .setURI(uri)
      .setClientId("testid")
      .setQos(1)
      .setTopic("#")
      .setDeserializationSchema(new SimpleStringSchema())
      .build()

    val source = env.addSource(new MQTTSource[String](config))
    //val source = env.socketTextStream("localhost", 9999)

    val histogram = source
      .map { x => ((x.length / 100.0).round * 100).toInt }
      .map { x => (x, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(2))
      .sum(1)
    val result = histogram
      .map(List(_))
      .keyBy({ x => 0 })
      .timeWindow(Time.seconds(2))
      .reduce(_ ::: _)


    result.print()

    env.execute("test job")

  }
}

