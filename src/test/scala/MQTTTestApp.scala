import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.mqtt.{MQTTSink, MQTTSinkConfig, MQTTSource, MQTTSourceConfig}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object MQTTTestApp {

  def main(args: Array[String]): Unit = {
    val uri = "tcp://test.mosquitto.org:1883"

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceConfig = new MQTTSourceConfig.Builder[String]()
      .setURI(uri)
      .setClientIdPrefix("testid_sub")
      .setQos(1)
      .setTopic("#")
      .setDeserializationSchema(new SimpleStringSchema())
      .build()
    val sinkConfig = new MQTTSinkConfig.Builder[String]()
      .setURI(uri)
      .setClientIdPrefix("testid_pub")
      .setQos(1)
      .setTopic("test")
      .setRetained(false)
      .setSerializationSchema(new SimpleStringSchema())
      .build()

    val source = env.addSource(new MQTTSource[String](sourceConfig))
    //val source = env.socketTextStream("localhost", 9999)

    val histogram = source
      .map { x => ((x.length / 100.0).round * 100).toInt }
      .map { x => (x, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
    val result = histogram
      .map(List(_))
      .keyBy({ x => 0 })
      .timeWindow(Time.seconds(5))
      .reduce(_ ::: _)

    result
      .map { x => x.toString }
      .addSink(new MQTTSink[String](sinkConfig))

    result.print()

    env.execute("test job")

  }
}

