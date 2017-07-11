package org.apache.flink.streaming.connectors.mqtt


import org.apache.flink.streaming.util.serialization.SerializationSchema
import org.apache.flink.util.Preconditions

class MQTTSinkConfig[IN](
                          uriArg: String,
                          clientIdPrefixArg: String,
                          topicArg: String,
                          qosArg: Int,
                          retainedArg: Boolean,
                          serializationSchemaArg: SerializationSchema[IN]
                           ) {

  val uri = Preconditions.checkNotNull(uriArg, "uri not set")
  val clientIdPrefix = Preconditions.checkNotNull(clientIdPrefixArg, "clientId not set")
  val topic = Preconditions.checkNotNull(topicArg, "topic not set")
  val qos = Preconditions.checkNotNull(qosArg, "qos not set")
  val retained = Preconditions.checkNotNull(retainedArg, "retained not set")
  val serializationSchema = Preconditions.checkNotNull(serializationSchemaArg, "deserializationSchema not set")
}

object MQTTSinkConfig {

  class Builder[IN] {
    var uri: String = _
    var clientIdPrefix: String = _
    var topic: String = _
    var qos: Int = _
    var retained: Boolean = _
    var serializationSchema: SerializationSchema[IN] = _

    def setURI(uri: String): Builder[IN] = {
      this.uri = uri
      this
    }

    def setClientIdPrefix(clientIdPrefix: String): Builder[IN] = {
      this.clientIdPrefix = clientIdPrefix
      this
    }

    def setTopic(topic: String): Builder[IN] = {
      this.topic = topic
      this
    }


    def setQos(qos: Int): Builder[IN] = {
      this.qos = qos
      this
    }

    def setRetained(retained: Boolean): Builder[IN] = {
      this.retained = retained
      this
    }

    def setSerializationSchema(serializationSchema: SerializationSchema[IN]): Builder[IN] = {
      this.serializationSchema = serializationSchema
      this
    }

    def build(): MQTTSinkConfig[IN] = {
      new MQTTSinkConfig[IN](uri, clientIdPrefix, topic, qos, retained, serializationSchema)
    }
  }

}
