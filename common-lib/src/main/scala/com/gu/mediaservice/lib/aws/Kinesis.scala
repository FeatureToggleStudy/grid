package com.gu.mediaservice.lib.aws

import java.nio.ByteBuffer
import java.util.UUID

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import com.gu.mediaservice.lib.config.CommonConfig
import com.gu.mediaservice.lib.json.JsonByteArrayUtil
import com.gu.mediaservice.model.usage.UsageNotice
import net.logstash.logback.marker.{LogstashMarker, Markers}
import play.api.Logger
import play.api.libs.json.{JodaWrites, Json}

class Kinesis(config: CommonConfig, streamName: String) {

  val runLocal = true

  private val builder = AmazonKinesisClientBuilder.standard()

  lazy val client: AmazonKinesis = if(runLocal) config.withLocalSetup(builder).build() else config.withAWSCredentials(builder).build()

  def publish(message: UpdateMessage) {
    val partitionKey = UUID.randomUUID().toString

    implicit val yourJodaDateWrites = JodaWrites.JodaDateTimeWrites
    implicit val unw = Json.writes[UsageNotice]

    println(s"publish to streamName = $streamName")

    val payload = JsonByteArrayUtil.toByteArray(message, withCompression = false)

    val markers: LogstashMarker = message.toLogMarker.and(Markers.append("compressed-size", payload.length))
    Logger.info("Publishing message to kinesis")(markers)

    val request = new PutRecordRequest()
      .withStreamName(streamName)
      .withPartitionKey(partitionKey)
      .withData(ByteBuffer.wrap(payload))

    val result = client.putRecord(request)
    Logger.info(s"Published kinesis message: $result")
  }
}

