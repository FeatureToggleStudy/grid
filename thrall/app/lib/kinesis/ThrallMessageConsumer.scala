package lib.kinesis

import java.net.InetAddress
import java.util.UUID

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import lib._
import org.joda.time.DateTime
import play.api.Logger

class ThrallMessageConsumer(config: ThrallConfig,
  es: ElasticSearchVersion,
  thrallMetrics: ThrallMetrics,
  store: ThrallStore,
  metadataEditorNotifications: MetadataEditorNotifications,
  syndicationRightsOps: SyndicationRightsOps,
  from: Option[DateTime]
) extends MessageConsumerVersion {

  val workerId = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()

  private val thrallEventProcessorFactory = new IRecordProcessorFactory {
    override def createProcessor(): IRecordProcessor = {
      val consumer = new ThrallEventConsumer(es, thrallMetrics, store, metadataEditorNotifications, syndicationRightsOps)
      println("createProcessor")
      println(consumer)
      consumer
    }
  }

//  val dynamoClient = AmazonDynamoDBClientBuilder.standard()
//    .withEndpointConfiguration(new EndpointConfiguration("http://localhost:4569", config.awsRegion))
//    .build()

  private def createBuilder(cfg: KinesisClientLibConfiguration): Worker = {
    new Worker.Builder()
      .recordProcessorFactory(thrallEventProcessorFactory)
      .config(cfg)
      .metricsFactory(new NullMetricsFactory())
      .build()
  }

  private val kinesisCfg = kinesisClientLibConfig(
    kinesisAppName = config.thrallKinesisStream,
    streamName = config.thrallKinesisStream,
    from = from
  )

  private val thrallKinesisWorker = createBuilder(kinesisCfg)

  private val thrallKinesisWorkerThread = makeThread(thrallKinesisWorker)

  def start(from: Option[DateTime] = None) = {
    println("Trying to start Thrall kinesis reader")
    thrallKinesisWorkerThread.start()
    println("Thrall kinesis reader started")
  }

  private def kinesisClientLibConfig(kinesisAppName: String, streamName: String, from: Option[DateTime]): KinesisClientLibConfiguration = {
    val credentialsProvider = config.awsCredentials

    val kinesisConfig = new KinesisClientLibConfiguration(
      kinesisAppName,
      streamName,
      credentialsProvider,
      credentialsProvider,
      credentialsProvider,
      workerId
    ).withRegionName(config.awsRegion).
      withMaxRecords(100).
      withIdleMillisBetweenCalls(1000).
      withIdleTimeBetweenReadsInMillis(250)
      .withKinesisEndpoint("http://localhost:4568")
      .withDynamoDBEndpoint("http://localhost:4569")

    println(s"kinesisConfig, ${kinesisConfig.getKinesisEndpoint}")

    from.fold(
      kinesisConfig.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
    ) { f =>
      kinesisConfig.withTimestampAtInitialPositionInStream(f.toDate)
    }
  }

  private def makeThread(worker: Runnable) = new Thread(worker, s"${getClass.getSimpleName}-$workerId")

  override def isStopped: Boolean = !thrallKinesisWorkerThread.isAlive

}
