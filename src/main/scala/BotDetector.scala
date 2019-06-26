
import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.cassandra._
import org.apache.spark.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.Time
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.log4j.{Level, Logger}

case class Window(start: Timestamp, end: Timestamp)

case class UserAction(unix_time: Timestamp, category_id: String, ip: String, `type`: String)

case class UserActionWithWindow(unix_time: Timestamp, category_id: String, ip: String, `type`: String, window: Window)

case class UserActionAggregation(clicks: Int, views: Int, ratio: Double, requestsPerWindow: Int, totalRequests: Int)

object BotDetector {
  def main(args: Array[String]) {

    val checkpointDir = "file:///Users/kpolunin/checkpoint/chkpnt20"

    val spark = SparkSession.builder
      .master("local[3]")
      .appName("Fraud Detector")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .enableHiveSupport
      .getOrCreate()

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    //    val df = spark
    //      .readStream
    //      .format("kafka")
    //      .option("kafka.bootstrap.servers", "localhost:9092")
    //      .option("subscribe", "user_actions")
    //      .option("startingOffsets", "earliest")
    //      //.option("endingOffsets", "latest")
    //      .load()
    import spark.implicits._

    val userSchema = new StructType()
      .add("unix_time", "Timestamp")
      .add("category_id", "String")
      .add("ip", "String")
      .add("type", "String")

    val value = classOf[StringDeserializer]
    val kafkaParams = Map(
      "key.deserializer" -> value,
      "value.deserializer" -> value,
      "group.id" -> "test_cons",
      "bootstrap.servers" -> "localhost:9092",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val streamingContext2 = StreamingContext.getActiveOrCreate(checkpointDir, () => {
      val sc = new StreamingContext(spark.sparkContext, Seconds(10))
      sc.checkpoint(checkpointDir)
      KafkaUtils.createDirectStream[String, String](
        sc,
        PreferConsistent,
        Subscribe[String, String](Set("user_actions"), kafkaParams)
      )
        .transform {
          message =>
            val res = message.map(record => record.value())
              .toDF("value")
              .where(from_json($"value", userSchema).isNotNull)
              .select(from_json($"value", userSchema).as[UserAction])
              .select($"*", window($"unix_time", "10 minutes", slideDuration = "1 minute"))
              .as[UserActionWithWindow]

            res.rdd
        }
        .map(r => ((r.ip, r.window), (r.ip, r.`type`, r.category_id)))
        .updateStateByKey((r1: Seq[(String, String, String)], r2: Option[UserActionAggregation]) => {
          val clicks = r1.count(r => r._2 == "click")
          val views = r1.count(r => r._2 == "view")
          r2 match {
            case Some(oldData) => {
              val aggClicks = oldData.clicks + clicks
              val aggViews = oldData.views + views
              Some(UserActionAggregation(aggClicks, aggViews, if (aggViews != 0) aggClicks / aggViews else 0, r1.length, r1.length + oldData.totalRequests))
            }
            case None => Some(UserActionAggregation(clicks, views, if (views != 0) clicks / views else 0, r1.length, r1.length))
          }
        })
        .filter(r => r._2.requestsPerWindow > 200)
        .print

      sc
    })

    streamingContext2.start
    streamingContext2.awaitTermination
  }
}







