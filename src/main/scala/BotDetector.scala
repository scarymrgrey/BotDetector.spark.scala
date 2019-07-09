
import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{count, window, _}
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
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.log4j.{Level, Logger}

case class Window(start: Timestamp, end: Timestamp)

case class UserAction(unix_time: Timestamp, category_id: String, ip: String, `type`: String)

case class UserActionsWindow(ip: String, categories: Long, clicks: Long, views: Long, ratio: Double, total: Long, alreadyStored: Boolean)

case class UserActionAggregation(clicks: Int, views: Int, ratio: Double, requestsPerWindow: Int, totalRequests: Int)

object BotDetector {
  def main(args: Array[String]) {

    val checkpointDir = "file:///Users/kpolunin/checkpoint/chkpnt33"

    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Bot Detector")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .enableHiveSupport
      .getOrCreate()

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user_actions")
      .option("startingOffsets", "earliest")
      .load()
    import spark.implicits._

    val userSchema = new StructType()
      .add("unix_time", "Timestamp")
      .add("category_id", "String")
      .add("ip", "String")
      .add("type", "String")

    val res = stream
      .selectExpr("CAST(value AS STRING)")
      .where(from_json($"value", userSchema).isNotNull)
      .select(from_json($"value", userSchema).as[UserAction])
      .withWatermark("unix_time", "30 seconds")
      .groupBy($"ip", window($"unix_time", "10 minutes", slideDuration = "1 minute"))
      .agg(
        sum(when($"type" === "click", 1).otherwise(0)).as("clicks"),
        sum(when($"type" === "view", 1).otherwise(0)).as("views"),
        approx_count_distinct("category_id").as("categories"),
        count("ip").as("total")
      )
      .withColumn("ratio", $"clicks" / $"views")
      .toDF()
      .as("runningBots")

    val storedBots: RDD[(String, Boolean)] = spark.sparkContext.cassandraTable("botdetection", "stored_bots")
      .select("ip")
      .map(row => (row.get[String]("ip"), true))

    res
      .join(storedBots.toDF("ip", "alreadyStored").as("storedBots"),
        $"storedBots.ip" === $"runningBots.ip",
        "left")
      .select("runningBots.ip", "clicks", "views", "categories", "ratio", "total", "alreadyStored")
      .na.fill(value = false, Array("alreadyStored"))
      .na.fill(value = 0, Array("ratio"))
      .as[UserActionsWindow]
      .filter(r => r.clicks > 10 && !r.alreadyStored)
      .filter(r => r.ratio > 3 || r.total > 250 || r.categories > 10)
      .writeStream
      .foreachBatch { (batchDF, _) =>
        batchDF
          .map(z => z.ip)
          .toDF("ip")
          .write
          .cassandraFormat("stored_bots", "botdetection")
          .mode(SaveMode.Overwrite)
          .option("confirm.truncate", true)
          .save
      }.start()
      .awaitTermination()
  }
}







