package org.apache.spark.streaming.kinesis

import java.nio.ByteBuffer

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.spark.storage.{StorageLevel, BlockManager, BlockId, StreamBlockId}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class KinesisBackedBlockRDDSuite extends FunSuite with BeforeAndAfterAll {

  private val streamName = "TDTest"
  private val regionId = "us-east-1"
  private val endPointUrl = "https://kinesis.us-east-1.amazonaws.com"
  private val proxy = new KinesisProxy(
    new DefaultAWSCredentialsProviderChain(), endPointUrl, regionId)
  private val shardInfo = proxy.getAllShards(streamName)
  require(shardInfo.size > 1, "Need a stream with more than 1 shard")

  private val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
  kinesisClient.setEndpoint(endPointUrl)

  private val testData = 1 to 8

  private var sc: SparkContext = null
  private var blockManager: BlockManager = null
  private var shardIdToDataAndSeqNumbers: Map[String, Seq[(Int, String)]] = null
  private var shardIds: Seq[String] = null
  private var shardIdToData: Map[String, Seq[Int]] = null
  private var shardIdToSeqNumbers: Map[String, Seq[String]] = null
  private var shardIdToRange: Map[String, SequenceNumberRange] = null
  private var allRanges: Seq[SequenceNumberRange] = null

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("KinesisBackedBlockRDDSuite")
    sc = new SparkContext(conf)
    blockManager = sc.env.blockManager

    shardIdToDataAndSeqNumbers = pushData(testData)
    assert(shardIdToDataAndSeqNumbers.size > 1, "Need at least 2 shards to test")
    shardIds = shardIdToDataAndSeqNumbers.keySet.toSeq
    shardIdToData = shardIdToDataAndSeqNumbers.mapValues { _.map { _._1 }}
    shardIdToSeqNumbers = shardIdToDataAndSeqNumbers.mapValues { _.map { _._2 }}
    shardIdToRange = shardIdToSeqNumbers.map { case (shardId, seqNumbers) =>
      (shardId, SequenceNumberRange(streamName, shardId, seqNumbers.head, seqNumbers.last))
    }.toMap
    allRanges = shardIdToRange.values.toSeq
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("Basic reading from Kinesis") {
    // Verify all data using multiple ranges in a single RDD partition
    val receivedData1 = new KinesisBackedBlockRDD(sc, regionId, endPointUrl,
      fakeBlockIds(1),
      Array(SequenceNumberRanges(allRanges.toArray))
    ).map { bytes => new String(bytes).toInt }.collect()
    assert(receivedData1.toSet === testData.toSet)

    // Verify all data using one range in each of the multiple RDD partitions
    val receivedData2 = new KinesisBackedBlockRDD(sc, regionId, endPointUrl,
      fakeBlockIds(allRanges.size),
      allRanges.map { range => SequenceNumberRanges(Array(range)) }.toArray
    ).map { bytes => new String(bytes).toInt }.collect()
    assert(receivedData2.toSet === testData.toSet)

    // Verify ordering within each partition
    val receivedData3 = new KinesisBackedBlockRDD(sc, regionId, endPointUrl,
      fakeBlockIds(allRanges.size),
      allRanges.map { range => SequenceNumberRanges(Array(range)) }.toArray
    ).map { bytes => new String(bytes).toInt }.collectPartitions()
    assert(receivedData3.length === allRanges.size)
    for (i <- 0 until allRanges.size) {
      assert(receivedData3(i).toSeq === shardIdToData(allRanges(i).shardId))
    }
  }

  test("Read data available in both block manager and Kinesis") {
    assert(shardIds.size === 2)
    testRDD(2, 2)
  }

  test("Read data available only in block manager, not in Kinesis") {
    assert(shardIds.size === 2)
    testRDD(2, 0)
  }

  test("Read data available only in Kinesis, not in block manager") {
    assert(shardIds.size === 2)
    testRDD(0, 2)
  }

  test("Read data available partially in block manager, rest in Kinesis") {
    assert(shardIds.size === 2)
    testRDD(1, 1)
  }

  private def testRDD(numPartitionsInBlockManager: Int, numPartitionsInKinesis: Int): Unit = {
    require(shardIds.size > 1, "Need at least 2 shards to test")
    assert(numPartitionsInBlockManager <= shardIds.size ,
      "Number of partitions in BlockManager cannot be more than the Kinesis test shards available"
    )
    assert(numPartitionsInKinesis <= shardIds.size ,
      "Number of partitions in Kinesisr cannot be more than the Kinesis test shards available"
    )

    val numPartitions = shardIds.size

    // Put necessary blocks in the block manager
    val blockIds = fakeBlockIds(numPartitions)
    blockIds.foreach(blockManager.removeBlock(_))
    (0 until numPartitionsInBlockManager).foreach { i =>
      val blockData = shardIdToData(shardIds(i)).iterator.map { _.toString.getBytes() }
      blockManager.putIterator(blockIds(i), blockData, StorageLevel.MEMORY_ONLY)
    }

    // Create the necessary ranges to use in the RDD
    val fakeRanges = Array.fill(numPartitions - numPartitionsInKinesis)(SequenceNumberRanges.empty)
    val realRanges = Array.tabulate(numPartitionsInKinesis) { i =>
      val range = shardIdToRange(shardIds(i + (numPartitions - numPartitionsInKinesis)))
      SequenceNumberRanges(Array(range))
    }
    val ranges = (fakeRanges ++ realRanges)


    // Make sure that the left `numPartitionsInBM` blocks are in block manager, and others are not
    require(
      blockIds.take(numPartitionsInBlockManager).forall(blockManager.get(_).nonEmpty),
      "Expected blocks not in BlockManager"
    )

    require(
      blockIds.drop(numPartitionsInBlockManager).forall(blockManager.get(_).isEmpty),
      "Unexpected blocks in BlockManager"
    )

    // Make sure that the right sequence `numPartitionsInKinesis` are configured, and others are not
    require(
      ranges.takeRight(numPartitionsInKinesis).forall(_.nonEmpty),
      "Incorrect configuration of RDD, expected ranges not set: "
    )

    require(
      ranges.dropRight(numPartitionsInKinesis).forall(_.isEmpty),
      "Incorrect configuration of RDD, unexpected ranges set"
    )

    val rdd = new KinesisBackedBlockRDD(sc, regionId, endPointUrl, blockIds, ranges)
    val collectedData = rdd.map { bytes =>
      new String(bytes).toInt
    }.collect()
    assert(collectedData.toSet === testData.toSet)
  }

  /**
   * Push data to Kinesis stream and return a map of
   *  shardId -> seq of (data, seq number) pushed to corresponding shard
   */
  private def pushData(testData: Seq[Int]): Map[String, Seq[(Int, String)]] = {
    val shardIdToSeqNumbers = new mutable.HashMap[String, ArrayBuffer[(Int, String)]]()

    testData.foreach { num =>
      val str = num.toString
      val putRecordRequest = new PutRecordRequest().withStreamName(streamName)
        .withData(ByteBuffer.wrap(str.getBytes()))
        .withPartitionKey(str)

      val putRecordResult = kinesisClient.putRecord(putRecordRequest)
      val shardId = putRecordResult.getShardId
      val seqNumber = putRecordResult.getSequenceNumber()
      val sentSeqNumbers = shardIdToSeqNumbers.getOrElseUpdate(shardId,
        new ArrayBuffer[(Int, String)]())
      sentSeqNumbers += ((num, seqNumber))
    }
    shardIdToSeqNumbers.toMap
  }

  /** Generate fake block ids */
  private def fakeBlockIds(num: Int): Array[BlockId] = {
    Array.tabulate(num) { i => new StreamBlockId(0, i) }
  }
}
