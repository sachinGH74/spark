package org.apache.spark.streaming.kinesis

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{BlockRDD, BlockRDDPartition}
import org.apache.spark.storage.BlockId

private[kinesis]
case class SequenceNumberRange(
    streamName: String, shardId: String, fromSeqNumber: String, toSeqNumber: String)

private[kinesis]
case class SequenceNumberRanges(ranges: Array[SequenceNumberRange]) {
  def isEmpty(): Boolean = ranges.isEmpty
  def nonEmpty(): Boolean = ranges.nonEmpty
}

private[kinesis]
object SequenceNumberRanges {

  def apply(range: SequenceNumberRange): SequenceNumberRanges = {
    new SequenceNumberRanges(Array(range))
  }

  def apply(ranges: Seq[SequenceNumberRange]): SequenceNumberRanges = {
    new SequenceNumberRanges(ranges.toArray)
  }

  /*
  def toArray(sequenceRanges: Seq[SequenceNumberRange]): Array[SequenceNumberRanges] = {
    sequenceRanges.map { range =>
      SequenceNumberRanges(range)
    }.toArray
  }
  */
  def empty: SequenceNumberRanges = {
    new SequenceNumberRanges(Array.empty)
  }
}

private[kinesis]
class KinesisBackedBlockRDDPartition(
    idx: Int,
    blockId: BlockId,
    val seqNumberRanges: SequenceNumberRanges
  ) extends BlockRDDPartition(blockId, idx)

private[kinesis]
class KinesisBackedBlockRDD(
    sc: SparkContext,
    regionId: String,
    endpointUrl: String,
    @transient blockIds: Array[BlockId],
    @transient arrayOfseqNumberRanges: Array[SequenceNumberRanges]
  ) extends BlockRDD[Array[Byte]](sc, blockIds) {

  require(blockIds.length == arrayOfseqNumberRanges.length,
    "Number of blockIds is not equal to the number of sequence number ranges")

  override def getPartitions: Array[Partition] = {
    arrayOfseqNumberRanges.zip(blockIds).zipWithIndex.map { case ((ranges, blockId), index) =>
      new KinesisBackedBlockRDDPartition(index, blockId, ranges)
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val blockManager = SparkEnv.get.blockManager
    val partition = split.asInstanceOf[KinesisBackedBlockRDDPartition]
    val blockId = partition.blockId
    blockManager.get(blockId) match {
      case Some(block) => // Data is in Block Manager
        val iterator = block.data.asInstanceOf[Iterator[Array[Byte]]]
        logDebug(s"Read partition data of $this from block manager, block $blockId")
        iterator
      case None => // Data not found in Block Manager, grab it from Kinesis
        val kinesisProxy = new KinesisProxy(
          new DefaultAWSCredentialsProviderChain(), endpointUrl, regionId)
        partition.seqNumberRanges.ranges.iterator.flatMap { range =>
          kinesisProxy.getSequenceNumberRange(
            range.streamName, range.shardId, range.fromSeqNumber, range.toSeqNumber)
        }
    }
  }
}
