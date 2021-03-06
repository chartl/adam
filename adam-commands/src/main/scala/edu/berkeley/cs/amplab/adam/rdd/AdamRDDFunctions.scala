/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.rdd

import spark.{Logging, RDD}
import parquet.hadoop.metadata.CompressionCodecName
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.ParquetOutputFormat
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import parquet.hadoop.util.ContextUtil
import spark.SparkContext._
import org.apache.avro.specific.SpecificRecord
import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import edu.berkeley.cs.amplab.adam.commands.ParquetArgs
import edu.berkeley.cs.amplab.adam.models.{SingleReadBucket, ReferencePosition}
import java.io.File

class AdamRDDFunctions[T <% SpecificRecord : Manifest](rdd: RDD[T]) extends Serializable {

  def adamSave(filePath: String, blockSize: Int = 128 * 1024 * 1024,
               pageSize: Int = 1 * 1024 * 1024, compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
               disableDictionaryEncoding: Boolean = false): RDD[T] = {
    val job = new Job(rdd.context.hadoopConfiguration)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    AvroParquetOutputFormat.setSchema(job, manifest[T].erasure.asInstanceOf[Class[T]].newInstance().getSchema)
    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the ADAM/Parquet file
    recordToSave.saveAsNewAPIHadoopFile(filePath,
      classOf[java.lang.Void], manifest[T].erasure.asInstanceOf[Class[T]], classOf[ParquetOutputFormat[T]],
      ContextUtil.getConfiguration(job))
    // Return the origin rdd
    rdd
  }

  def adamSave(filePath: String, parquetArgs: ParquetArgs): RDD[T] = {
    adamSave(filePath, parquetArgs.blockSize, parquetArgs.pageSize,
      parquetArgs.compressionCodec, parquetArgs.disableDictionary)
  }

}

class AdamRecordRDDFunctions(rdd: RDD[ADAMRecord]) extends Serializable with Logging {
  initLogging()

  def adamSortReadsByReferencePosition(): RDD[ADAMRecord] = {
    log.info("Sorting reads by reference position")
    rdd.groupBy(ReferencePosition(_)).sortByKey().flatMap(_._2)
  }

  def adamMarkDuplicates(): RDD[ADAMRecord] = {
    MarkDuplicates(rdd)
  }

  def adamBQSR(dbSNP : File) : RDD[ADAMRecord] = {
    log.info("Reading the dbSNP file")
    println("Reading the dbSNP file")
    val dbsnpMap = scala.io.Source.fromFile(dbSNP).getLines().map( posLine => {
      val split = posLine.split("\t")
      val contig = split(0)
      val pos = split(1).toInt
      (contig, pos)
    }).foldLeft(Map[String,Set[Int]]())( (dbMap,pair) => {
      dbMap + (pair._1 -> (dbMap.getOrElse(pair._1,Set[Int]()) + pair._2) )
    })

    log.info("Broadcasting the dbsnp file")
    println("Broadcasting the dbsnp file")
    val broadcastDbSNP = rdd.context.broadcast(dbsnpMap)

    RecalibrateBaseQualities(rdd,broadcastDbSNP)
  }

  // Returns a tuple of (failedQualityMetrics, passedQualityMetrics)
  def adamFlagStat(): (FlagStatMetrics, FlagStatMetrics) = {
    FlagStat(rdd)
  }

  /**
   * Groups all reads by record group and read name
   * @return SingleReadBuckets with primary, secondary and unmapped reads
   */
  def adamSingleReadBuckets(): RDD[SingleReadBucket] = {
    SingleReadBucket(rdd)
  }
}

class AdamPileupRDDFunctions(rdd: RDD[ADAMPileup]) extends Serializable {

  def adamAggregatePileups(): RDD[ADAMPileup] = {
    val helper = new PileupAggregator
    helper.aggregate(rdd)
  }

}
