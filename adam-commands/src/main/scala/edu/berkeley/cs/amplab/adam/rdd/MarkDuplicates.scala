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

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import spark.{Logging, RDD}
import spark.SparkContext._
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Output, Input}

private[rdd] object MarkDuplicates {

  def apply(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    new MarkDuplicates().markDuplicates(rdd)
  }
}

private[rdd] class MarkDuplicates extends Serializable with Logging {
  initLogging()

  // This tuple captures the pos and orientation of both the read and its mate
  // if one exists. It's an imperfect, albeit lightweight, way to group reads
  // without parsing strings. Of course, you may capture more than one group
  // of pairs so you will need to check the readgroup and readname afterwards.
  def createPositionTuple(record: ADAMRecord): (Long, Long) = {
    def calcPos(pos: Long, isNegative: Boolean) = {
      if (isNegative) 0 - pos else pos
    }
    if (record.getReadMapped) {
      if (record.getMateMapped) {
        val recordPos = calcPos(record.getStart, record.getReadNegativeStrand)
        val matePos = calcPos(record.getMateAlignmentStart, record.getMateNegativeStrand)
        if (record.getStart < record.getMateAlignmentStart) {
          (recordPos, matePos)
        } else {
          (matePos, recordPos)
        }
      } else {
        // Single read
        (calcPos(record.getStart, record.getReadNegativeStrand), 0)
      }
    } else {
      (0, 0)
    }
  }

  // This method makes no assumptions about the order of the incoming records.
  // However, it will currently return records in a different order. You should
  // do sorting AFTER marking duplicates. Maintaining ordering would cost a
  // performance hit since you would need to do a join against the original rdd.
  def markDuplicates(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {

    // Group the paired reads
    val pairGroups = rdd.map(record => (createPositionTuple(record), record)).groupByKey()

    val unclippedPositions: RDD[ADAMRecord] = pairGroups.flatMap {
      p =>
      // This groupBy will sort out any position tuple collisions
        val groupedReads = p._2.groupBy(record => (record.getRecordGroupId + ":" + record.getReadName).reverse)
        // Walk the grouped reads and emit the unclipped positions
        val foo = groupedReads.flatMap {
          p =>
            val key: String = p._1
            val reads: Seq[ADAMRecord] = p._2
            reads.size match {
              case 1 =>
                val read1: ADAMRecord = reads(0)
                Seq(read1)
              case 2 =>
                /*
                val read1 = reads.find(_.getFirstOfPair)
                val read2 = reads.find(_.getSecondOfPair)*/
                Seq(reads(0), reads(1))
              case _ =>
                throw new IllegalStateException("Found unexpected number '%d' of reads for pair groupBy '%s'".format(reads.size, key.reverse))
            }
        }
        foo
    }

    unclippedPositions
  }

}
