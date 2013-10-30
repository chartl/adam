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
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
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

  // This method makes no assumptions about the order of the incoming records.
  // However, it will currently return records in a different order. You should
  // do sorting AFTER marking duplicates. Maintaining ordering would cost a
  // performance hit since you would need to do a join against the original rdd.
  def markDuplicates(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {

    // Group the paired reads
    val pairGroups = rdd.map(record => (ReadPairPositions(record), record)).groupByKey()

    val unclippedPositions: RDD[ADAMRecord] = pairGroups.flatMap {
      p =>
      // This groupBy will fix any position tuple collisions for read pairs...
        val groupedReads = p._2.groupBy(p =>
          (p.getReadName.toString.reverse, p.getRecordGroupId.toString.reverse))

        // Walk the grouped reads and emit the unclipped positions
        groupedReads.flatMap {
          p =>
            val key: (CharSequence, CharSequence) = p._1
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
                throw new IllegalStateException("Found unexpected number '%d' of reads for pair groupBy '%s:%s'".format(reads.size, key._1, key._2))
            }
        }
    }

    unclippedPositions
  }

}

// This captures captures the pos and orientation of both the read and its mate
// if one exists. It's an imperfect, albeit lightweight, way to group reads
// without parsing strings. Of course, you may capture more than one group
// of pairs so you will need to check the readgroup and readname afterwards.
object ReadPairPositions {

  def apply(record: ADAMRecord): ReadPairPositions = {
    def calcPos(pos: Long, isNegative: Boolean) = {
      if (isNegative) 0 - pos else pos
    }
    if (record.getReadMapped) {
      if (record.getMateMapped) {
        val recordPos = calcPos(record.getStart, record.getReadNegativeStrand)
        val matePos = calcPos(record.getMateAlignmentStart, record.getMateNegativeStrand)
        if (record.getStart < record.getMateAlignmentStart) {
          new ReadPairPositions(recordPos, matePos)
        } else {
          new ReadPairPositions(matePos, recordPos)
        }
      } else {
        // Single read
        new ReadPairPositions(calcPos(record.getStart, record.getReadNegativeStrand), 0L)
      }
    } else {
      new ReadPairPositions(0L, 0L)
    }
  }
}

class ReadPairPositions(val pos1: Long, val pos2: Long) {

  override def hashCode = 41 * (41 + pos1.hashCode()) + pos2.hashCode()

  override def equals(other: Any) = other match {
    case that: ReadPairPositions =>
      (that canEqual this) &&
        (this.pos1 == that.pos1) && (this.pos2 == that.pos2)
    case _ =>
      false
  }

  def canEqual(other: Any) = other.isInstanceOf[ReadPairPositions]
}

class ReadPairPositionsSerializer extends Serializer[ReadPairPositions] {

  def write(kryo: Kryo, output: Output, obj: ReadPairPositions) = {
    output.writeLong(obj.pos1)
    output.writeLong(obj.pos2)
  }

  def read(kryo: Kryo, input: Input, klass: Class[ReadPairPositions]): ReadPairPositions = {
    val pos1 = input.readLong()
    val pos2 = input.readLong()
    new ReadPairPositions(pos1, pos2)
  }
}
