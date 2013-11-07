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
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import spark.RDD
import spark.SparkContext._
import edu.berkeley.cs.amplab.adam.models.ReferencePosition

object ReadPair {

  def apply(rdd: RDD[ADAMRecord]): RDD[ReadPair] = {
    // First we group the reads by position which is imperfect but faster than using strings as keys
    val readsGroupedByPos = (for (read <- rdd) yield (ReadPairPositions(read), read)).groupByKey()

    // Then we take each group that match position and group them accurately by recordgroup / readname
    val readGroup = (for ((pos, reads) <- readsGroupedByPos;
                          read <- reads) yield ((read.getRecordGroupId, read.getReadName), read)).groupByKey()

    // Finish by emitting each single read and read pair
    for (((group, name), reads) <- readGroup) yield {
      reads.size match {
        case 1 =>
          new ReadPair(reads(0), None)
        case 2 =>
          // Fail fast here for now...
          assert(reads.forall(_.getReadPaired), "Unpaired reads with matching readname/record group")
          val read1 = reads.find(_.getFirstOfPair)
          if (read1.isEmpty) throw new IllegalStateException("Unable to first of pair")
          val read2 = reads.find(_.getSecondOfPair)
          if (read2.isEmpty) throw new IllegalStateException("Unable to find second of pair")
          new ReadPair(read1.get, read2)
        case _ =>
          throw new IllegalStateException("Unexpected '%d' reads for pair groupBy '%s:%s'"
            .format(reads.size, group, name))
      }
    }
  }
}

case class ReadPair(read1: ADAMRecord, read2: Option[ADAMRecord]) {

  def setDuplicateFlag(value: Boolean) = {
    read1.setDuplicateRead(value)
    read2 match {
      case None =>
      case Some(read) => read.setDuplicateRead(value)
    }
    this
  }

  lazy val markDuplicatesKey: Option[MarkDuplicatesKey] = {
    def unclipped5prime(record: ADAMRecord): Long = {
      if (record.getReadNegativeStrand) record.unclippedEnd else record.unclippedStart
    }
    def unclipped5PrimeWithOrientation(record: ADAMRecord): Long = {
      if (record.getReadNegativeStrand) 0 - unclipped5prime(record) else unclipped5prime(record)
    }
    read2 match {
      case None =>
        // We only have one read in this pair
        if (read1.getReadMapped && read1.getPrimaryAlignment && !read1.getReadPaired) {
          Some(new MarkDuplicatesKey(read1.getRecordGroupLibrary,
            new ReferencePosition(read1.getReferenceId, unclipped5PrimeWithOrientation(read1))))
        } else {
          None
        }
      case Some(r2) =>
        // We have two reads in this pair
        if (read1.getPrimaryAlignment && read1.getReadMapped && r2.getReadMapped) {
          // Both reads are mapped
          val read1pos = unclipped5prime(read1)
          val read2pos = unclipped5prime(r2)
          if (read1pos < read2pos) {
            Some(new MarkDuplicatesKey(read1.getRecordGroupLibrary,
              new ReferencePosition(read1.getReferenceId, unclipped5PrimeWithOrientation(read1)),
              Some(new ReferencePosition(r2.getReferenceId, unclipped5PrimeWithOrientation(r2)))))
          } else {
            Some(new MarkDuplicatesKey(read1.getRecordGroupLibrary,
              new ReferencePosition(r2.getReferenceId, unclipped5PrimeWithOrientation(r2)),
              Some(new ReferencePosition(read1.getReferenceId, unclipped5PrimeWithOrientation(read1)))))
          }
        } else {
          if (read1.getPrimaryAlignment && read1.getReadMapped && !read1.getReadPaired) {
            Some(new MarkDuplicatesKey(read1.getRecordGroupLibrary,
              new ReferencePosition(read1.getReferenceId, unclipped5PrimeWithOrientation(read1))))
          } else if (r2.getPrimaryAlignment && r2.getReadMapped && read1.getReadPaired) {
            Some(new MarkDuplicatesKey(r2.getRecordGroupLibrary,
              new ReferencePosition(r2.getReferenceId, unclipped5PrimeWithOrientation(r2))))
          } else {
            None
          }
        }
    }
  }

  lazy val score: Int = {
    val read2score = read2 match {
      case None => 0
      case Some(read) => read.score
    }
    read1.score + read2score
  }
}

// TODO: use an Int library id instead of a library string
case class MarkDuplicatesKey(library: CharSequence, read1refPos: ReferencePosition, read2refPos: Option[ReferencePosition] = None)


// This is use for quickly (and roughly) grouping together read pairs.
// It captures the pos and orientation of both the read and its mate
// if one exists. It's an imperfect, albeit lightweight, way to group reads
// without parsing strings. Of course, you may capture more than one group
// of pairs so you MUST check the readgroup and readname afterwards.
object ReadPairPositions {

  def apply(record: ADAMRecord): ReadPairPositions = {
    def calcPosition(position: Long, isNegative: Boolean) = {
      if (isNegative) 0 - position else position
    }
    val readPos = if (record.getReadMapped) calcPosition(record.getStart, record.getReadNegativeStrand) else 0L
    val matePos = if (record.getMateMapped) calcPosition(record.getMateAlignmentStart, record.getMateNegativeStrand) else 0L
    // Flip positions so that the read and it's mate have the same
    if (readPos < matePos) {
      new ReadPairPositions(readPos, matePos)
    } else {
      new ReadPairPositions(matePos, readPos)
    }
  }

}

case class ReadPairPositions(pos1: Long, pos2: Long)

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
