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
  def markDuplicates(rdd: RDD[ADAMRecord], deleteDuplicates: Boolean = false): RDD[ADAMRecord] = {
    for ((key, duplicateGroup) <- rdd.adamReadPairs().groupBy(_.markDuplicatesKey);
         (readPair, i) <- duplicateGroup.sortBy(_.score)(Ordering[Int].reverse).zipWithIndex;
         read <- {
           key match {
             case None =>
               readPair.setDuplicateFlag(value = false)
             case Some(duplicatesKey) =>
               duplicatesKey.read2refPos match {
                 case None =>
                   readPair.setDuplicateFlag(true)
                 case Some(read2refPos) =>
                   readPair.setDuplicateFlag(i != 0)
               }
           }
           Some(readPair.read1) ++ readPair.read2
         }) yield read
  }

  // Useful for debugging. Use coalesce(1) to ensure that only a single thread is writing
  def debugInfo(title: String, readPairs: Seq[ReadPair]) = {
    def printRead(record: ADAMRecord) = {
      println("!%s:%s start=%s mstart=%s cigar=%s primary?%s 1read=%s 2read=%s mapped?%s pair?%s dup?%s %s"
        .format(record.getRecordGroupId, record.getReadName, record.getStart,
        record.getMateAlignmentStart, record.getCigar, record.getPrimaryAlignment, record.getFirstOfPair,
        record.getSecondOfPair,
        record.getReadMapped, record.getReadPaired, record.getDuplicateRead, record.getSequence))
    }
    println("." * 20 + title + "." * 20)
    readPairs.foreach {
      p =>
        printRead(p.read1)
        if (p.read2.isDefined) {
          printRead(p.read2.get)
        } else {
          println("No mate")
        }
    }
    println("." * (40 + title.length))
  }

}

