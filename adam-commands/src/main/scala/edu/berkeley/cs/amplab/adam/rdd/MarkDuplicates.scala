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
import edu.berkeley.cs.amplab.adam.models.ReferencePosition

private[rdd] object MarkDuplicates {

  def apply(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    new MarkDuplicates().markDuplicates(rdd)
  }
}

// TODO: use an Int library id instead of a library string
case class MarkDuplicatesKey(library: CharSequence, fivePrimeUnclippedPos: ReferencePosition)

private[rdd] class MarkDuplicates extends Serializable with Logging {
  initLogging()

  def fivePrimeReferencePosition(record: ADAMRecord) = {
    new ReferencePosition(record.getReferenceId, record.unclipped5primeWithOrientation())
  }

  // Groups reads together by library and 5 prime position
  def duplicatesGroup(readPair: ReadPair): Option[MarkDuplicatesKey] = {

    val r1 = readPair.read1
    if (readPair.isMappedPrimaryPair) {
      val r2 = readPair.read2.get
      val read1pos = r1.unclipped5prime()
      val read2pos = r2.unclipped5prime()
      if (read1pos < read2pos) {
        Some(new MarkDuplicatesKey(r1.getRecordGroupLibrary, fivePrimeReferencePosition(r1)))
      } else {
        Some(new MarkDuplicatesKey(r1.getRecordGroupLibrary, fivePrimeReferencePosition(r2)))
      }
    } else if (r1.getReadMapped && r1.getPrimaryAlignment) {
      Some(new MarkDuplicatesKey(r1.getRecordGroupLibrary, fivePrimeReferencePosition(r1)))
    } else {
      None
    }
  }

  // This method makes no assumptions about the order of the incoming records.
  // However, it will currently return records in a different order. You should
  // do sorting AFTER marking duplicates. Maintaining ordering would cost a
  // performance hit since you would need to do a join against the original rdd.
  def markDuplicates(rdd: RDD[ADAMRecord], deleteDuplicates: Boolean = false): RDD[ADAMRecord] = {

    for ((key, readPairs) <- rdd.adamReadPairs().groupBy(duplicatesGroup);
         readPair <- {
           if (readPairs.size <= 1) {
             // No duplicate possible since we have a group of one or less
             for (readPair <- readPairs) yield {
               // Set the duplicate flag to false just in case it was incorrect on input
               readPair.setDuplicateFlag(value = false)
               readPair
             }
           } else {
             // Duplicates have been found (more than a single read pair)
             val (pairs, fragments) = readPairs.partition(_.isMappedPrimaryPair)
             val hasPairs = pairs.size > 0
             val hasFrags = fragments.size > 0

             if (hasPairs) {

               val processedFrags =
                 for (frag <- fragments) yield {
                   // All fragments mixed with pairs are marked as duplicates
                   frag.setDuplicateFlag(!frag.read2.isDefined)
                   frag
                 }

               def getSecondFivePrimePosition(pair: ReadPair): ReferencePosition = {
                 val read1pos = pair.read1.unclipped5prime()
                 val read2pos = pair.read2.get.unclipped5prime()
                 if (read1pos < read2pos) {
                   fivePrimeReferencePosition(pair.read2.get)
                 } else {
                   fivePrimeReferencePosition(pair.read1)
                 }
               }

               val processedPairs =
                 for ((read2pos, read2posGroups) <- pairs.groupBy(p => getSecondFivePrimePosition(p));
                      (pair, i) <- read2posGroups.sortBy(_.score)(Ordering[Int].reverse).zipWithIndex) yield {
                   pair.setDuplicateFlag(i != 0)
                   pair
                 }

               // Return all process fragments and pairs
               processedFrags ++ processedPairs

             } else if (hasFrags) {
               // Only have frags with no pairs... keep the highest scoring frag and mark the rest as dups
               for ((frags, i) <- fragments.sortBy(_.score)(Ordering[Int].reverse).zipWithIndex) yield {
                 frags.setDuplicateFlag(i != 0)
                 frags
               }
             } else {
               Seq.empty
             }
           }
         };
         read <- Some(readPair.read1) ++ readPair.read2) yield read
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

