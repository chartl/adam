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
package edu.berkeley.cs.amplab.adam.rich

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import net.sf.samtools.{CigarElement, CigarOperator, Cigar, TextCigarCodec}
import scala.collection.JavaConversions._

object RichAdamRecord {
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton
  val ILLUMINA_READNAME_REGEX = "[a-zA-Z0-9]+:[0-9]:([0-9]+):([0-9]+):([0-9]+).*".r

  def apply(record: ADAMRecord) = {
    new RichAdamRecord(record)
  }

  implicit def recordToRichRecord(record: ADAMRecord): RichAdamRecord = new RichAdamRecord(record)
}

class IlluminaOptics(val tile: Long, val x: Long, val y: Long) {}

class RichAdamRecord(record: ADAMRecord) {

  lazy val phredQuals = {
    record.getQual.toString.map(p => p - 33)
  }

  // Calculates the sum of the phred scores that are greater than or equal to 15
  lazy val score = phredQuals.filter(15 <=).sum

  // Parses the readname to Illumina optics information
  lazy val illuminaOptics: Option[IlluminaOptics] = {
    try {
      val RichAdamRecord.ILLUMINA_READNAME_REGEX(tile, x, y) = record.getReadName
      Some(new IlluminaOptics(tile.toInt, x.toInt, y.toInt))
    } catch {
      case e: MatchError => None
    }
  }

  lazy val samtoolsCigar: Cigar = {
    RichAdamRecord.CIGAR_CODEC.decode(record.getCigar.toString)
  }

  private def isClipped(el: CigarElement) = {
    el.getOperator == CigarOperator.SOFT_CLIP ||
      el.getOperator == CigarOperator.HARD_CLIP
  }

  // Returns the end position if the read is mapped, None otherwise
  lazy val end: Long = {
    if (!record.getReadMapped) {
      throw new IllegalStateException("Read %s is unmapped and has no end".format(record.getReadName))
    }
    samtoolsCigar.getCigarElements
      .filter(p => p.getOperator.consumesReferenceBases())
      .foldLeft(record.getStart) {
      (pos, cigarEl) => pos + cigarEl.getLength
    }
  }

  // Returns the position of the unclipped end if the read is mapped, None otherwise
  lazy val unclippedEnd: Long = {
    if (!record.getReadMapped) {
      throw new IllegalStateException("Read %s is unmapped and has no unclippedEnd".format(record.getReadName))
    }
    samtoolsCigar.getCigarElements.reverse.takeWhile(isClipped).foldLeft(end) {
      (pos, cigarEl) => pos + cigarEl.getLength
    }
  }

  // Returns the position of the unclipped start if the read is mapped, None otherwise.
  lazy val unclippedStart: Long = {
    if (!record.getReadMapped) {
      throw new IllegalStateException("Read %s is unmapped and has no unclippedStart".format(record.getReadName))
    }
    samtoolsCigar.getCigarElements.takeWhile(isClipped).foldLeft(record.getStart) {
      (pos, cigarEl) => pos - cigarEl.getLength
    }
  }

  // Return the unclipped 5 prime position.
  def unclipped5prime(withOrientation: Boolean = false): Long = {
    val fivePrime = if (record.getReadNegativeStrand) unclippedEnd else unclippedStart
    if (withOrientation && record.getReadNegativeStrand) {
      0 - fivePrime
    } else {
      fivePrime
    }
  }

  // Return the 5 prime position. Will be negative if the read is matched to the negative strand
  def unclipped5primeWithOrientation() = unclipped5prime(withOrientation = true)
}
