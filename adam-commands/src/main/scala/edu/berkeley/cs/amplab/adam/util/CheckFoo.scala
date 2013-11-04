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

package edu.berkeley.cs.amplab.adam.util

import edu.berkeley.cs.amplab.adam.commands.{SparkArgs, AdamSparkCommand, AdamCommand, AdamCommandCompanion}
import spark.{RDD, SparkContext}
import spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._

object CheckFoo extends AdamCommandCompanion {

  override def main(args: Array[String]) {
    new CheckFoo().run()
  }

  val commandName: String = "foo"
  val commandDescription: String = "foo"

  def apply(cmdLine: Array[String]): AdamCommand = new CheckFoo()
}

class Helper extends Serializable {
  def canonicalName(read: ADAMRecord): String = {
    read.getRecordGroupId + ":" + read.getReadName + "/" + (if (read.getFirstOfPair) 0 else 1)
  }

  def recordDiff(r1: ADAMRecord, r2: ADAMRecord): Boolean = {
    if (r1.equals(r2)) return true
    for (i <- 0 until ADAMRecord.SCHEMA$.getFields.size) {
      val f1 = r1.get(i)
      val f2 = r2.get(i)
      if (f1 != f2) {
        println("Mismatch field=%d %s != %s".format(i, f1, f2))
      }
    }
    false
  }
}

class CheckFoo extends AdamSparkCommand[SparkArgs] {
  protected val args = new SparkArgs {
    spark_master = "local[4]"
  }


  def run(sc: SparkContext, job: Job): Unit = {
    val adamFile: RDD[ADAMRecord] = sc.adamLoad("/workspace/bams/test/ADAM.adam")
    val picardFile: RDD[ADAMRecord] = sc.adamLoad("/workspace/bams/test/PICARD.adam")

    val helper = new Helper()
    val keyedAdamFile = adamFile.map(p => (helper.canonicalName(p), p))
    val keyedPicardFile = picardFile.map(p => (helper.canonicalName(p), p))

    val join = keyedAdamFile.cogroup(keyedPicardFile)
    assert(join.count() == keyedPicardFile.count())
    val mismatched = join.filter {
      p =>
        val key = p._1
        assert(p._2._1.size == 1)
        assert(p._2._2.size == 1)
        val adamRead = p._2._1(0)
        val picardRead = p._2._2(0)
        helper.recordDiff(adamRead, picardRead)
    }
    println("There are %d mismatched reads %d total".format(mismatched.count(), keyedPicardFile.count()))
  }

  val companion: AdamCommandCompanion = CheckFoo
}

