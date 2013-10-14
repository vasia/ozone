/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.scala.examples.datamining

import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.common.`type`.base.PactDouble
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.pact.generic.contract.BulkIteration
import eu.stratosphere.scala.Args
import eu.stratosphere.scala.DataSource
import eu.stratosphere.scala.DataStream
import eu.stratosphere.scala.ScalaPlan
import eu.stratosphere.scala.ScalaPlanAssembler
import eu.stratosphere.scala.operators.DelimitedDataSourceFormat
import eu.stratosphere.scala.operators.DelimitedDataSinkFormat
import eu.stratosphere.scala.operators.RecordDataSinkFormat
import eu.stratosphere.scala.analysis.GlobalSchemaPrinter

object RunKMeans {
  def main(args: Array[String]) {
    val plan = KMeans.getPlan(20,
      "file:///home/aljoscha/kmeans-points",
      "file:///home/aljoscha/kmeans-clusters",
      "file:///home/aljoscha/kmeans-output")

    GlobalSchemaPrinter.printSchema(plan)
    LocalExecutor.execute(plan)

    System.exit(0)
  }
}

class KMeansDescriptor extends ScalaPlanAssembler with PlanAssemblerDescription {
  override def getDescription = "[-numIterations <int:2>] -dataPoints <file> -clusterCenters <file> -output <file>"

  override def getScalaPlan(args: Args) = KMeans.getPlan(args("numIterations", "2").toInt, args("dataPoints"), args("clusterCenters"), args("output"))
}

object KMeans {
  case class Point(x: Double, y: Double, z: Double) {
    def computeEuclidianDistance(other: Point) = other match {
      case Point(x2, y2, z2) => math.sqrt(math.pow(x - x2, 2) + math.pow(y - y2, 2) + math.pow(z - z2, 2))
    }
  }

  case class Distance(dataPoint: Point, clusterId: Int, distance: Double)
  
  def asPointSum = (pid: Int, dist: Distance) => dist.clusterId -> PointSum(1, dist.dataPoint)

//  def sumPointSums = (dataPoints: Iterator[(Int, PointSum)]) => dataPoints.reduce { (z, v) => z.copy(_2 = z._2 + v._2) }
  def sumPointSums = (dataPoints: Iterator[(Int, PointSum)]) => {
    val res = dataPoints.reduce { (z, v) => z.copy(_2 = z._2 + v._2) }
    (res._1, res._2.toPoint)
  }


  case class PointSum(count: Int, pointSum: Point) {
    def +(that: PointSum) = that match {
      case PointSum(c, Point(x, y, z)) => PointSum(count + c, Point(x + pointSum.x, y + pointSum.y, z + pointSum.z))
    }

    def toPoint() = Point(round(pointSum.x / count), round(pointSum.y / count), round(pointSum.z / count))

    // Rounding ensures that we get the same results in a multi-iteration run
    // as we do in successive single-iteration runs, since the output format
    // only contains two decimal places.
    private def round(d: Double) = math.round(d * 100.0) / 100.0;
  }

  def parseInput = (line: String) => {
    val PointInputPattern = """(\d+)\|-?(\d+\.\d+)\|-?(\d+\.\d+)\|-?(\d+\.\d+)\|""".r
    val PointInputPattern(id, x, y, z) = line
    (id.toInt, Point(x.toDouble, y.toDouble, z.toDouble))
  }

  def formatOutput = (cid: Int, p: Point) => "%d|%.2f|%.2f|%.2f|".format(cid, p.x, p.y, p.z)
  
  def computeDistance(p: (Int, Point), c: (Int, Point)) = {
    val ((pid, dataPoint), (cid, clusterPoint)) = (p, c)
    val distToCluster = dataPoint.computeEuclidianDistance(clusterPoint)

    pid -> Distance(dataPoint, cid, distToCluster)
  }

  def getPlan(numIterations: Int, dataPointInput: String, clusterInput: String, clusterOutput: String) = {
    val dataPoints = DataSource(dataPointInput, DelimitedDataSourceFormat(parseInput))
    val clusterPoints = DataSource(clusterInput, DelimitedDataSourceFormat(parseInput))
    clusterPoints.degreeOfParallelism(1)

    def computeNewCenters(centers: DataStream[(Int, Point)]) = {

      val distances = dataPoints cross centers map computeDistance
      val nearestCenters = distances groupBy { case (pid, _) => pid } combinableReduce { ds => ds.minBy(_._2.distance) } map asPointSum.tupled
      val newCenters = nearestCenters groupBy { case (cid, _) => cid } hadoopReduce sumPointSums
      // TODO change to using combinableReduce with map when the problem with iterations is fixed
//      val newCenters = nearestCenters groupBy { case (cid, _) => cid } hadoopReduce sumPointSums map { case (cid, pSum) => cid -> pSum.toPoint() }

      distances.left neglects { case (pid, _) => pid }
      distances.left preserves({ dp => dp }, { case (pid, dist) => (pid, dist.dataPoint) })
      distances.right neglects { case (cid, _) => cid }
      distances.right preserves({ case (cid, _) => cid }, { case (_, dist) => dist.clusterId })

      nearestCenters neglects { case (pid, _) => pid }

      newCenters neglects { case (cid, _) => cid }
      newCenters.preserves({ case (cid, _) => cid }, { case (cid, _) => cid })

      distances.avgBytesPerRecord(48)
      nearestCenters.avgBytesPerRecord(40)
      newCenters.avgBytesPerRecord(36)
      
      val diff = centers join newCenters where { _._1 } isEqualTo { _._1 } map { (c1, c2) => c1._2.computeEuclidianDistance(c2._2) }

      newCenters
    }

    val finalCenters = clusterPoints.iterate(numIterations, computeNewCenters)
    finalCenters.contract.asInstanceOf[BulkIteration].setMaximumNumberOfIterations(numIterations)

    val output = finalCenters.write(clusterOutput, DelimitedDataSinkFormat(formatOutput.tupled))

    new ScalaPlan(Seq(output), "KMeans Iteration (Immutable)")
  }
}