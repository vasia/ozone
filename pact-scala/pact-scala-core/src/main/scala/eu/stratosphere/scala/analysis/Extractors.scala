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

package eu.stratosphere.scala.analysis

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.collectionAsScalaIterable
import eu.stratosphere.scala.analysis._
import eu.stratosphere.scala.contracts._
import eu.stratosphere.pact.compiler.plan._
import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.pact.common.contract.GenericDataSink
import eu.stratosphere.pact.generic.contract.DualInputContract
import eu.stratosphere.pact.generic.contract.SingleInputContract
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.pact.common.contract.GenericDataSource
import eu.stratosphere.scala.OneInputScalaContract
import eu.stratosphere.scala.OneInputKeyedScalaContract
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.common.contract.CrossContract
import eu.stratosphere.scala.TwoInputScalaContract
import eu.stratosphere.pact.common.contract.MatchContract
import eu.stratosphere.scala.TwoInputKeyedScalaContract
import eu.stratosphere.pact.common.contract.CoGroupContract
import eu.stratosphere.pact.generic.contract.WorksetIteration
import eu.stratosphere.pact.generic.contract.BulkIteration
import eu.stratosphere.scala.WorksetIterationScalaContract
import eu.stratosphere.scala.BulkIterationScalaContract
import eu.stratosphere.scala.UnionScalaContract

object Extractors {

  object DataSinkNode {
    def unapply(node: Contract): Option[(UDF1[_, _], List[Contract])] = node match {
      case contract: GenericDataSink with ScalaContract[_] => {
        Some((contract.getUDF.asInstanceOf[UDF1[_, _]], node.asInstanceOf[GenericDataSink].getInputs().toList))
      }
      case _                               => None
    }
  }

  object DataSourceNode {
    def unapply(node: Contract): Option[(UDF0[_])] = node match {
      case contract: GenericDataSource[_] with ScalaContract[_] => Some(contract.getUDF.asInstanceOf[UDF0[_]])
      case _                                 => None
    }
  }

  object CoGroupNode {
    def unapply(node: Contract): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, List[Contract], List[Contract])] = node match {
      case contract: CoGroupContract with TwoInputKeyedScalaContract[_, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, contract.asInstanceOf[DualInputContract[_]].getFirstInputs().toList, contract.asInstanceOf[DualInputContract[_]].getSecondInputs().toList))
      case _                                       => None
    }
  }

  object CrossNode {
    def unapply(node: Contract): Option[(UDF2[_, _, _], List[Contract], List[Contract])] = node match {
      case contract: CrossContract with TwoInputScalaContract[_, _, _] => Some((contract.getUDF, contract.asInstanceOf[DualInputContract[_]].getFirstInputs().toList, contract.asInstanceOf[DualInputContract[_]].getSecondInputs().toList))
      case _                                  => None
    }
  }

  object JoinNode {
    def unapply(node: Contract): Option[(UDF2[_, _, _], FieldSelector, FieldSelector, List[Contract], List[Contract])] = node match {
      case contract: MatchContract with TwoInputKeyedScalaContract[ _, _, _] => Some((contract.getUDF, contract.leftKey, contract.rightKey, contract.asInstanceOf[DualInputContract[_]].getFirstInputs().toList, contract.asInstanceOf[DualInputContract[_]].getSecondInputs().toList))
      case _                                    => None
    }
  }

  object MapNode {
    def unapply(node: Contract): Option[(UDF1[_, _], List[Contract])] = node match {
      case contract: MapContract with OneInputScalaContract[_, _] => Some((contract.getUDF, contract.asInstanceOf[SingleInputContract[_]].getInputs().toList))
      case _                             => None
    }
  }
  
  object UnionNode {
    def unapply(node: Contract): Option[(UDF1[_, _], List[Contract])] = node match {
      case contract: MapContract with UnionScalaContract[_] => Some((contract.getUDF, contract.asInstanceOf[SingleInputContract[_]].getInputs().toList))
      case _                             => None
    }
  }

  object ReduceNode {
    def unapply(node: Contract): Option[(UDF1[_, _], FieldSelector, List[Contract])] = node match {
      case contract: ReduceContract with OneInputKeyedScalaContract[_, _] => Some((contract.getUDF, contract.key, contract.asInstanceOf[SingleInputContract[_]].getInputs().toList))
      case _                                   => None
    }
  }
 object WorksetIterationNode {
    def unapply(node: Contract): Option[(UDF0[_], FieldSelector, List[Contract], List[Contract])] = node match {
        case contract: WorksetIteration with WorksetIterationScalaContract[_] => Some((contract.getUDF, contract.key, contract.asInstanceOf[DualInputContract[_]].getFirstInputs().toList, contract.asInstanceOf[DualInputContract[_]].getSecondInputs().toList))
        case _                                  => None
      }
  }
  
  object BulkIterationNode {
    def unapply(node: Contract): Option[(UDF0[_], List[Contract])] = node match {
      case contract: BulkIteration with BulkIterationScalaContract[_] => Some((contract.getUDF, contract.asInstanceOf[SingleInputContract[_]].getInputs().toList))
      case _ => None
    }
  } 
}
