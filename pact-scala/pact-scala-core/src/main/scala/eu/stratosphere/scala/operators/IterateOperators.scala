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

package eu.stratosphere.scala.operators

import language.experimental.macros
import scala.reflect.macros.Context
import eu.stratosphere.scala.codegen.MacroContextHolder
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.scala.analysis.UDT
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.stubs.MapStub
import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.scala.contracts.Annotations
import eu.stratosphere.scala.analysis.UDF1
import eu.stratosphere.scala.analysis.UDTSerializer
import eu.stratosphere.nephele.configuration.Configuration
import eu.stratosphere.pact.generic.contract.BulkIteration
import eu.stratosphere.scala.analysis.UDF0
import eu.stratosphere.pact.generic.stub.AbstractStub
import eu.stratosphere.scala.BulkIterationScalaContract
import eu.stratosphere.scala.WorksetIterationScalaContract
import eu.stratosphere.scala.DataStream
import eu.stratosphere.scala.analysis.FieldSelector
import eu.stratosphere.scala.OutputHintable
import eu.stratosphere.pact.generic.contract.WorksetIteration

object IterateMacros {

  def iterateWithDelta[SolutionItem: c.WeakTypeTag, DeltaItem: c.WeakTypeTag](c: Context { type PrefixType = DataStream[SolutionItem] })(stepFunction: c.Expr[DataStream[SolutionItem] => (DataStream[SolutionItem], DataStream[DeltaItem])]): c.Expr[DataStream[SolutionItem]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtSolution, createUdtSolution) = slave.mkUdtClass[SolutionItem]
    val (udtDelta, createUdtDelta) = slave.mkUdtClass[DeltaItem]

    val contract = reify {
      val solutionUDT = c.Expr[UDT[SolutionItem]](createUdtSolution).splice
      val contract = new BulkIteration with BulkIterationScalaContract[SolutionItem] {
        val udf = new UDF0[SolutionItem](solutionUDT)
        override def getUDF = udf
        private val inputPlaceHolder2 = new BulkIteration.PartialSolutionPlaceHolder(this) with ScalaContract[SolutionItem] with Serializable {
          val udf = new UDF0[SolutionItem](solutionUDT)
          override def getUDF = udf
          
        }
        override def getPartialSolution: Contract = inputPlaceHolder2.asInstanceOf[Contract]
      }
      
      val partialSolution = new DataStream(contract.getPartialSolution().asInstanceOf[Contract with ScalaContract[SolutionItem]])

      val (output, term) = stepFunction.splice.apply(partialSolution)

      contract.setInput(c.prefix.splice.contract)
      contract.setNextPartialSolution(output.contract)

      // is currently not implemented in stratosphere
//      if (term != null) contract.setTerminationCriterion(term)

      new DataStream(contract)
    }

    val result = c.Expr[DataStream[SolutionItem]](Block(List(udtSolution, udtDelta), contract.tree))

    return result
  }
  
  def iterate[SolutionItem: c.WeakTypeTag](c: Context { type PrefixType = DataStream[SolutionItem] })(n: c.Expr[Int], stepFunction: c.Expr[DataStream[SolutionItem] => DataStream[SolutionItem]]): c.Expr[DataStream[SolutionItem]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtSolution, createUdtSolution) = slave.mkUdtClass[SolutionItem]

    val contract = reify {
      val solutionUDT = c.Expr[UDT[SolutionItem]](createUdtSolution).splice
      val contract = new BulkIteration with BulkIterationScalaContract[SolutionItem] {
        val udf = new UDF0[SolutionItem](solutionUDT)
        override def getUDF = udf
        private val inputPlaceHolder2 = new BulkIteration.PartialSolutionPlaceHolder(this) with ScalaContract[SolutionItem] with Serializable {
          val udf = new UDF0[SolutionItem](solutionUDT)
          override def getUDF = udf
          
        }
        override def getPartialSolution: Contract = inputPlaceHolder2.asInstanceOf[Contract]
      }
      
      val partialSolution = new DataStream(contract.getPartialSolution().asInstanceOf[Contract with ScalaContract[SolutionItem]])

      val output = stepFunction.splice.apply(partialSolution)

      contract.setInput(c.prefix.splice.contract)
      contract.setNextPartialSolution(output.contract)
      contract.setMaximumNumberOfIterations(n.splice)

      new DataStream(contract)
    }

    val result = c.Expr[DataStream[SolutionItem]](Block(List(udtSolution), contract.tree))

    return result
  }
}


object WorksetIterateMacros {

   
  def iterateWithWorkset[SolutionItem: c.WeakTypeTag, SolutionKey: c.WeakTypeTag, WorksetItem: c.WeakTypeTag](c: Context { type PrefixType = DataStream[SolutionItem] })(workset: c.Expr[DataStream[WorksetItem]], solutionSetKey: c.Expr[SolutionItem => SolutionKey], stepFunction: c.Expr[(DataStream[SolutionItem], DataStream[WorksetItem]) => (DataStream[SolutionItem], DataStream[WorksetItem])]): c.Expr[DataStream[SolutionItem]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtSolution, createUdtSolution) = slave.mkUdtClass[SolutionItem]
    val (udtWorkset, createUdtWorkset) = slave.mkUdtClass[WorksetItem]

    val keySelection = slave.getSelector(solutionSetKey)

    val contract = reify {
      
      val solutionUDT = c.Expr[UDT[SolutionItem]](createUdtSolution).splice
      val worksetUDT = c.Expr[UDT[WorksetItem]](createUdtWorkset).splice
      
      val keySelector = new FieldSelector(solutionUDT, keySelection.splice)
      val keyFields = keySelector.selectedFields
      val keyPositions = keyFields mapToArray { _ => -1 }

      val contract = new WorksetIteration(keyPositions) with WorksetIterationScalaContract[SolutionItem] {
        override val key = keySelector
        val udf = new UDF0[SolutionItem](solutionUDT)     
        override def getUDF = udf

        private val solutionSetPlaceHolder2 = new WorksetIteration.SolutionSetPlaceHolder(this) with ScalaContract[SolutionItem] with Serializable {
          val udf = new UDF0[SolutionItem](solutionUDT)
          override def getUDF = udf

        }
        override def getSolutionSet: Contract = solutionSetPlaceHolder2.asInstanceOf[Contract]
        
        private val worksetPlaceHolder2 = new WorksetIteration.WorksetPlaceHolder(this) with ScalaContract[WorksetItem] with Serializable {
          val udf = new UDF0[WorksetItem](worksetUDT)
          override def getUDF = udf

        }
        override def getWorkset: Contract = worksetPlaceHolder2.asInstanceOf[Contract]
      }

      val solutionInput = new DataStream(contract.getSolutionSet().asInstanceOf[Contract with ScalaContract[SolutionItem]])
      val worksetInput = new DataStream(contract.getWorkset().asInstanceOf[Contract with ScalaContract[WorksetItem]])


      contract.setInitialSolutionSet(c.prefix.splice.contract)
      contract.setInitialWorkset(workset.splice.contract)

      val (delta, nextWorkset) = stepFunction.splice.apply(solutionInput, worksetInput)
      contract.setSolutionSetDelta(delta.contract)
      contract.setNextWorkset(nextWorkset.contract)

      new DataStream(contract)
    }
    
    val result = c.Expr[DataStream[SolutionItem]](Block(List(udtSolution, udtWorkset), contract.tree))

    return result
  }
}