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
import eu.stratosphere.scala.ScalaContract
import eu.stratosphere.pact.common.contract.MapContract
import eu.stratosphere.scala.analysis.UDT
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.stubs.MapStub
import eu.stratosphere.pact.common.stubs.Collector
import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.scala.contracts.Annotations
import eu.stratosphere.pact.common.contract.ReduceContract
import eu.stratosphere.pact.common.stubs.ReduceStub
import eu.stratosphere.scala.analysis.UDTSerializer
import eu.stratosphere.scala.analysis.UDF1
import eu.stratosphere.scala.operators.stubs.DeserializingIterator
import eu.stratosphere.nephele.configuration.Configuration
import java.util.{ Iterator => JIterator }
import eu.stratosphere.scala.analysis.FieldSelector
import eu.stratosphere.scala.OneInputKeyedScalaContract
import eu.stratosphere.pact.common.contract.CrossContract
import eu.stratosphere.scala.analysis.UDF2
import eu.stratosphere.pact.common.contract.MatchContract
import eu.stratosphere.scala.TwoInputKeyedScalaContract
import eu.stratosphere.pact.common.stubs.MatchStub
import eu.stratosphere.scala.codegen.MacroContextHolder
import eu.stratosphere.scala.DataSet
import eu.stratosphere.pact.generic.contract.UserCodeObjectWrapper
import eu.stratosphere.scala.TwoInputHintable
import eu.stratosphere.scala.codegen.Util

class JoinDataSet[LeftIn, RightIn](val leftInput: DataSet[LeftIn], val rightInput: DataSet[RightIn]) {
  def where[Key](keyFun: LeftIn => Key) = macro JoinMacros.whereImpl[LeftIn, RightIn, Key]
}

class JoinDataSetWithWhere[LeftIn, RightIn, Key](val leftKey: List[Int], val leftInput: DataSet[LeftIn], val rightInput: DataSet[RightIn]) {
  def isEqualTo[Key](keyFun: RightIn => Key) = macro JoinMacros.isEqualToImpl[LeftIn, RightIn, Key]
}

class JoinDataSetWithWhereAndEqual[LeftIn, RightIn](val leftKey: List[Int], val rightKey: List[Int], val leftInput: DataSet[LeftIn], val rightInput: DataSet[RightIn]) {
  def map[Out](fun: (LeftIn, RightIn) => Out): DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out] = macro JoinMacros.map[LeftIn, RightIn, Out]
  def flatMap[Out](fun: (LeftIn, RightIn) => Iterator[Out]): DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out] = macro JoinMacros.flatMap[LeftIn, RightIn, Out]
  def filter(fun: (LeftIn, RightIn) => Boolean): DataSet[(LeftIn, RightIn)] with TwoInputHintable[LeftIn, RightIn, (LeftIn, RightIn)] = macro JoinMacros.filter[LeftIn, RightIn]
}

class NoKeyMatchBuilder(s: MatchStub) extends MatchContract.Builder(new UserCodeObjectWrapper(s))

object JoinMacros {
  
  def whereImpl[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = JoinDataSet[LeftIn, RightIn] })(keyFun: c.Expr[LeftIn => Key]): c.Expr[JoinDataSetWithWhere[LeftIn, RightIn, Key]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val keySelection = slave.getSelector(keyFun)

    val helper = reify {
      val helper = c.prefix.splice
      new JoinDataSetWithWhere[LeftIn, RightIn, Key](keySelection.splice, helper.leftInput, helper.rightInput)
    }

    return helper
  }
  
  def isEqualToImpl[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = JoinDataSetWithWhere[LeftIn, RightIn, Key] })(keyFun: c.Expr[RightIn => Key]): c.Expr[JoinDataSetWithWhereAndEqual[LeftIn, RightIn]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val keySelection = slave.getSelector(keyFun)

    val helper = reify {
      val helper = c.prefix.splice
      new JoinDataSetWithWhereAndEqual[LeftIn, RightIn](helper.leftKey, keySelection.splice, helper.leftInput, helper.rightInput)
    }

    return helper
  }

  def map[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = JoinDataSetWithWhereAndEqual[LeftIn, RightIn] })(fun: c.Expr[(LeftIn, RightIn) => Out]): c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val contract = reify {
      val helper: JoinDataSetWithWhereAndEqual[LeftIn, RightIn] = c.prefix.splice
      val leftKeySelection = helper.leftKey
      val rightKeySelection = helper.rightKey

      val generatedStub = new MatchStub with Serializable {
        val leftInputUDT = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
        val rightInputUDT = c.Expr[UDT[RightIn]](createUdtRightIn).splice
        val outputUDT = c.Expr[UDT[Out]](createUdtOut).splice
        val leftKeySelector = new FieldSelector(leftInputUDT, leftKeySelection)
        val rightKeySelector = new FieldSelector(rightInputUDT, rightKeySelection)
        val udf: UDF2[LeftIn, RightIn, Out] = new UDF2(leftInputUDT, rightInputUDT, outputUDT)

        private var leftDeserializer: UDTSerializer[LeftIn] = _
        private var leftDiscard: Array[Int] = _
        private var leftForwardFrom: Array[Int] = _
        private var leftForwardTo: Array[Int] = _
        private var rightDeserializer: UDTSerializer[RightIn] = _
        private var rightForwardFrom: Array[Int] = _
        private var rightForwardTo: Array[Int] = _
        private var serializer: UDTSerializer[Out] = _
        private var outputLength: Int = _

        override def open(config: Configuration) = {
          super.open(config)

          this.leftDeserializer = udf.getLeftInputDeserializer
          this.leftDiscard = udf.getLeftDiscardIndexArray.filter(_ < udf.getOutputLength)
          this.leftForwardFrom = udf.getLeftForwardIndexArrayFrom
          this.leftForwardTo = udf.getLeftForwardIndexArrayTo
          this.rightDeserializer = udf.getRightInputDeserializer
          this.rightForwardFrom = udf.getRightForwardIndexArrayFrom
          this.rightForwardTo = udf.getRightForwardIndexArrayTo
          this.serializer = udf.getOutputSerializer
          this.outputLength = udf.getOutputLength
        }

        override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) = {

          val left = leftDeserializer.deserializeRecyclingOn(leftRecord)
          val right = rightDeserializer.deserializeRecyclingOn(rightRecord)
          val output = fun.splice.apply(left, right)

          leftRecord.setNumFields(outputLength)
          for (field <- leftDiscard)
            leftRecord.setNull(field)

          leftRecord.copyFrom(rightRecord, rightForwardFrom, rightForwardTo)
          leftRecord.copyFrom(leftRecord, leftForwardFrom, leftForwardTo)

          serializer.serialize(output, leftRecord)
          out.collect(leftRecord)
        }

      }
      
      val builder = new NoKeyMatchBuilder(generatedStub).input1(helper.leftInput.contract).input2(helper.rightInput.contract)

      val leftKeyPositions = generatedStub.leftKeySelector.selectedFields.toIndexArray
      val rightKeyPositions = generatedStub.leftKeySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.leftInputUDT.getKeySet(leftKeyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), leftKeyPositions(i), rightKeyPositions(i)) }

      
      
      val ret = new MatchContract(builder) with TwoInputKeyedScalaContract[LeftIn, RightIn, Out] {
        override val leftKey: FieldSelector = generatedStub.leftKeySelector
        override val rightKey: FieldSelector = generatedStub.rightKeySelector
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFieldsFirst(
            Util.filterNonForwards(getUDF.getLeftForwardIndexArrayFrom, getUDF.getLeftForwardIndexArrayTo)),
          Annotations.getConstantFieldsSecond(
            Util.filterNonForwards(getUDF.getRightForwardIndexArrayFrom, getUDF.getRightForwardIndexArrayTo)))
      }
      new DataSet[Out](ret) with TwoInputHintable[LeftIn, RightIn, Out] {}
    }
    
    val result = c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]](Block(List(udtLeftIn, udtRightIn, udtOut), contract.tree))
    
    return result
  }
  
  def flatMap[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag, Out: c.WeakTypeTag](c: Context { type PrefixType = JoinDataSetWithWhereAndEqual[LeftIn, RightIn] })(fun: c.Expr[(LeftIn, RightIn) => Iterator[Out]]): c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val contract = reify {
      val helper: JoinDataSetWithWhereAndEqual[LeftIn, RightIn] = c.prefix.splice
      val leftKeySelection = helper.leftKey
      val rightKeySelection = helper.rightKey

      val generatedStub = new MatchStub with Serializable {
        val leftInputUDT = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
        val rightInputUDT = c.Expr[UDT[RightIn]](createUdtRightIn).splice
        val outputUDT = c.Expr[UDT[Out]](createUdtOut).splice
        val leftKeySelector = new FieldSelector(leftInputUDT, leftKeySelection)
        val rightKeySelector = new FieldSelector(rightInputUDT, rightKeySelection)
        val udf: UDF2[LeftIn, RightIn, Out] = new UDF2(leftInputUDT, rightInputUDT, outputUDT)

        private var leftDeserializer: UDTSerializer[LeftIn] = _
        private var leftDiscard: Array[Int] = _
        private var leftForwardFrom: Array[Int] = _
        private var leftForwardTo: Array[Int] = _
        private var rightDeserializer: UDTSerializer[RightIn] = _
        private var rightForwardFrom: Array[Int] = _
        private var rightForwardTo: Array[Int] = _
        private var serializer: UDTSerializer[Out] = _
        private var outputLength: Int = _

        override def open(config: Configuration) = {
          super.open(config)

          this.leftDeserializer = udf.getLeftInputDeserializer
          this.leftDiscard = udf.getLeftDiscardIndexArray.filter(_ < udf.getOutputLength)
          this.leftForwardFrom = udf.getLeftForwardIndexArrayFrom
          this.leftForwardTo = udf.getLeftForwardIndexArrayTo
          this.rightDeserializer = udf.getRightInputDeserializer
          this.rightForwardFrom = udf.getRightForwardIndexArrayFrom
          this.rightForwardTo = udf.getRightForwardIndexArrayTo
          this.serializer = udf.getOutputSerializer
          this.outputLength = udf.getOutputLength
        }

        override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) = {

          val left = leftDeserializer.deserializeRecyclingOn(leftRecord)
          val right = rightDeserializer.deserializeRecyclingOn(rightRecord)
          val output = fun.splice.apply(left, right)

          if (output.nonEmpty) {

            leftRecord.setNumFields(outputLength)

            for (field <- leftDiscard)
              leftRecord.setNull(field)

            leftRecord.copyFrom(rightRecord, rightForwardFrom, rightForwardTo)
            leftRecord.copyFrom(leftRecord, leftForwardFrom, leftForwardTo)

            for (item <- output) {
              serializer.serialize(item, leftRecord)
              out.collect(leftRecord)
            }
          }
        }

      }
      
      val builder = new NoKeyMatchBuilder(generatedStub).input1(helper.leftInput.contract).input2(helper.rightInput.contract)

      val leftKeyPositions = generatedStub.leftKeySelector.selectedFields.toIndexArray
      val rightKeyPositions = generatedStub.leftKeySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.leftInputUDT.getKeySet(leftKeyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), leftKeyPositions(i), rightKeyPositions(i)) }
      
      
      val ret = new MatchContract(builder) with TwoInputKeyedScalaContract[LeftIn, RightIn, Out] {
        override val leftKey: FieldSelector = generatedStub.leftKeySelector
        override val rightKey: FieldSelector = generatedStub.rightKeySelector
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFieldsFirst(
            Util.filterNonForwards(getUDF.getLeftForwardIndexArrayFrom, getUDF.getLeftForwardIndexArrayTo)),
          Annotations.getConstantFieldsSecond(
            Util.filterNonForwards(getUDF.getRightForwardIndexArrayFrom, getUDF.getRightForwardIndexArrayTo)))
      }
      new DataSet[Out](ret) with TwoInputHintable[LeftIn, RightIn, Out] {}
    }

    val result = c.Expr[DataSet[Out] with TwoInputHintable[LeftIn, RightIn, Out]](Block(List(udtLeftIn, udtRightIn, udtOut), contract.tree))
    
    return result
  }
  
  def filter[LeftIn: c.WeakTypeTag, RightIn: c.WeakTypeTag](c: Context { type PrefixType = JoinDataSetWithWhereAndEqual[LeftIn, RightIn] })(fun: c.Expr[(LeftIn, RightIn) => Boolean]): c.Expr[DataSet[(LeftIn, RightIn)] with TwoInputHintable[LeftIn, RightIn, (LeftIn, RightIn)]] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtLeftIn, createUdtLeftIn) = slave.mkUdtClass[LeftIn]
    val (udtRightIn, createUdtRightIn) = slave.mkUdtClass[RightIn]
    val (udtOut, createUdtOut) = slave.mkUdtClass[(LeftIn, RightIn)]
    
    val contract = reify {
      val helper: JoinDataSetWithWhereAndEqual[LeftIn, RightIn] = c.prefix.splice
      val leftKeySelection = helper.leftKey
      val rightKeySelection = helper.rightKey

      val generatedStub = new MatchStub with Serializable {
        val leftInputUDT = c.Expr[UDT[LeftIn]](createUdtLeftIn).splice
        val rightInputUDT = c.Expr[UDT[RightIn]](createUdtRightIn).splice
        val outputUDT = c.Expr[UDT[(LeftIn, RightIn)]](createUdtOut).splice
        val leftKeySelector = new FieldSelector(leftInputUDT, leftKeySelection)
        val rightKeySelector = new FieldSelector(rightInputUDT, rightKeySelection)
        val udf: UDF2[LeftIn, RightIn, (LeftIn, RightIn)] = new UDF2(leftInputUDT, rightInputUDT, outputUDT)

        private var leftDeserializer: UDTSerializer[LeftIn] = _
        private var leftDiscard: Array[Int] = _
        private var rightDeserializer: UDTSerializer[RightIn] = _
        private var rightForwardFrom: Array[Int] = _
        private var rightForwardTo: Array[Int] = _
        private var serializer: UDTSerializer[(LeftIn, RightIn)] = _
        private var outputLength: Int = _

        override def open(config: Configuration) = {
          super.open(config)

          this.leftDeserializer = udf.getLeftInputDeserializer
          this.leftDiscard = udf.getLeftDiscardIndexArray.filter(_ < udf.getOutputLength)
          this.rightDeserializer = udf.getRightInputDeserializer
          this.rightForwardFrom = udf.getRightForwardIndexArrayFrom
          this.rightForwardTo = udf.getRightForwardIndexArrayTo
          this.serializer = udf.getOutputSerializer
          this.outputLength = udf.getOutputLength
        }

        override def `match`(leftRecord: PactRecord, rightRecord: PactRecord, out: Collector[PactRecord]) = {
          val left = leftDeserializer.deserializeRecyclingOn(leftRecord)
          val right = rightDeserializer.deserializeRecyclingOn(rightRecord)
          if (fun.splice.apply(left, right)) {
            val output = (left, right)
            leftRecord.setNumFields(outputLength)
            serializer.serialize(output, leftRecord)
            out.collect(leftRecord)
          }
        }

      }
      
      val builder = new NoKeyMatchBuilder(generatedStub).input1(helper.leftInput.contract).input2(helper.rightInput.contract)

      val leftKeyPositions = generatedStub.leftKeySelector.selectedFields.toIndexArray
      val rightKeyPositions = generatedStub.leftKeySelector.selectedFields.toIndexArray
      val keyTypes = generatedStub.leftInputUDT.getKeySet(leftKeyPositions)
      // global indexes haven't been computed yet...
      0 until keyTypes.size foreach { i => builder.keyField(keyTypes(i), leftKeyPositions(i), rightKeyPositions(i)) }
      
      
      val ret = new MatchContract(builder) with TwoInputKeyedScalaContract[LeftIn, RightIn, (LeftIn, RightIn)] {
        override val leftKey: FieldSelector = generatedStub.leftKeySelector
        override val rightKey: FieldSelector = generatedStub.rightKeySelector
        override def getUDF = generatedStub.udf
        override def annotations = Seq(
          Annotations.getConstantFieldsFirst(
            Util.filterNonForwards(getUDF.getLeftForwardIndexArrayFrom, getUDF.getLeftForwardIndexArrayTo)),
          Annotations.getConstantFieldsSecond(
            Util.filterNonForwards(getUDF.getRightForwardIndexArrayFrom, getUDF.getRightForwardIndexArrayTo)))
      }
      new DataSet[(LeftIn, RightIn)](ret) with TwoInputHintable[LeftIn, RightIn, (LeftIn, RightIn)] {}
    }

    val result = c.Expr[DataSet[(LeftIn, RightIn)] with TwoInputHintable[LeftIn, RightIn, (LeftIn, RightIn)]](Block(List(udtLeftIn, udtRightIn, udtOut), contract.tree))
    
    return result
  }
  
}
