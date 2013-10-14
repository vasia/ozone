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

package eu.stratosphere.scala

import language.experimental.macros
import scala.util.DynamicVariable
import eu.stratosphere.scala.analysis._
import eu.stratosphere.scala.contracts._
import eu.stratosphere.pact.common.util.{ FieldSet => PactFieldSet }
import eu.stratosphere.pact.generic.contract.Contract
import eu.stratosphere.scala.codegen.MacroContextHolder
import scala.reflect.macros.Context

case class KeyCardinality(key: FieldSelector, isUnique: Boolean, distinctCount: Option[Long], avgNumRecords: Option[Float]) {

  private class RefreshableFieldSet extends PactFieldSet {
    def refresh(indexes: Set[Int]) = {
      this.collection.clear()
      for (index <- indexes)
        this.add(index)
    }
  }

  @transient private var pactFieldSets = collection.mutable.Map[Contract with ScalaContract[_], RefreshableFieldSet]()

  def getPactFieldSet(contract: Contract with ScalaContract[_]): PactFieldSet = {

    if (pactFieldSets == null)
      pactFieldSets = collection.mutable.Map[Contract with ScalaContract[_], RefreshableFieldSet]()

    val keyCopy = key.copy
    contract.getUDF.attachOutputsToInputs(keyCopy.inputFields)
    val keySet = keyCopy.selectedFields.toIndexSet

    val fieldSet = pactFieldSets.getOrElseUpdate(contract, new RefreshableFieldSet())
    fieldSet.refresh(keySet)
    fieldSet
  }
}

trait OutputHintable[Out] { this: DataStream[Out] =>
  def getContract = contract
  
  private var _cardinalities: List[KeyCardinality] = List[KeyCardinality]()
  
  def addCardinality(card: KeyCardinality) { _cardinalities = card :: _cardinalities }

  def degreeOfParallelism = contract.getDegreeOfParallelism()
  def degreeOfParallelism_=(value: Int) = contract.setDegreeOfParallelism(value)
  def degreeOfParallelism(value: Int): this.type = { contract.setDegreeOfParallelism(value); this }

  def avgBytesPerRecord = contract.getCompilerHints().getAvgBytesPerRecord()
  def avgBytesPerRecord_=(value: Float) = contract.getCompilerHints().setAvgBytesPerRecord(value)
  def avgBytesPerRecord(value: Float): this.type = { contract.getCompilerHints().setAvgBytesPerRecord(value); this }

  def avgRecordsEmittedPerCall = contract.getCompilerHints().getAvgRecordsEmittedPerStubCall()
  def avgRecordsEmittedPerCall_=(value: Float) = contract.getCompilerHints().setAvgRecordsEmittedPerStubCall(value)
  def avgRecordsEmittedPerCall(value: Float): this.type = { contract.getCompilerHints().setAvgRecordsEmittedPerStubCall(value); this }

  def uniqueKey[Key](fields: Out => Key) = macro OutputHintableMacros.uniqueKey[Out, Key]
  def uniqueKey[Key](fields: Out => Key, distinctCount: Long) = macro OutputHintableMacros.uniqueKeyWithDistinctCount[Out, Key]

  def cardinality[Key](fields: Out => Key) = macro OutputHintableMacros.cardinality[Out, Key]
  def cardinality[Key](fields: Out => Key, distinctCount: Long) = macro OutputHintableMacros.cardinalityWithDistinctCount[Out, Key]
  def cardinality[Key](fields: Out => Key, avgNumRecords: Float) = macro OutputHintableMacros.cardinalityWithAvgNumRecords[Out, Key]
  def cardinality[Key](fields: Out => Key, distinctCount: Long, avgNumRecords: Float) = macro OutputHintableMacros.cardinalityWithAll[Out, Key]

  def applyHints(contract: Contract with ScalaContract[_]): Unit = {
    val hints = contract.getCompilerHints

    if (hints.getUniqueFields != null)
      hints.getUniqueFields.clear()
    hints.getDistinctCounts.clear()
    hints.getAvgNumRecordsPerDistinctFields.clear()

    _cardinalities.foreach { card =>

      val fieldSet = card.getPactFieldSet(contract)

      if (card.isUnique) {
        hints.addUniqueField(fieldSet)
      }

      card.distinctCount.foreach(hints.setDistinctCount(fieldSet, _))
      card.avgNumRecords.foreach(hints.setAvgNumRecordsPerDistinctFields(fieldSet, _))
    }
  }
}

object OutputHintableMacros {
  
  def uniqueKey[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, true, None, None)
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
  
  def uniqueKeyWithDistinctCount[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key], distinctCount: c.Expr[Long]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, true, Some(distinctCount.splice), None)
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
  
  def cardinality[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, false, None, None)
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
  
  def cardinalityWithDistinctCount[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key], distinctCount: c.Expr[Long]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, false, Some(distinctCount.splice), None)
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
  
  def cardinalityWithAvgNumRecords[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key], avgNumRecords: c.Expr[Float]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, false, None, Some(avgNumRecords.splice))
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
  
  def cardinalityWithAll[Out: c.WeakTypeTag, Key: c.WeakTypeTag](c: Context { type PrefixType = OutputHintable[Out] })(fields: c.Expr[Out => Key], distinctCount: c.Expr[Long], avgNumRecords: c.Expr[Float]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedKeySelector = slave.getSelector(fields)

    val result = reify {
      val contract = c.prefix.splice.getContract
      val hints = contract.getCompilerHints
      
      val keySelection = generatedKeySelector.splice
      val key = new FieldSelector(c.prefix.splice.getContract.getUDF.outputUDT, keySelection)
      val card = KeyCardinality(key, false, Some(distinctCount.splice), Some(avgNumRecords.splice))
      
      c.prefix.splice.addCardinality(card)
    }
    return result
  }
}

trait InputHintable[In, Out] { this: DataStream[Out] =>
  def markUnread: Int => Unit
  def markCopied: (Int, Int) => Unit
  
  def getInputUDT: UDT[In]
  def getOutputUDT: UDT[Out]

  def neglects[Fields](fields: In => Fields): Unit = macro InputHintableMacros.neglects[In, Out, Fields]
  def observes[Fields](fields: In => Fields): Unit = macro InputHintableMacros.observes[In, Out, Fields]
  def preserves[Fields](from: In => Fields, to: Out => Fields) = macro InputHintableMacros.preserves[In, Out, Fields]
}

object InputHintable {

  private val enabled = new DynamicVariable[Boolean](true)

  def withEnabled[T](isEnabled: Boolean)(thunk: => T): T = enabled.withValue(isEnabled) { thunk }
  
}

object InputHintableMacros {
  
  def neglects[In: c.WeakTypeTag, Out: c.WeakTypeTag, Fields: c.WeakTypeTag](c: Context { type PrefixType = InputHintable[In, Out] })(fields: c.Expr[In => Fields]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedFieldSelector = slave.getSelector(fields)

    val result = reify {
      val fieldSelection = generatedFieldSelector.splice
      val fieldSelector = new FieldSelector(c.prefix.splice.getInputUDT, fieldSelection)
      val unreadFields = fieldSelector.selectedFields.map(_.localPos).toSet
      unreadFields.foreach(c.prefix.splice.markUnread(_))
    }
    return result
  }
  
  def observes[In: c.WeakTypeTag, Out: c.WeakTypeTag, Fields: c.WeakTypeTag](c: Context { type PrefixType = InputHintable[In, Out] })(fields: c.Expr[In => Fields]): c.Expr[Unit] = {
    import c.universe._

    val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedFieldSelector = slave.getSelector(fields)

    val result = reify {
      val fieldSelection = generatedFieldSelector.splice
      val fieldSelector = new FieldSelector(c.prefix.splice.getInputUDT, fieldSelection)
      val fieldSet = fieldSelector.selectedFields.map(_.localPos).toSet
      val unreadFields = fieldSelector.inputFields.map(_.localPos).toSet.diff(fieldSet)
      unreadFields.foreach(c.prefix.splice.markUnread(_))
    }
    return result
  }
  
  def preserves[In: c.WeakTypeTag, Out: c.WeakTypeTag, Fields: c.WeakTypeTag](c: Context { type PrefixType = InputHintable[In, Out] })(from: c.Expr[In => Fields], to: c.Expr[Out => Fields]): c.Expr[Unit] = {
    import c.universe._

     val slave = MacroContextHolder.newMacroHelper(c)
    
    val generatedFromFieldSelector = slave.getSelector(from)
    val generatedToFieldSelector = slave.getSelector(to)

    val result = reify {
      val fromSelection = generatedFromFieldSelector.splice
      val fromSelector = new FieldSelector(c.prefix.splice.getInputUDT, fromSelection)
      val toSelection = generatedToFieldSelector.splice
      val toSelector = new FieldSelector(c.prefix.splice.getOutputUDT, toSelection)
      val pairs = fromSelector.selectedFields.map(_.localPos).zip(toSelector.selectedFields.map(_.localPos))
      pairs.foreach(c.prefix.splice.markCopied.tupled)
    }
    return result
  }
}

trait OneInputHintable[In, Out] extends InputHintable[In, Out] with OutputHintable[Out] { this: DataStream[Out] =>
	override def markUnread = contract.getUDF.asInstanceOf[UDF1[In, Out]].markInputFieldUnread _ 
	override def markCopied = contract.getUDF.asInstanceOf[UDF1[In, Out]].markFieldCopied _ 
	
	override def getInputUDT = contract.getUDF.asInstanceOf[UDF1[In, Out]].inputUDT
	override def getOutputUDT = contract.getUDF.asInstanceOf[UDF1[In, Out]].outputUDT
}

trait TwoInputHintable[LeftIn, RightIn, Out] extends OutputHintable[Out] { this: DataStream[Out] =>
  val left = new DataStream[Out](contract) with OneInputHintable[LeftIn, Out] {
	override def markUnread = { pos: Int => contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].markInputFieldUnread(Left(pos))}
	override def markCopied = { (from: Int, to: Int) => contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].markFieldCopied(Left(from), to)} 
	override def getInputUDT = contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].leftInputUDT
	override def getOutputUDT = contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].outputUDT
  }
  
  val right = new DataStream[Out](contract) with OneInputHintable[RightIn, Out] {
	override def markUnread = { pos: Int => contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].markInputFieldUnread(Right(pos))}
	override def markCopied = { (from: Int, to: Int) => contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].markFieldCopied(Right(from), to)} 
	override def getInputUDT = contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].rightInputUDT
	override def getOutputUDT = contract.getUDF.asInstanceOf[UDF2[LeftIn, RightIn, Out]].outputUDT
  }
}
