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

import java.io.DataInput
import scala.language.experimental.macros
import scala.reflect.macros.Context
import eu.stratosphere.nephele.configuration.Configuration
import eu.stratosphere.pact.common.`type`.PactRecord
import eu.stratosphere.pact.common.`type`.base.PactDouble
import eu.stratosphere.pact.common.`type`.base.PactDouble
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactLong
import eu.stratosphere.pact.common.`type`.base.PactLong
import eu.stratosphere.pact.common.`type`.base.PactString
import eu.stratosphere.pact.common.`type`.base.PactString
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextDoubleParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextDoubleParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextIntParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextIntParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextLongParser
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextLongParser
import eu.stratosphere.pact.common.`type`.base.parser.FieldParser
import eu.stratosphere.pact.common.`type`.base.parser.VarLengthStringParser
import eu.stratosphere.pact.common.`type`.base.parser.VarLengthStringParser
import eu.stratosphere.pact.common.io.FixedLengthInputFormat
import eu.stratosphere.pact.common.io.RecordInputFormat
import eu.stratosphere.pact.common.io.TextInputFormat
import eu.stratosphere.pact.generic.io.SequentialInputFormat
import eu.stratosphere.scala.DataSourceFormat
import eu.stratosphere.scala.analysis.OutputField
import eu.stratosphere.scala.analysis.UDF0
import eu.stratosphere.scala.analysis.UDT
import eu.stratosphere.scala.operators.stubs.FixedLengthInput4sStub
import eu.stratosphere.scala.operators.stubs.BinaryInput4sStub
import eu.stratosphere.scala.operators.stubs.DelimitedInput4sStub
import eu.stratosphere.scala.operators.stubs.InputFormat4sStub
import eu.stratosphere.pact.generic.io.BinaryInputFormat
import eu.stratosphere.scala.operators.stubs.FixedLengthInput4sStub
import eu.stratosphere.pact.common.io.DelimitedInputFormat
import eu.stratosphere.scala.codegen.MacroContextHolder

object BinaryDataSourceFormat {
  // We need to do the "optional parameters" manually here (and in all other formats) because scala macros
  // do (not yet?) support optional parameters in macros.
  
  def apply[Out](readFunction: DataInput => Out): DataSourceFormat[Out] = macro implWithoutBlocksize[Out]
  def apply[Out](readFunction: DataInput => Out, blockSize: Long): DataSourceFormat[Out] = macro implWithBlocksize[Out]
  
  def implWithoutBlocksize[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[DataInput => Out]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(readFunction, reify { None })
  }
  def implWithBlocksize[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[DataInput => Out], blockSize: c.Expr[Long]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(readFunction, reify { Some(blockSize.splice) })
  }
  
  def impl[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[DataInput => Out], blockSize: c.Expr[Option[Long]]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      class GeneratedInputFormat extends BinaryInput4sStub[Out] {
        override val udt = c.Expr(createUdtOut).splice
        override val userFunction = readFunction.splice
      }
      
      new DataSourceFormat[Out](new GeneratedInputFormat, c.Expr[UDT[Out]](createUdtOut).splice) {
        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)
          blockSize.splice map { config.setLong(BinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, _) }
        }
        override def getUDF = this.format.asInstanceOf[InputFormat4sStub[Out]].udf
      }
      
    }
    
    val result = c.Expr[DataSourceFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}

// TODO find out whether this stuff ever worked ...
object SequentialDataSourceFormat {
  def apply[Out](): DataSourceFormat[Out] = macro implWithoutBlocksize[Out]
  def apply[Out](blockSize: Long): DataSourceFormat[Out] = macro implWithBlocksize[Out]
  
  def implWithoutBlocksize[Out: c.WeakTypeTag](c: Context)() : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(reify { None })
  }
  def implWithBlocksize[Out: c.WeakTypeTag](c: Context)(blockSize: c.Expr[Long]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(reify { Some(blockSize.splice) })
  }
  
  def impl[Out: c.WeakTypeTag](c: Context)(blockSize: c.Expr[Option[Long]]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      new DataSourceFormat[Out](new SequentialInputFormat[PactRecord], c.Expr[UDT[Out]](createUdtOut).splice) {
        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)
          blockSize.splice map { config.setLong(BinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, _) }
        }
        
        override val udt: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
        lazy val udf: UDF0[Out] = new UDF0(udt)
        override def getUDF = udf
      }
      
    }
    
    val result = c.Expr[DataSourceFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}

object DelimitedDataSourceFormat {
  def asReadFunction[Out](parseFunction: String => Out) = {
    (source: Array[Byte], offset: Int, numBytes: Int) => {
        parseFunction(new String(source, offset, numBytes))
    }
  }
  
  def apply[Out](readFunction: (Array[Byte], Int, Int) => Out, delimiter: Option[String]): DataSourceFormat[Out] = macro impl[Out]
  def apply[Out](parseFunction: String => Out): DataSourceFormat[Out] = macro parseFunctionImplWithoutDelim[Out]
  def apply[Out](parseFunction: String => Out, delimiter: String): DataSourceFormat[Out] = macro parseFunctionImplWithDelim[Out]
  
  def parseFunctionImplWithoutDelim[Out: c.WeakTypeTag](c: Context)(parseFunction: c.Expr[String => Out]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    val readFun = reify {
      asReadFunction[Out](parseFunction.splice)
    }
    impl(c)(readFun, reify { None })
  }
  def parseFunctionImplWithDelim[Out: c.WeakTypeTag](c: Context)(parseFunction: c.Expr[String => Out], delimiter: c.Expr[String]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    val readFun = reify {
      asReadFunction[Out](parseFunction.splice)
    }
    impl(c)(readFun, reify { Some(delimiter.splice) })
  }

  
  def impl[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[(Array[Byte], Int, Int) => Out], delimiter: c.Expr[Option[String]]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      class GeneratedInputFormat extends DelimitedInput4sStub[Out] {
        override val udt = c.Expr(createUdtOut).splice
        override val userFunction = readFunction.splice
      }
      
      new DataSourceFormat[Out](new GeneratedInputFormat, c.Expr[UDT[Out]](createUdtOut).splice) {
        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)
          delimiter.splice map { config.setString(DelimitedInputFormat.RECORD_DELIMITER, _) }
        }
        override def getUDF = this.format.asInstanceOf[InputFormat4sStub[Out]].udf
      }
      
    }
    
    val result = c.Expr[DataSourceFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}

object RecordDataSourceFormat {
  
  def apply[Out](): DataSourceFormat[Out] = macro implWithoutAll[Out]
  def apply[Out](recordDelimiter: String): DataSourceFormat[Out] = macro implWithRD[Out]
  def apply[Out](recordDelimiter: String, fieldDelimiter: String): DataSourceFormat[Out] = macro implWithRDandFD[Out]
  
  def implWithoutAll[Out: c.WeakTypeTag](c: Context)() : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(reify { None }, reify { None })
  }
  def implWithRD[Out: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[String]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(reify { Some(recordDelimiter.splice) }, reify { None })
  }
  def implWithRDandFD[Out: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[String], fieldDelimiter: c.Expr[String]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    impl(c)(reify { Some(recordDelimiter.splice) }, reify { Some(fieldDelimiter.splice) })
  }
  
  val fieldParserTypes: Map[Class[_ <: eu.stratosphere.pact.common.`type`.Value], Class[_ <: FieldParser[_]]] = Map(
    classOf[PactDouble] -> classOf[DecimalTextDoubleParser],
    classOf[PactInteger] -> classOf[DecimalTextIntParser],
    classOf[PactLong] -> classOf[DecimalTextLongParser],
    classOf[PactString] -> classOf[VarLengthStringParser]
  )
  
  def impl[Out: c.WeakTypeTag](c: Context)(recordDelimiter: c.Expr[Option[String]], fieldDelimiter: c.Expr[Option[String]]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      new DataSourceFormat[Out](new RecordInputFormat, c.Expr[UDT[Out]](createUdtOut).splice) {
        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)

          val fields: Seq[OutputField] = getUDF.outputFields.filter(_.isUsed)

          config.setInteger(RecordInputFormat.NUM_FIELDS_PARAMETER, fields.length)
          
          // for some reason we canno use fields.zipWithIndex here,
          // this works, is not as pretty (functional) though
          var index = 0;
          fields foreach { field: OutputField => 
            val fieldType  = getUDF.outputUDT.fieldTypes(field.localPos)
            val parser = fieldParserTypes(fieldType)
            config.setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX + index, parser)
            config.setInteger(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX + index, field.localPos)
            config.setInteger(RecordInputFormat.RECORD_POSITION_PARAMETER_PREFIX + index, field.globalPos.getValue)
            index = index + 1
          }

          recordDelimiter.splice map { config.setString(RecordInputFormat.RECORD_DELIMITER_PARAMETER, _) }
          fieldDelimiter.splice map { config.setString(RecordInputFormat.FIELD_DELIMITER_PARAMETER, _) }
        }
        
        override val udt: UDT[Out] = c.Expr[UDT[Out]](createUdtOut).splice
        lazy val udf: UDF0[Out] = new UDF0(udt)
        override def getUDF = udf
      }
      
    }
    
    val result = c.Expr[DataSourceFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}

object TextDataSourceFormat {
  def apply(charSetName: Option[String] = None): DataSourceFormat[String] = {

    new DataSourceFormat[String](new TextInputFormat, UDT.StringUDT) {
      override def persistConfiguration(config: Configuration) {
        super.persistConfiguration(config)

        charSetName map { config.setString(TextInputFormat.CHARSET_NAME, _) }

        config.setInteger(TextInputFormat.FIELD_POS, getUDF.outputFields(0).globalPos.getValue)
      }
      override val udt: UDT[String] = UDT.StringUDT
      lazy val udf: UDF0[String] = new UDF0(udt)
      override def getUDF = udf
    }
  }
}

object FixedLengthDataSourceFormat {
  def apply[Out](readFunction: (Array[Byte], Int) => Out, recordLength: Int): DataSourceFormat[Out] = macro impl[Out]
  
  def impl[Out: c.WeakTypeTag](c: Context)(readFunction: c.Expr[(Array[Byte], Int) => Out], recordLength: c.Expr[Int]) : c.Expr[DataSourceFormat[Out]] = {
    import c.universe._
    
    val slave = MacroContextHolder.newMacroHelper(c)
    
    val (udtOut, createUdtOut) = slave.mkUdtClass[Out]
    
    val pact4sFormat = reify {
      
      class GeneratedInputFormat extends FixedLengthInput4sStub[Out] {
        override val udt = c.Expr(createUdtOut).splice
        override val userFunction = readFunction.splice
      }
      
      new DataSourceFormat[Out](new GeneratedInputFormat, c.Expr[UDT[Out]](createUdtOut).splice) {
        override def persistConfiguration(config: Configuration) {
          super.persistConfiguration(config)
          config.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, (recordLength.splice))
        }
        override def getUDF = this.format.asInstanceOf[InputFormat4sStub[Out]].udf
      }
      
    }
    
    val result = c.Expr[DataSourceFormat[Out]](Block(List(udtOut), pact4sFormat.tree))

//    c.info(c.enclosingPosition, s"GENERATED Pact4s DataSource Format: ${show(result)}", true)

    return result
    
  }
}
