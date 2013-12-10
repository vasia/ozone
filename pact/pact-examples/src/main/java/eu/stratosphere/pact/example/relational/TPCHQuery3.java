/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.example.relational;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * The TPC-H is a decision support benchmark on relational data.
 * Its documentation and the data generator (DBGEN) can be found
 * on http://www.tpc.org/tpch/ .This implementation is tested with
 * the DB2 data format.  
 * The PACT program implements a modified version of the query 3 of 
 * the TPC-H benchmark including one join, some filtering and an
 * aggregation.
 * 
 * SELECT l_orderkey, o_shippriority, sum(l_extendedprice) as revenue
 *   FROM orders, lineitem
 *   WHERE l_orderkey = o_orderkey
 *     AND o_orderstatus = "X" 
 *     AND YEAR(o_orderdate) > Y
 *     AND o_orderpriority LIKE "Z%"
 * GROUP BY l_orderkey, o_shippriority;
 */
public class TPCHQuery3 implements PlanAssembler, PlanAssemblerDescription {

	public static final String YEAR_FILTER = "parameter.YEAR_FILTER";
	public static final String PRIO_FILTER = "parameter.PRIO_FILTER";

	/**
	 * Map PACT implements the selection and projection on the orders table.
	 */
	@ConstantFields({0,1})
	public static class FilterO extends MapStub implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private String prioFilter;		// filter literal for the order priority
		private int yearFilter;			// filter literal for the year
		
		// reusable objects for the fields touched in the mapper
		private PactString orderStatus;
		private PactString orderDate;
		private PactString orderPrio;
		
		/**
		 * Reads the filter literals from the configuration.
		 * 
		 * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
		 */
		@Override
		public void open(Configuration parameters) {
			this.yearFilter = parameters.getInteger(YEAR_FILTER, 1990);
			this.prioFilter = parameters.getString(PRIO_FILTER, "0");
		}
	
		/**
		 * Filters the orders table by year, order status and order priority.
		 *
		 *  o_orderstatus = "X" 
		 *  AND YEAR(o_orderdate) > Y
		 *  AND o_orderpriority LIKE "Z"
	 	 *  
	 	 * Output Schema: 
	 	 *   0:ORDERKEY, 
	 	 *   1:SHIPPRIORITY
		 */
		@Override
		public void map(final PactRecord record, final Collector<PactRecord> out) {
			orderStatus = record.getField(2, PactString.class);
			if (!orderStatus.getValue().equals("F"))
				return;
			
			orderPrio = record.getField(4, PactString.class);
			if(!orderPrio.getValue().startsWith(this.prioFilter))
				return;
			
			orderDate = record.getField(3, PactString.class);
			if (!(Integer.parseInt(orderDate.getValue().substring(0, 4)) > this.yearFilter))
				return;
			
			record.setNumFields(2);
			out.collect(record);
		}
	}

	/**
	 * Match PACT realizes the join between LineItem and Order table.
	 *
	 */
	@ConstantFieldsFirst({0,1})
	public static class JoinLiO extends MatchStub implements Serializable {
		private static final long serialVersionUID = 1L;
		
		/**
		 * Implements the join between LineItem and Order table on the order key.
		 * 
		 * Output Schema:
		 *   0:ORDERKEY
		 *   1:SHIPPRIORITY
		 *   2:EXTENDEDPRICE
		 */
		@Override
		public void match(PactRecord order, PactRecord lineitem, Collector<PactRecord> out) {
			order.setField(2, lineitem.getField(1, PactDouble.class));
			out.collect(order);
		}
	}

	/**
	 * Reduce PACT implements the sum aggregation. 
	 * The Combinable annotation is set as the partial sums can be calculated
	 * already in the combiner
	 *
	 */
	@Combinable
	@ConstantFields({0,1})
	public static class AggLiO extends ReduceStub implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final PactDouble extendedPrice = new PactDouble();
		
		/**
		 * Implements the sum aggregation.
		 * 
		 * Output Schema:
		 *   0:ORDERKEY
		 *   1:SHIPPRIORITY
		 *   2:SUM(EXTENDEDPRICE)
		 */
		@Override
		public void reduce(Iterator<PactRecord> values, Collector<PactRecord> out) {
			PactRecord rec = null;
			double partExtendedPriceSum = 0;

			while (values.hasNext()) {
				rec = values.next();
				partExtendedPriceSum += rec.getField(2, PactDouble.class).getValue();
			}

			this.extendedPrice.setValue(partExtendedPriceSum);
			rec.setField(2, this.extendedPrice);
			out.collect(rec);
		}

		/**
		 * Creates partial sums on the price attribute for each data batch.
		 */
		@Override
		public void combine(Iterator<PactRecord> values, Collector<PactRecord> out) {
			reduce(values, out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(final String... args) {
		// parse program parameters
		final int numSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String ordersPath    = (args.length > 1 ? args[1] : "");
		final String lineitemsPath = (args.length > 2 ? args[2] : "");
		final String output        = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for Orders input
		FileDataSource orders = new FileDataSource(new RecordInputFormat(), ordersPath, "Orders");
		RecordInputFormat.configureRecordFormat(orders)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(PactLong.class, 0)		// order id
			.field(PactInteger.class, 7) 		// ship prio
			.field(PactString.class, 2, 2)	// order status
			.field(PactString.class, 4, 10)	// order date
			.field(PactString.class, 5, 8);	// order prio

		// create DataSourceContract for LineItems input
		FileDataSource lineitems = new FileDataSource(new RecordInputFormat(), lineitemsPath, "LineItems");
		RecordInputFormat.configureRecordFormat(lineitems)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(PactLong.class, 0)		// order id
			.field(PactDouble.class, 5);	// extended price

		// create MapContract for filtering Orders tuples
		MapContract filterO = MapContract.builder(new FilterO())
			.input(orders)
			.name("FilterO")
			.build();
		// filter configuration
		filterO.setParameter(YEAR_FILTER, 1993);
		filterO.setParameter(PRIO_FILTER, "5");
		// compiler hints
		filterO.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.05f);

		// create MatchContract for joining Orders and LineItems
		MatchContract joinLiO = MatchContract.builder(new JoinLiO(), PactLong.class, 0, 0)
			.input1(filterO)
			.input2(lineitems)
			.name("JoinLiO")
			.build();

		// create ReduceContract for aggregating the result
		// the reducer has a composite key, consisting of the fields 0 and 1
		ReduceContract aggLiO = ReduceContract.builder(new AggLiO())
			.keyField(PactLong.class, 0)
			.keyField(PactString.class, 1)
			.input(joinLiO)
			.name("AggLio")
			.build();

		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(new RecordOutputFormat(), output, aggLiO, "Output");
		RecordOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.lenient(true)
			.field(PactLong.class, 0)
			.field(PactInteger.class, 1)
			.field(PactDouble.class, 2);
		
		// assemble the PACT plan
		Plan plan = new Plan(result, "TPCH Q3");
		plan.setDefaultParallelism(numSubtasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks], [orders], [lineitem], [output]";
	}
}
