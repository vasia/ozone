package eu.stratosphere.example.java.costmodel;

import java.util.Iterator;

import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.aggregators.LongSumAggregator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;

/**
 * A Connected Components implementation that uses the "active subgraph" as the workset.
 * In the beginning of the algorithm, the edges input is pre-processed to generate
 * the dependencies set, which corresponds to the part of the graph that is active
 * in each iteration. 
 *
 */
@SuppressWarnings("serial")
public class AlternativeDependencyConnectedComponents implements ProgramDescription {
	
	private static final String WORKSET_ELEMENTS_AGGR = "workset.elements.aggr";

	public static void main(String... args) throws Exception {
		if (args.length < 4) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>");
			return;
		}
		
		final int maxIterations = Integer.parseInt(args[3]);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		env.setDegreeOfParallelism(1);
		
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = env.readCsvFile(args[0]).types(Long.class).map(new DuplicateValue());
		
		DataSet<Tuple2<Long, Long>> edges = env.readCsvFile(args[1]).fieldDelimiter('\t').types(Long.class, Long.class);
		
		DataSet<Tuple3<Long, Long, Long>> dependencies = edges.join(edges).where(1).equalTo(1)
															.with(new ProjectEdges());
		
//		dependencies.print();
		
		DataSet<Tuple2<Long, Long>> result = doConnectedComponents(verticesWithInitialId, dependencies, maxIterations);		
				
		result.writeAsCsv(args[2] + "_dep", "\n", " ");
		
		LocalExecutor.execute(env.createProgramPlan());
		
	}
	
	public static DataSet<Tuple2<Long, Long>> doConnectedComponents(DataSet<Tuple2<Long, Long>> verticesWithInitialId, 
			DataSet<Tuple3<Long, Long, Long>> dependencies, int maxIterations) {
		
		// open a delta iteration
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = 
				verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);
				
		// compute the updates for the solution set
		DataSet<Tuple2<Long, Long>> newSolutionSet = iteration.getWorkset().joinWithHuge(dependencies)
															.where(0).equalTo(0).with(new ProjectTargetWithId())
															.groupBy(0).aggregate(Aggregations.MIN, 1)
															.join(iteration.getSolutionSet()).where(0).equalTo(0)
															.flatMap(new ComponentIdFilter());

		DataSet<Tuple2<Long, Long>> mappedWorkset = newSolutionSet.map(new DummyMapper());
		
		// register the workset elements aggregator
		iteration.registerAggregator(WORKSET_ELEMENTS_AGGR, new LongSumAggregator());
		
		// close the bulk iteration
		DataSet<Tuple2<Long, Long>> result = iteration.closeWith(newSolutionSet, mappedWorkset);
		
		return result;
		
	}
	
	public static final class ProjectEdges extends JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, 
			Tuple3<Long, Long, Long>> {

		@Override
		public Tuple3<Long, Long, Long> join(Tuple2<Long, Long> first, Tuple2<Long, Long> second) throws Exception {
			return new Tuple3<Long, Long, Long>(first.f0, second.f0, second.f1);
		}
		
	}
	
	public static final class ProjectTargetWithId extends JoinFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>, 
	Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithId, Tuple3<Long, Long, Long> dependency) 
				throws Exception {
			return new Tuple2<Long, Long>(dependency.f2, vertexWithId.f1);
		}

	}
	
	/**
	 * Function that turns a value into a 2-tuple where both fields are that value.
	 */
	public static final class DuplicateValue extends MapFunction<Tuple1<Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Tuple1<Long> value) throws Exception {
			return new Tuple2<Long, Long>(value.f0, value.f0);
		}
	}
	
	public static final class DeduplicateReducer extends GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public void reduce(Iterator<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(values.next());
			
		}
	}
	
	/**
	 * The input is nested tuples ( (vertex-id, candidate-component) , (vertex-id, current-component) )
	 */
	@ConstantFieldsFirst("0 -> 0")
	public static final class ComponentIdFilter extends FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {
		
		@Override
		public void open(Configuration conf) {
			int superstep = getIterationRuntimeContext().getSuperstepNumber();
			System.out.println("Iteration " + superstep);
		}
		
		@Override
		public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> newAndOldValue, Collector<Tuple2<Long, Long>> out) {
			if (newAndOldValue.f0.f1 < newAndOldValue.f1.f1) {
				out.collect(newAndOldValue.f0);
			}
		}
	}
	
	@ConstantFieldsFirst("0 -> 0, 1 - > 1")
	public static final class DummyMapper extends MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private LongSumAggregator aggr;
		private int superstep;
		
		@Override
		public void open(Configuration conf) {
			aggr = getIterationRuntimeContext().getIterationAggregator(WORKSET_ELEMENTS_AGGR);
			superstep = getIterationRuntimeContext().getSuperstepNumber();
			if (superstep > 1) {
				System.out.println("Elements in workset in superstep " + superstep 
						+ ": " + getIterationRuntimeContext().getPreviousIterationAggregate(WORKSET_ELEMENTS_AGGR));
			}
		}
		
		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
			aggr.aggregate(1L);
			return value;
		}
	}

	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations> "
				+ "<number-of-vertices> <avg-node-degree>";
	}

}
