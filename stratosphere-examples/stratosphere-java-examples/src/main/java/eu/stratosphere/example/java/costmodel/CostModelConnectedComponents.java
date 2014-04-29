package eu.stratosphere.example.java.costmodel;

import java.util.Iterator;

import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.aggregators.LongSumAggregator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIterativeDataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;


/**
 * A Connected Components implementation to test the cost model functionality.
 * The algorithm starts running Bulk iterations until the cost of the dependency
 * plan becomes less than the cost of the bulk plan or until the maximum number of iterations.
 * In the case of cost-based convergence of the bulk plan, the execution continues
 * with a dependency plan, for the remaining iterations or until no element changes.
 *
 */
@SuppressWarnings("serial")
public class CostModelConnectedComponents implements ProgramDescription {
	
	private static final String UPDATED_ELEMENTS_AGGR = "updated.elements.aggr";
	private static int bulkFinishIteration;

	public static void main(String... args) throws Exception {
		if (args.length < 4) {
			System.err.println("Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>");
			return;
		}
		
		final int maxIterations = Integer.parseInt(args[3]);
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Long> vertices = env.readCsvFile(args[0]).types(Long.class).map(new MapFunction<Tuple1<Long>, Long>() {
			public Long map(Tuple1<Long> value) { return value.f0; } });
		
		DataSet<Tuple2<Long, Long>> edges = env.readCsvFile(args[1]).fieldDelimiter(' ').types(Long.class, Long.class);
		
		DataSet<Tuple2<Long, Long>> result = doCostConnectedComponents(vertices, edges, maxIterations);
		
		result.writeAsCsv(args[2], "\n", " ");
		
		//env.execute("Cost Connected Components");
		
		LocalExecutor.execute(env.createProgramPlan("Cost Connected Components"));
		
	}
	
	public static DataSet<Tuple2<Long, Long>> doCostConnectedComponents(DataSet<Long> vertices, 
			DataSet<Tuple2<Long, Long>> edges, int maxIterations) {
		
		/**
		 *  === start bulk iterations === 
		 */
		
		// assign the initial components (equal to the vertex id.
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices.map(new DuplicateValue<Long>());
		
		// open a bulk iteration
		IterativeDataSet<Tuple2<Long, Long>> iteration = verticesWithInitialId.iterate(maxIterations);
		
		// apply the step logic: join with the edges, select the minimum neighbor, update the component if the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration.join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
		                                               .groupBy(0).aggregate(Aggregations.MIN, 1)
		                                               .join(iteration).where(0).equalTo(0)
		                                                .flatMap(new ComponentIdFilter());
		
		// register updated elements aggregator and cost model convergence criterion
		iteration.registerAggregationConvergenceCriterion(UPDATED_ELEMENTS_AGGR, LongSumAggregator.class, 
				UpdatedElementsCostModelConvergence.class);
		
		// close the bulk iteration
		DataSet<Tuple2<Long, Long>> bulkResult = iteration.closeWith(changes);
		
		/**
		 *  === start dependency iterations === 
		 */
		DeltaIterativeDataSet<Tuple2<Long, Long>, Tuple2<Long, Long>> depIteration = 
				bulkResult.iterateDelta(bulkResult, maxIterations - bulkFinishIteration + 1, 0);
		
		DataSet<Long> candidates = depIteration.join(edges).where(0).equalTo(0)
				.with(new FindCandidatesJoin())
				.groupBy(new KeySelector<Long, Long>() { 
                    public Long getKey(Long id) { return id; } 
                  }).reduceGroup(new RemoveDuplicatesReduce());
		
		DataSet<Tuple2<Long, Long>> candidatesDependencies = 
				candidates.join(edges)
				.where(new KeySelector<Long, Long>() { 
                    public Long getKey(Long id) { return id; } 
                  }).equalTo(new KeySelector<Tuple2<Long, Long>, Long>() { 
                    public Long getKey(Tuple2<Long, Long> vertexWithId) 
                    { return vertexWithId.f1; } 
                  }).with(new FindCandidatesDependenciesJoin());
		
		DataSet<Tuple2<Long, Long>> verticesWithNewComponents = 
				candidatesDependencies.join(depIteration.getSolutionSet()).where(0).equalTo(0)
				.with(new NeighborWithComponentIDJoinDep())
				.groupBy(0).reduceGroup(new MinimumReduce());
		
		DataSet<Tuple2<Long, Long>> updatedComponentId = 
				verticesWithNewComponents.join(depIteration.getSolutionSet()).where(0).equalTo(0)
				.flatMap(new MinimumIdFilter());
		
		return depIteration.closeWith(updatedComponentId, updatedComponentId);
	}
	
	/* == Bulk iteration classes == */
	
	/**
	 * Function that turns a value into a 2-tuple where both fields are that value.
	 */
	public static final class DuplicateValue<T> extends MapFunction<T, Tuple2<T, T>> {
		
		@Override
		public Tuple2<T, T> map(T value) {
			return new Tuple2<T, T>(value, value);
		}
	}
	
	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that
	 * a vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	public static final class NeighborWithComponentIDJoin extends JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
		}
	}
	
	/**
	 * The input is nested tuples ( (vertex-id, candidate-component) , (vertex-id, current-component) )
	 */
	public static final class ComponentIdFilter extends FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		private LongSumAggregator updatedElementsAggr;
		
		@Override
		public void open(Configuration conf) {
			updatedElementsAggr = getIterationRuntimeContext().getIterationAggregator(UPDATED_ELEMENTS_AGGR);
			bulkFinishIteration = getIterationRuntimeContext().getSuperstepNumber();
		}
		
		@Override
		public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> value, Collector<Tuple2<Long, Long>> out) {
			if (value.f0.f1 < value.f1.f1) {
				out.collect(value.f0);
				updatedElementsAggr.aggregate(1);
			}
			else {
				out.collect(value.f1);
			}
		}
	}
	
	/* == Dependency iteration classes == */
	
	public static final class FindCandidatesJoin extends JoinFunction
		<Tuple2<Long, Long>, Tuple2<Long, Long>, Long> {
	
		@Override
		public void open(Configuration params) {
			int step = getIterationRuntimeContext().getSuperstepNumber();
			System.out.println("Dependency Iteration Step " + step);
		}
		
		@Override
		public Long join(Tuple2<Long, Long> vertexWithCompId,
				Tuple2<Long, Long> edge) throws Exception {
			// emit target vertex
			return edge.f1;
		}
	}

	public static final class RemoveDuplicatesReduce extends GroupReduceFunction<Long, Long> {
	
		@Override
		public void reduce(Iterator<Long> values, Collector<Long> out) throws Exception {
				out.collect(values.next());
		}
	}
	
	public static final class FindCandidatesDependenciesJoin extends JoinFunction
		<Long, Tuple2<Long, Long>, Tuple2<Long, Long>> {
		
		@Override
		public Tuple2<Long, Long> join(Long candidateId, Tuple2<Long, Long> edge) throws Exception {
			return edge;
		}
	}
	
	public static final class MinimumReduce extends GroupReduceFunction
		<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		final Tuple2<Long, Long> resultVertex = new Tuple2<Long, Long>();

		@Override
		public void reduce(Iterator<Tuple2<Long, Long>> values,
				Collector<Tuple2<Long, Long>> out) throws Exception {
	
			final Tuple2<Long, Long> first = values.next();		
			final Long vertexId = first.f0;
			Long minimumCompId = first.f1;
			
			while ( values.hasNext() ) {
				Long candidateCompId = values.next().f1;
				if ( candidateCompId < minimumCompId ) {
					minimumCompId = candidateCompId;
				}
			}
			resultVertex.setField(vertexId, 0);
			resultVertex.setField(minimumCompId, 1);
	
			out.collect(resultVertex);
		}
	}
	
	public static final class NeighborWithComponentIDJoinDep extends JoinFunction
		<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
	
		private static final long serialVersionUID = 1L;
		
		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> edge,
				Tuple2<Long, Long> vertexWithCompId) throws Exception {
			
			vertexWithCompId.setField(edge.f1, 0);
			return vertexWithCompId;
		}
}
	
	public static final class MinimumIdFilter extends FlatMapFunction
		<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {
	
		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> vertexWithNewAndOldId,
				Collector<Tuple2<Long, Long>> out) throws Exception {
						
			if ( vertexWithNewAndOldId.f0.f1 < vertexWithNewAndOldId.f1.f1 ) {
				out.collect(vertexWithNewAndOldId.f0);
			}
		}
	}

	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>";
	}

}
