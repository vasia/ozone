package eu.stratosphere.example.java.costmodel;

import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;

/**
 * Bulk Connected Components
 *
 */
@SuppressWarnings("serial")
public class BulkConnectedComponents implements ProgramDescription {

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
		
		DataSet<Tuple2<Long, Long>> bulkResult = doBulkCostConnectedComponents(verticesWithInitialId, edges, maxIterations);
		
		bulkResult.writeAsCsv(args[2] + "_bulk", "\n", " ");
		
		LocalExecutor.execute(env.createProgramPlan());
		
//		env.execute("Bulk CC");
		
	}
	
	public static DataSet<Tuple2<Long, Long>> doBulkCostConnectedComponents(DataSet<Tuple2<Long, Long>> verticesWithInitialId, 
			DataSet<Tuple2<Long, Long>> edges, int maxIterations) {
		
		/**
		 *  === start bulk iterations === 
		 */
		
		// open a bulk iteration
		IterativeDataSet<Tuple2<Long, Long>> iteration = verticesWithInitialId.iterate(maxIterations);
		
		// apply the step logic: join with the edges, select the minimum neighbor, update the component if the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration.join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
														.groupBy(0).aggregate(Aggregations.MIN, 1)
		                                                .join(iteration).where(0).equalTo(0)
		                                                .flatMap(new ComponentIdFilter());		
		// close the bulk iteration
		DataSet<Tuple2<Long, Long>> bulkResult = iteration.closeWith(changes);
		
		return bulkResult;
		
	}
	
	/* == Bulk iteration classes == */
	
	/**
	 * Function that turns a value into a 2-tuple where both fields are that value.
	 */
	public static final class DuplicateValue extends MapFunction<Tuple1<Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Tuple1<Long> value) throws Exception {
			return new Tuple2<Long, Long>(value.f0, value.f0);
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
		
		@Override
		public void open(Configuration conf) {
			int superstep = getIterationRuntimeContext().getSuperstepNumber();
			System.out.println("Bulk Iteration " + superstep);
		}
		
		@Override
		public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> value, Collector<Tuple2<Long, Long>> out) {
			if (value.f0.f1 < value.f1.f1) {
				out.collect(value.f0);
			}
			else {
				out.collect(value.f1);
			}
		}
	}

	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations> ";
	}

}
