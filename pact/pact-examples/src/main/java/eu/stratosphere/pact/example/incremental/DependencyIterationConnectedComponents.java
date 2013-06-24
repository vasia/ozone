package eu.stratosphere.pact.example.incremental;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.example.iterative.LongLongInputFormat;
import eu.stratosphere.pact.incremental.plans.DependencyIterationPlan;

/**
 * 
 * @author hinata
 * 
 * Connected Components as a Dependency WorkSet Iteration
 *
 */
public class DependencyIterationConnectedComponents implements PlanAssembler,
		PlanAssemblerDescription {

	@Override
	public String getDescription() {
		return "Parameters: <numberOfSubTasks> <vertices> <edges> <out>";
	}
	
	public class WorkSetWithDependenciesJoin extends MatchStub {

		private final PactRecord result = new PactRecord();

		@Override
		public void match(PactRecord vertexWithComponent, PactRecord edge, Collector<PactRecord> out) {
			//TODO: Auto-generated method stub
		}
	}
	
	public class GroupCandidatesReduce extends ReduceStub {
		
		private final PactRecord result = new PactRecord();

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			// TODO Auto-generated method stub
		}
	}
	
	public class RetrieveDependenciesJoin extends MatchStub {

		private final PactRecord result = new PactRecord();

		@Override
		public void match(PactRecord in1, PactRecord in2, Collector<PactRecord> out) {
			//TODO: Auto-generated method stub
		}
	}

	public class RetrieveSolutionSetValuesJoin extends MatchStub {

		private final PactRecord result = new PactRecord();

		@Override
		public void match(PactRecord in1, PactRecord in2, Collector<PactRecord> out) {
			//TODO: Auto-generated method stub
		}
	}
	
	public class updateCoGroup extends CoGroupStub {
		
		private final PactRecord result = new PactRecord();

		@Override
		public void coGroup(Iterator<PactRecord> in1, Iterator<PactRecord> in2, Collector<PactRecord> out) {
			// TODO Auto-generated method stub
		}
	}


	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String verticesInput = (args.length > 1 ? args[1] : "");
		final String edgeInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for the vertices
		FileDataSource initialVertices = new FileDataSource(DuplicateLongInputFormat.class, verticesInput, "Vertices");
				
		//create DataSource for the dependency set
		// create DataSourceContract for the edges
		FileDataSource edges = new FileDataSource(LongLongInputFormat.class, edgeInput, "Edges");

		// create DataSink
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output);

		DependencyIterationPlan plan = new DependencyIterationPlan(result, "");
		
		//TODO: setup the Contracts of the plan
		
		
		result.setInput(plan.getIteration());
		
		plan.assemble();
		plan.setDefaultParallelism(numSubTasks);
		
		return plan;
	}



}
