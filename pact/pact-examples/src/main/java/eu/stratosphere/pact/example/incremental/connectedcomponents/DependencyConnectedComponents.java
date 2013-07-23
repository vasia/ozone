package eu.stratosphere.pact.example.incremental.connectedcomponents;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.example.incremental.DuplicateLongInputFormat;
import eu.stratosphere.pact.example.incremental.LongLongInputFormat;
import eu.stratosphere.pact.incremental.plans.DependencyIterationPlan;

public class DependencyConnectedComponents implements PlanAssembler, PlanAssemblerDescription {
	
	public static final class FindCandidatesMatch extends MatchStub {

		private final PactRecord result = new PactRecord();
		
		@Override
		public void match(PactRecord wRecord, PactRecord dRecord, Collector<PactRecord> out) throws Exception {
			this.result.setField(0, dRecord.getField(1, PactLong.class));
			out.collect(this.result);
		}
		
	}
	
	public static final class GroupCandidatesReduce extends ReduceStub {

		@Override
		public void reduce(Iterator<PactRecord> candidates, Collector<PactRecord> out) throws Exception {
			out.collect(candidates.next());
		}
		
	}
	
	public static final class FindCandidatesDependenciesMatch extends MatchStub {
		
		@Override
		public void match(PactRecord wRecord, PactRecord dRecord, Collector<PactRecord> out) throws Exception {
			out.collect(dRecord);
		}
		
	}
	
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String solutionSetInput = (args.length > 1 ? args[1] : "");
		final String dependencySetInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");
		final int maxIterations = (args.length > 4 ? Integer.parseInt(args[4]) : 1);
		
		// create DataSourceContract for the initalSolutionSet
		FileDataSource initialSolutionSet = new FileDataSource(DuplicateLongInputFormat.class, solutionSetInput, "Initial Solution Set");
		
		// create DataSourceContract for the edges
		FileDataSource dependencySet = new FileDataSource(LongLongInputFormat.class, dependencySetInput, "Dependency Set");
		
		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, "Result");
		RecordOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactLong.class, 0)
			.field(PactLong.class, 1);
		
		//create the Bulk Iteration Plan
		DependencyIterationPlan iterationPlan = new DependencyIterationPlan(result, "Dependency Iteration Connected Components");
		
		//configure the iteration plan
		iterationPlan.setUpDependencyIteration(initialSolutionSet, initialSolutionSet, dependencySet);
		iterationPlan.setMaxIterations(maxIterations);
		
		iterationPlan.setUpCandidatesMatch(FindCandidatesMatch.class, PactLong.class, 0, 0);
		iterationPlan.setUpCandidatesReduce(GroupCandidatesReduce.class, PactLong.class, 0);
		iterationPlan.setUpCandidatespDependenciesMatch(FindCandidatesDependenciesMatch.class, PactLong.class, 0, 1);
		iterationPlan.setUpDependenciesMatch(CCDependenciesComputationMatch.class, PactLong.class, 0, 0);
		iterationPlan.setUpUpdateReduce(CCUpdateCmpIdReduce.class, PactLong.class, 0);
		iterationPlan.setUpComparisonMatch(CCWorksetOldValueComparisonMatch.class, PactLong.class, 0, 0);
		
		iterationPlan.assemble();
		iterationPlan.setDefaultParallelism(numSubTasks);
		
		return iterationPlan;
	}
	
	@Override
	public String getDescription() {
		return "Parameters: <numberOfSubTasks> <initialSolutionSet(NodeId, CmpId)> <dependencySet(SrcId, TrgId)> <out> <maxIterations>";
	}

}
