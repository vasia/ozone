package eu.stratosphere.pact.example.incremental.pagerank;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.incremental.plans.DeltaIterationPlan;

public class DeltaPageRank implements PlanAssembler, PlanAssemblerDescription {

	public static final class BothRanksMatch extends MatchStub {

		private final PactRecord result = new PactRecord();
		
		@Override
		public void match(PactRecord vertexWithOldRank, PactRecord vertexWithNewRank, Collector<PactRecord> out) throws Exception {
			this.result.setField(0, vertexWithOldRank.getField(0, PactLong.class));
			this.result.setField(1, vertexWithOldRank.getField(1, PactDouble.class));
			this.result.setField(2, vertexWithNewRank.getField(1, PactDouble.class));
			out.collect(result);
		}
		
	}
	
	// emit the new rank
	public static final class SolutionSetMap extends MapStub {
		
		private final PactRecord newRank = new PactRecord();

		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			this.newRank.setField(0, record.getField(0, PactLong.class));
			this.newRank.setField(1, record.getField(2, PactLong.class));
			out.collect(newRank);
		}
		
	}
	
	// emit the difference of the ranks
	public static final class UpdateDeltasMap extends MapStub {

		private final PactRecord vertexWithDeltaRank = new PactRecord();
		private final PactDouble deltaRank = new PactDouble();
		
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			this.vertexWithDeltaRank.setField(0, record.getField(0, PactLong.class));
			// newRank - oldRank
			double delta = record.getField(2, PactLong.class).getValue() - record.getField(1, PactLong.class).getValue();
			this.deltaRank.setValue(delta);
			this.vertexWithDeltaRank.setField(1, deltaRank);
			out.collect(vertexWithDeltaRank);
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
		FileDataSource initialSolutionSet = new FileDataSource(InitialRankInputFormat.class, solutionSetInput, "Initial Solution Set");		
		
		// create DataSourceContract for the edges
		FileDataSource dependencySet = new FileDataSource(EdgesWithWeightInputFormat.class, dependencySetInput, "Dependency Set");
		
		// create DataSinkContract for writing the final ranks
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, "Final Ranks");
		RecordOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactLong.class, 0)
			.field(PactDouble.class, 1);
	
	
		// assemble one bulk iteration to initialize the Delta set
		/*MatchContract dependencyMatch = MatchContract.builder(PRDependenciesComputationMatch.class, PactLong.class, 0, 0)
				.input1(initialSolutionSet)
				.input2(dependencySet)
				.name("Bulk: Join SolutionSet with Dependency Set")
				.build();
		
		ReduceContract updateReduce = ReduceContract.builder(UpdateRankReduce.class, PactLong.class, 0)
				.input(dependencyMatch)
				.name("Bulk: Update Function")
				.build();
		
		MatchContract comparisonMatch = MatchContract.builder(BothRanksMatch.class, PactLong.class, 0, 0)
				.input1(updateReduce)
				.input2(initialSolutionSet)
				.name("Bulk: Comparison with Solution Set")
				.build();
		
		MapContract newSolutionSet = MapContract.builder(SolutionSetMap.class)
				.input(comparisonMatch)
				.name("Bulk: Initialize the Solution Set for the Delta Iteration")
				.build();
		
		MapContract initializeDeltas = MapContract.builder(InitializeDeltasMap.class)
				.input(comparisonMatch)
				.name("Bulk: Initialize Deltas")
				.build();*/
		
		// create the delta iteration plan
		DeltaIterationPlan iterationPlan = new DeltaIterationPlan(result, "Delta PageRank");
		
		//configure the delta iteration plan
		iterationPlan.setUpDeltaIteration(initialSolutionSet, initialSolutionSet, dependencySet);
		iterationPlan.setMaxIterations(maxIterations);
		
		iterationPlan.setUpDependenciesMatch(PRDependenciesComputationMatch.class, PactLong.class, 0, 0);
		iterationPlan.setUpUpdateReduce(UpdateRankReduce.class, PactLong.class, 0);
		iterationPlan.setUpComparisonMatch(BothRanksMatch.class, PactLong.class, 0, 0);
		iterationPlan.setUpUpdateSolutionSetMap(SolutionSetMap.class);
		iterationPlan.setUpUpdateDeltaSetMap(UpdateDeltasMap.class);
		
		iterationPlan.assemble();		
		iterationPlan.setDefaultParallelism(numSubTasks);
		
		return iterationPlan;
	}

	
	@Override
	public String getDescription() {
		return "Parameters: <numberOfSubTasks> <initialSolutionSet(pageId, rank)> <dependencySet(srcId, trgId, out_links)> <out> <maxIterations>";
	}

}
