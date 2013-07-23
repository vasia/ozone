package eu.stratosphere.pact.example.incremental.pagerank;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.incremental.plans.BulkIterationPlan;
	
public class BulkPageRank implements PlanAssembler, PlanAssemblerDescription {
	
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
		
		//create the Bulk Iteration Plan
		BulkIterationPlan iterationPlan = new BulkIterationPlan(result, "Bulk PageRank");
		
		//configure the iteration plan
		iterationPlan.setUpBulkIteration(initialSolutionSet, dependencySet);
		iterationPlan.setMaxIterations(maxIterations);
		iterationPlan.setUpDependenciesMatch(PRDependenciesComputationMatch.class, PactLong.class, 0, 0);
		iterationPlan.setUpUpdateReduce(UpdateRankReduce.class, PactLong.class, 0);
		iterationPlan.setUpComparisonMatch(OldRankComparisonMatch.class, PactLong.class, 0, 0);		
		iterationPlan.assemble();
		iterationPlan.setDefaultParallelism(numSubTasks);
		
		return iterationPlan;
	}
	
	@Override
	public String getDescription() {
		return "Parameters: <numberOfSubTasks> <initialSolutionSet(pageId, rank)> <dependencySet(srcId, trgId, out_links)> <out> <maxIterations>";	}


}
