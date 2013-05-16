package eu.stratosphere.pact.example.incremental;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.example.pagerank.DanglingPageRankInputFormat;
import eu.stratosphere.pact.example.pagerank.ImprovedAdjacencyListInputFormat;
import eu.stratosphere.pact.example.pagerank.PageWithRankOutFormat;
import eu.stratosphere.pact.incremental.contracts.BulkIterationGeneric;
import eu.stratosphere.pact.incremental.plans.BulkIterationPlan;
	
public class BulkPageRank implements PlanAssembler, PlanAssemblerDescription {

	public static final String NUM_VERTICES_CONFIG_PARAM = "pageRank.numVertices";
	
	@Override
	public Plan getPlan(String... args) {
		int dop = 1;
		String pageWithRankInputPath = "";
		String adjacencyListInputPath = "";
		String outputPath = "";
		int numIterations = 25;
		long numVertices = 5;
		long numDanglingVertices = 1;

		if (args.length >= 7) {
			dop = Integer.parseInt(args[0]);
			pageWithRankInputPath = args[1];
			adjacencyListInputPath = args[2];
			outputPath = args[3];
			numIterations = Integer.parseInt(args[4]);
			numVertices = Long.parseLong(args[5]);
			numDanglingVertices = Long.parseLong(args[6]);
		}
		
		FileDataSource pageWithRankInput = new FileDataSource(DanglingPageRankInputFormat.class, pageWithRankInputPath, "DanglingPageWithRankInput");
		pageWithRankInput.getParameters().setLong(DanglingPageRankInputFormat.NUM_VERTICES_PARAMETER, numVertices);
		
		FileDataSource adjacencyListInput = new FileDataSource(ImprovedAdjacencyListInputFormat.class, adjacencyListInputPath, "AdjancencyListInput");
			
		BulkIterationGeneric iteration = new BulkIterationGeneric("Bulk PageRank");
		
		//configure Bulk Iteration
		iteration.setInitialSolutionSet(pageWithRankInput);
		iteration.setDependencySet(adjacencyListInput);
		
		FileDataSink out = new FileDataSink(PageWithRankOutFormat.class, outputPath, iteration, "Final Ranks");
		
		BulkIterationPlan p = new BulkIterationPlan(out, "Bulk PageRank", iteration);
		p.setDefaultParallelism(dop);
		return p;
	}
	
	@Override
	public String getDescription() {
		return "Parameters: <degree-of-parallelism> <pages-input-path> <edges-input-path> <output-path> <max-iterations> <num-vertices> <num-dangling-vertices>";	}


}
