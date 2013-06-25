package eu.stratosphere.pact.example.incremental;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.example.iterative.DuplicateLongInputFormat;
import eu.stratosphere.pact.example.iterative.LongLongInputFormat;
import eu.stratosphere.pact.incremental.plans.BulkIterationPlan;

public class BulkConnectedComponents implements PlanAssembler,	PlanAssemblerDescription {
	
	
	public static final class DependenciesComputationMatch extends MatchStub {

		private final PactRecord result = new PactRecord();

		@Override
		public void match(PactRecord vertexWithComponent, PactRecord edge, Collector<PactRecord> out) {
			this.result.setField(0, edge.getField(1, PactLong.class));
			this.result.setField(1, vertexWithComponent.getField(1, PactLong.class));
			out.collect(this.result);
		}
	}
	
	@Combinable
	@ConstantFields(0)
	public static final class UpdateCmpIdReduce extends ReduceStub {

		private final PactRecord result = new PactRecord();
		private final PactLong vertexId = new PactLong();
		private final PactLong minComponentId = new PactLong();
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {

			final PactRecord first = records.next();
			final long vertexID = first.getField(0, PactLong.class).getValue();
			
			long minimumComponentID = first.getField(1, PactLong.class).getValue();

			while (records.hasNext()) {
				long candidateComponentID = records.next().getField(1, PactLong.class).getValue();
				if (candidateComponentID < minimumComponentID) {
					minimumComponentID = candidateComponentID;
				}
			}
			
			this.vertexId.setValue(vertexID);
			this.minComponentId.setValue(minimumComponentID);
			this.result.setField(0, this.vertexId);
			this.result.setField(1, this.minComponentId);
			out.collect(this.result);
		}
	}
	
	@ConstantFieldsFirst(0)
	public static final class OldValueComparisonMatch extends MatchStub {

		@Override
		public void match(PactRecord newVertexWithComponent, PactRecord currentVertexWithComponent, Collector<PactRecord> out){
	
			long candidateComponentID = newVertexWithComponent.getField(1, PactLong.class).getValue();
			long currentComponentID = currentVertexWithComponent.getField(1, PactLong.class).getValue();
	
			if (candidateComponentID < currentComponentID) {
				out.collect(newVertexWithComponent);
			}
			else
				out.collect(currentVertexWithComponent);
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
		
		// create DataSinkContract for writing the new cluster positions
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, "Result");
		RecordOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactLong.class, 0)
			.field(PactLong.class, 1);
		
		//create the Bulk Iteration Plan
		BulkIterationPlan iterationPlan = new BulkIterationPlan(result, "Bulk Connected Components");
		
		//configure the iteration plan
		iterationPlan.setUpBulkIteration(initialSolutionSet, dependencySet);
		iterationPlan.setMaxIterations(maxIterations);
		iterationPlan.setUpDependenciesMatch(DependenciesComputationMatch.class, PactLong.class, 0, 0);
		iterationPlan.setUpUpdateReduce(UpdateCmpIdReduce.class, PactLong.class, 0);
		iterationPlan.setUpComparisonMatch(OldValueComparisonMatch.class, PactLong.class, 0, 0);
		iterationPlan.assemble();
		iterationPlan.setDefaultParallelism(numSubTasks);
		
		return iterationPlan;
	}
	
	@Override
	public String getDescription() {
		return "Parameters: <numberOfSubTasks> <initialSolutionSet(NodeId, CmpId)> <dependencySet(SrcId, TrgId)> <out> <maxIterations>";
	}

}
