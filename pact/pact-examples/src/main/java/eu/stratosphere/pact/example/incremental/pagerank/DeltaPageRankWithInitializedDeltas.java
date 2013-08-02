package eu.stratosphere.pact.example.incremental.pagerank;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
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
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.generic.contract.WorksetIteration;

public class DeltaPageRankWithInitializedDeltas implements PlanAssembler, PlanAssemblerDescription {

	@ConstantFieldsFirst(0)
	public static final class RankComparisonMatch extends MatchStub {

		private final PactRecord result = new PactRecord();
		private final PactDouble newRank = new PactDouble();
		
		@Override
		public void match(PactRecord vertexWithDelta, PactRecord vertexWithOldRank, Collector<PactRecord> out) throws Exception {
			result.setField(0, vertexWithOldRank.getField(0, PactLong.class));
			newRank.setValue(vertexWithOldRank.getField(1, PactDouble.class).getValue() +
					vertexWithDelta.getField(1, PactDouble.class).getValue());
			result.setField(1, newRank);
			out.collect(result);
		}
	}
	
	@Combinable
	@ConstantFields(0)
	public static final class UpdateRankReduceDelta extends ReduceStub {
		
		private final PactDouble newRank = new PactDouble();
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {
			
			double rankSum = 0.0;
			double rank;
			PactRecord rec = null;
			
			while (records.hasNext()) {
				rec = records.next();
				rank = rec.getField(1, PactDouble.class).getValue();
				rankSum += rank;
			}
			
			// ignore small deltas
			if (rankSum > 0.001) {
				newRank.setValue(rankSum);
				rec.setField(1, newRank);
				out.collect(rec);
			}

		}
	}
	
	@Override
	public Plan getPlan(String... args) {
		
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String solutionSetInput = (args.length > 1 ? args[1] : "");
		final String deltasInput = (args.length > 2 ? args[2] : "");
		final String dependencySetInput = (args.length > 3 ? args[3] : "");
		final String output = (args.length > 4 ? args[4] : "");
		final int maxIterations = (args.length > 5 ? Integer.parseInt(args[5]) : 1);
		
		// create DataSourceContract for the initalSolutionSet
		FileDataSource initialSolutionSet = new FileDataSource(InitialRankInputFormat.class, solutionSetInput, "Initial Solution Set");		

		// create DataSourceContract for the initalDeltaSet
		FileDataSource initialDeltaSet = new FileDataSource(InitialRankInputFormat.class, deltasInput, "Initial DeltaSet");		
				
		// create DataSourceContract for the edges
		FileDataSource dependencySet = new FileDataSource(EdgesWithWeightInputFormat.class, dependencySetInput, "Dependency Set");
		
		WorksetIteration iteration = new WorksetIteration(0, "Delta PageRank");
		iteration.setInitialSolutionSet(initialSolutionSet);
		iteration.setInitialWorkset(initialDeltaSet);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		MatchContract dependenciesMatch = MatchContract.builder(PRDependenciesComputationMatch.class, 
				PactLong.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(dependencySet)
				.name("calculate dependencies")
				.build();
		
		ReduceContract updateRanks = ReduceContract.builder(UpdateRankReduceDelta.class, PactLong.class, 0)
				.input(dependenciesMatch)
				.name("update ranks")
				.build();
		
		MatchContract oldRankComparison = MatchContract.builder(RankComparisonMatch.class, PactLong.class, 0, 0)
				.input1(updateRanks)
				.input2(iteration.getSolutionSet())
				.name("comparison with old ranks")
				.build();
		
		iteration.setNextWorkset(updateRanks);
		iteration.setSolutionSetDelta(oldRankComparison);
		
		// create DataSinkContract for writing the final ranks
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, iteration, "Final Ranks");
		RecordOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactLong.class, 0)
			.field(PactDouble.class, 1);
		
		// return the PACT plan
		Plan plan = new Plan(result, "Delta PageRank");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
		
	}

	
	@Override
	public String getDescription() {
		return "Parameters: <numberOfSubTasks> <initialSolutionSet(pageId, rank)> <deltas(pageId, delta)> <dependencySet(srcId, trgId, out_links)> <out> <maxIterations>";
	}

}
