/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.test.iterative.nephele;

import java.util.Collection;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.aggregators.LongSumAggregator;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextLongParser;
import eu.stratosphere.pact.example.incremental.pagerank.DeltaPageRankWithInitializedDeltas.ProjectForWorkSetMapper;
import eu.stratosphere.pact.example.incremental.pagerank.DeltaPageRankWithInitializedDeltas.RankComparisonMatch;
import eu.stratosphere.pact.example.incremental.pagerank.DeltaPageRankWithInitializedDeltas.UpdateRankReduceDelta;
import eu.stratosphere.pact.example.incremental.pagerank.PRDependenciesComputationMatchDelta;
import eu.stratosphere.pact.generic.contract.UserCodeClassWrapper;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.BuildSecondCachedMatchDriver;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.JoinWithSolutionSetMatchDriver.SolutionSetSecondJoinDriver;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.test.util.TestBase2;

@RunWith(Parameterized.class)
public class DeltaPageRankNepheleITCase extends TestBase2 {
	
	protected String solutionsetPath;
	protected String edgesPath;
	protected String deltasPath;
	protected String resultPath;
	
	
	public DeltaPageRankNepheleITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		solutionsetPath = createTempFile("solutionset.txt", getInitialSolutionSet());
		edgesPath = createTempFile("edges.txt", getEdges());
		deltasPath = createTempFile("deltas.txt", getInitialDeltas());
		resultPath = getTempDirPath("results");
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		int dop = config.getInteger("DeltaPageRank#NumSubtasks", 1);
		int maxIterations = config.getInteger("DeltaPageRank#NumIterations", 1);
		return createDeltaPageRankJobGraph(solutionsetPath, edgesPath, deltasPath, resultPath, dop, maxIterations);
	}
	

	@Override
	protected void postSubmit() throws Exception {
//		for (BufferedReader reader : getResultReader(resultPath)) {
//			checkOddEvenResult(reader);
//		}
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("DeltaPageRank#NumSubtasks", 1);
		config1.setInteger("DeltaPageRank#NumIterations", 2);
		return toParameterList(config1);
	}
	
	// --------------------------------------------------------------------------------------------
	
	private JobGraph createDeltaPageRankJobGraph(String solutionsetPath, String edgesPath, String deltasPath,
			String resultPath, int degreeOfParallelism, int maxIterations) throws JobGraphDefinitionException
	{
		final int numSubTasksPerInstance = degreeOfParallelism;
		
		final TypeSerializerFactory<?> serializer = PactRecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> comparator = new PactRecordComparatorFactory(new int[] {0}, new Class[] {PactLong.class}, new boolean[] {true});
		final TypePairComparatorFactory<?, ?> pairComparator = PactRecordPairComparatorFactory.get();
		
		final long MEM_PER_CONSUMER = 10;
		final int ITERATION_ID = 1;
		
		
		JobGraph jobGraph = new JobGraph("Delta Page Rank");
		
		// --------------- the inputs ---------------------

		// initial solution set
		JobInputVertex solutionsetInput = JobGraphUtils.createInput(RecordInputFormat.class,
				solutionsetPath, "Initial Solution Set Input", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		{
			TaskConfig solutionsetInputConfig = new TaskConfig(solutionsetInput.getConfiguration());
			Configuration solutionsetInputUserConfig = solutionsetInputConfig.getStubParameters();
			solutionsetInputConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			solutionsetInputConfig.setOutputSerializer(serializer);
			solutionsetInputConfig.setOutputComparator(comparator, 0);
			
		/*	solutionsetInputUserConfig.setString(RecordInputFormat.RECORD_DELIMITER_PARAMETER, "\n");
			solutionsetInputUserConfig.setString(RecordInputFormat.FIELD_DELIMITER_PARAMETER, " ");
			solutionsetInputUserConfig.setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX + 0, DecimalTextLongParser.class);
			solutionsetInputUserConfig.setInteger(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX + 0, 0);
			solutionsetInputUserConfig.setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX + 1, DecimalTextDoubleParser.class);
			solutionsetInputUserConfig.setInteger(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX + 1, 1);
			solutionsetInputUserConfig.setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX + 2, DecimalTextDoubleParser.class);
			solutionsetInputUserConfig.setInteger(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX + 2, 2);
			solutionsetInputUserConfig.setInteger(RecordInputFormat.NUM_FIELDS_PARAMETER, 3);*/
			
		}

		// edges with number of outlinks
		JobInputVertex edgeInput = JobGraphUtils.createInput(RecordInputFormat.class,
			edgesPath, "EdgesInput", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		{
			TaskConfig edgesInputConfig = new TaskConfig(edgeInput.getConfiguration());
			edgesInputConfig.setOutputSerializer(serializer);
			edgesInputConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			edgesInputConfig.setOutputComparator(comparator, 0);
			
			Configuration edgesInputUserConfig = edgesInputConfig.getStubParameters();
	/*		edgesInputUserConfig.setString(RecordInputFormat.RECORD_DELIMITER_PARAMETER, "\n");
			edgesInputUserConfig.setString(RecordInputFormat.FIELD_DELIMITER_PARAMETER, " ");
			edgesInputUserConfig.setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX + 0, DecimalTextLongParser.class);
			edgesInputUserConfig.setInteger(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX + 0, 0);
			edgesInputUserConfig.setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX + 1, DecimalTextLongParser.class);
			edgesInputUserConfig.setInteger(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX + 1, 1);
			edgesInputUserConfig.setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX + 2, DecimalTextLongParser.class);
			edgesInputUserConfig.setInteger(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX + 2, 2);
			edgesInputUserConfig.setInteger(RecordInputFormat.NUM_FIELDS_PARAMETER, 3);	*/
		}
		
		// initial deltas
		JobInputVertex deltasInput = JobGraphUtils.createInput(RecordInputFormat.class,
				deltasPath, "Initial Deltas Input", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		{
			TaskConfig deltasInputConfig = new TaskConfig(deltasInput.getConfiguration());
			Configuration deltasInputUserConfig = deltasInputConfig.getStubParameters();
			deltasInputConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			deltasInputConfig.setOutputSerializer(serializer);
					
/*			deltasInputUserConfig.setString(RecordInputFormat.RECORD_DELIMITER_PARAMETER, "\n");
			deltasInputUserConfig.setString(RecordInputFormat.FIELD_DELIMITER_PARAMETER, " ");
			deltasInputUserConfig.setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX + 0, DecimalTextLongParser.class);
			deltasInputUserConfig.setInteger(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX + 0, 0);
			deltasInputUserConfig.setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX + 1, DecimalTextDoubleParser.class);
			deltasInputUserConfig.setInteger(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX + 1, 1);
			deltasInputUserConfig.setInteger(RecordInputFormat.NUM_FIELDS_PARAMETER, 2);	*/
					
		}
		
		// --------------- the iteration head ---------------------
		JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "Join With Edges (Iteration Head)", jobGraph,
			degreeOfParallelism, numSubTasksPerInstance);
		{
			TaskConfig headConfig = new TaskConfig(head.getConfiguration());
			headConfig.setIterationId(ITERATION_ID);
			
			// initial input / workset
			headConfig.addInputToGroup(0);
			headConfig.setInputSerializer(serializer, 0);
			headConfig.setInputComparator(comparator, 0);
			headConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
			headConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(0);
			
			// regular plan input (second input to the join)
			headConfig.addInputToGroup(1);
			headConfig.setInputSerializer(serializer, 1);
			headConfig.setInputComparator(comparator, 1);
			headConfig.setInputLocalStrategy(1, LocalStrategy.NONE);
			headConfig.setInputCached(1, true);
			headConfig.setInputMaterializationMemory(1, MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
			
			// initial solution set input
			headConfig.addInputToGroup(2);
			headConfig.setInputSerializer(serializer, 2);
			headConfig.setInputComparator(comparator, 2);
			headConfig.setInputLocalStrategy(2, LocalStrategy.NONE);
			headConfig.setIterationHeadSolutionSetInputIndex(2);
			
			headConfig.setSolutionSetSerializer(serializer);
			headConfig.setSolutionSetComparator(comparator);
			headConfig.setSolutionSetProberSerializer(serializer);
			headConfig.setSolutionSetProberComparator(comparator);
			headConfig.setSolutionSetPairComparator(pairComparator);
			
			// back channel / iterations
		//	headConfig.setWorksetIteration();
			headConfig.setBackChannelMemory(MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
			headConfig.setSolutionSetMemory(MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
			
			// output into iteration
			headConfig.setOutputSerializer(serializer);
			headConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			headConfig.setOutputComparator(comparator, 0);
			
			// final output
			TaskConfig headFinalOutConfig = new TaskConfig(new Configuration());
			headFinalOutConfig.setOutputSerializer(serializer);
			headFinalOutConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			headConfig.setIterationHeadFinalOutputConfig(headFinalOutConfig);
			
			// the sync
			headConfig.setIterationHeadIndexOfSyncOutput(2);
			// what does this do? It sets among the many outputs the head has (into step function, final results, and sync)
			// the number of the gate to the sync task, which synchronizes the barrier across all heads. 
			
			// the driver 
			headConfig.setDriver(BuildSecondCachedMatchDriver.class);
			headConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
			headConfig.setStubWrapper(new UserCodeClassWrapper<PRDependenciesComputationMatchDelta>(PRDependenciesComputationMatchDelta.class));
			headConfig.setDriverComparator(comparator, 0);
			headConfig.setDriverComparator(comparator, 1);
			headConfig.setDriverPairComparator(pairComparator);
			headConfig.setMemoryDriver(MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
			
			headConfig.addIterationAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, LongSumAggregator.class);
		}
		
		// --------------- the intermediate (reduce to sum deltas) ---------------
		JobTaskVertex intermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
			"Sum deltas", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
		{
			intermediateConfig.setIterationId(ITERATION_ID);
		
			intermediateConfig.addInputToGroup(0);
			intermediateConfig.setInputSerializer(serializer, 0);
			intermediateConfig.setInputComparator(comparator, 0);
			intermediateConfig.setInputLocalStrategy(0, LocalStrategy.SORT);
			intermediateConfig.setMemoryInput(0, MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
			intermediateConfig.setFilehandlesInput(0, 64);
			intermediateConfig.setSpillingThresholdInput(0, 0.85f);
		
			intermediateConfig.setOutputSerializer(serializer);
			intermediateConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		
			intermediateConfig.setDriver(ReduceDriver.class);
			intermediateConfig.setDriverStrategy(DriverStrategy.SORTED_GROUP);
			intermediateConfig.setDriverComparator(comparator, 0);
			intermediateConfig.setStubWrapper(new UserCodeClassWrapper<UpdateRankReduceDelta>(UpdateRankReduceDelta.class));
		}
		
		// --------------- solution set tail (solution set join) ---------------
		// his is actually not a tail, as it has successors ;-)
		JobTaskVertex solutionsetTail = JobGraphUtils.createTask(IterationIntermediatePactTask.class, 
				"SolutionSetIterationTail", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		TaskConfig solutionsetTailConfig = new TaskConfig(solutionsetTail.getConfiguration());
		{
			solutionsetTailConfig.setIterationId(ITERATION_ID);
			//solutionsetTailConfig.setWorksetIteration();
	//		solutionsetTailConfig.setUpdateSolutionSet();
	//		solutionsetTailConfig.setUpdateSolutionSetWithoutReprobe();
		
			// inputs and driver
			solutionsetTailConfig.addInputToGroup(0);
			solutionsetTailConfig.setInputSerializer(serializer, 0);
			
			// output
			solutionsetTailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			solutionsetTailConfig.setOutputSerializer(serializer);
		
			// the driver
			solutionsetTailConfig.setDriver(SolutionSetSecondJoinDriver.class);
			solutionsetTailConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
			solutionsetTailConfig.setStubWrapper(new UserCodeClassWrapper<RankComparisonMatch>(RankComparisonMatch.class));
			solutionsetTailConfig.setSolutionSetSerializer(serializer);
			solutionsetTailConfig.setIterationSolutionSetJoinNum(0);
		}
		
		// --------------- deltas tail (identity mapper after the rank comparison) ---------------
		JobTaskVertex deltasTail = JobGraphUtils.createTask(IterationTailPactTask.class, 
				"DeltasIterationTail", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		TaskConfig deltasTailConfig = new TaskConfig(deltasTail.getConfiguration());
		{
			deltasTailConfig.setIterationId(ITERATION_ID); 
	//		deltasTailConfig.setWorksetIteration();
		
			// inputs and driver
			deltasTailConfig.addInputToGroup(0);
			deltasTailConfig.setInputSerializer(serializer, 0);
			
			// output
			deltasTailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			deltasTailConfig.setOutputSerializer(serializer);
		
			// the driver
			deltasTailConfig.setDriver(MapDriver.class);
			deltasTailConfig.setDriverStrategy(DriverStrategy.MAP);
			deltasTailConfig.setStubWrapper(new UserCodeClassWrapper<ProjectForWorkSetMapper>(ProjectForWorkSetMapper.class));
		}
		
		// --------------- the output ---------------------
		JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "Final Output", degreeOfParallelism, numSubTasksPerInstance);
		{
			TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
			
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(serializer, 0);
			
			outputConfig.setStubWrapper(new UserCodeClassWrapper<RecordOutputFormat>(RecordOutputFormat.class));
			outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, resultPath);
			
			Configuration outputUserConfig = outputConfig.getStubParameters();
			outputUserConfig.setString(RecordOutputFormat.RECORD_DELIMITER_PARAMETER, "\n");
			outputUserConfig.setString(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, " ");
			outputUserConfig.setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactLong.class);
			outputUserConfig.setInteger(RecordOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 0);
			outputUserConfig.setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactDouble.class);
			outputUserConfig.setInteger(RecordOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 1);
			outputUserConfig.setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
		}
		
		// --------------- the auxiliaries ---------------------
		// you need this only when the solution set has a proper iteration tail (later in the proper setup we are going for)
//		JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput",
//			degreeOfParallelism, numSubTasksPerInstance);
		
		JobOutputVertex fakeWSTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeWSTailOutput",
				degreeOfParallelism, numSubTasksPerInstance);


		JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);	//2*degreeofParallelism? only 1*DOP
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setNumberOfIterations(maxIterations);
		syncConfig.setIterationId(ITERATION_ID);
		syncConfig.addIterationAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, LongSumAggregator.class);
		syncConfig.setConvergenceCriterion(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, WorksetEmptyConvergenceCriterion.class);
		
		// --------------- the wiring ---------------------

		// the order of the wiring matters (per task input), unfortunately... It must happen in the same order as the gates are used
		// workset (deltas), edges, solution set
		JobGraphUtils.connect(deltasInput, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(edgeInput, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(solutionsetInput, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

		JobGraphUtils.connect(head, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);
		
		JobGraphUtils.connect(intermediate, solutionsetTail, ChannelType.INMEMORY, DistributionPattern.POINTWISE);
		solutionsetTailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);
		
		JobGraphUtils.connect(solutionsetTail, deltasTail, ChannelType.INMEMORY, DistributionPattern.POINTWISE);
		deltasTailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(deltasTail, fakeWSTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		
		solutionsetInput.setVertexToShareInstancesWith(head);
		edgeInput.setVertexToShareInstancesWith(head);
		deltasInput.setVertexToShareInstancesWith(head);
		
		intermediate.setVertexToShareInstancesWith(head);
		solutionsetTail.setVertexToShareInstancesWith(head);
		deltasTail.setVertexToShareInstancesWith(head);
		
		output.setVertexToShareInstancesWith(head);
		sync.setVertexToShareInstancesWith(head);
		fakeWSTailOutput.setVertexToShareInstancesWith(deltasTail);
		
		return jobGraph;
	}
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	public static final String getEnumeratingVertices(int num) {
		if (num < 1 || num > 1000000)
			throw new IllegalArgumentException();
		
		StringBuilder bld = new StringBuilder(3 * num);
		for (int i = 1; i <= num; i++) {
			bld.append(i);
			bld.append('\n');
		}
		return bld.toString();
	}
	
	private String getInitialSolutionSet() {
		return "1 0.025 0.0\n2 0.125 0.0\n3 0.083 0.0\n4 0.083 0.0\n5 0.075 0.0\n6 0.075 0.0\n7 0.183 0.0\n8 0.150 0.0\n9 0.1 0.0";
	}
	
	private String getInitialDeltas() {
		return "1 -0.075\n2 0.025\n3 -0.017\n4 -0.017\n5 -0.025\n6 -0.025\n7 0.0833\n8 0.05\n9 0.0";
	}
	
	private String getEdges() {
		return "1 2 2\n1 3 2\n2 3 3\n2 4 3\n3 1 4\n3 2 4\n4 2 2\n5 6 2\n6 5 2\n7 8 2\n7 9 2\n8 7 2\n" +
	"8 9 2\n9 7 2\n9 8 2\n3 5 4\n3 6 4\n4 8 2\n2 7 3\n5 7 2\n6 4 2";
	}
	
	public static final class IdDuplicator extends MapStub {

		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			record.setField(1, record.getField(0, PactLong.class));
			out.collect(record);
		}
		
	}
}