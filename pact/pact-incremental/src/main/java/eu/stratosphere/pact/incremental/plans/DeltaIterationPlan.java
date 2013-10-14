package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanException;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.incremental.contracts.DeltaIterationContract;

/**
 * 
 * Delta Iteration Plan
 * In order to initialize the deltas
 * one bulk iteration is executed first
 * and then connected to the delta plan
 *
 *
 *	The internal plan of the DeltaIterationContract is built as follows:
 * 
 * 
 * 		|===================== *Map*: updateDeltaSet	
 * 		|									|
 * 		|									|
 * 		|	===	*Map*: updateSolutionSet	|	
 * 		|	|				|				|
 * 		|	|				|				|	
 *		|	|			 *Match*: oldValueComparison
 *		|	|				|				|
 *		|	|				|				|
 *		|	|				|		*Reduce*: valuesUpdate (aggregate update function)
 *		|	|				|						|
 *		|	|=====>	S: solutionSet					|
 *		|											|
 *		|											|
 *		|							*Match*: dependencies computation
		|									|				|
 *		|									|				|
 *		|============================> W: deltaSet		D: dependencySet
 *
 */
 

public class DeltaIterationPlan extends Plan implements DeltaIterationPlanner {

	private DeltaIterationContract iteration;
	
	// Delta Iteration Parameters
	private Contract initialSolutionSet;
	private Contract initialWorkSet;
	
	// Delta Iteration Contracts
	private MatchContract dependenciesMatch;
	private ReduceContract updateReduce;
	private MatchContract comparisonMatch;
	private MapContract updateSolutionSetMap;
	private MapContract updateDeltaSetMap;
	
	// Initial Bulk Iteration Contracts
	private MatchContract bulkDependenciesMatch;
	private ReduceContract bulkUpdateReduce;
	private MatchContract bulkComparisonMatch;
	private MapContract bulkUpdateSolutionSetMap;
	private MapContract bulkUpdateDeltaSetMap;
	
	// Contract Parameters to be used by both the Bulk and the Delta Plans
	private Class<? extends MatchStub> dependencyUdf;
	private Class<? extends Key> dependencyKeyClass;
	private int dependencyKeyCol1;
	private int dependencyKeyCol2;
	
	private Class<? extends ReduceStub> updateUdf;
	private Class<? extends Key> updateKeyClass;
	private int updateKeyColumn;
	
	private Class<? extends MatchStub> comparisonUdf;
	private Class<? extends Key> comparisonKeyClass;
	private int comparisonKeyCol1;
	private int comparisonkeyCol2;
	
	private Class<? extends MapStub> solutionSetMapUdf;
	private Class<? extends MapStub> workSetMapUdf;
	
	
	public DeltaIterationPlan(GenericDataSink sink, String jobName, int keyPosition) {
		super(sink, jobName);
		iteration = new DeltaIterationContract(keyPosition, jobName);
	}
	
	public DeltaIterationPlan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
		iteration = new DeltaIterationContract(0, jobName);
	}

	@Override
	public void setUpDeltaIteration(Contract initialSolutionSet, Contract initialWorkSet, GenericDataSource<?> dependencySet) {
		
		this.initialSolutionSet = initialSolutionSet;
		this.initialWorkSet = initialWorkSet;
		iteration.setDependencySet(dependencySet);
	}


	@Override
	public void setUpDependenciesMatch(Class<? extends MatchStub> udf,
			Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		
		bulkDependenciesMatch = MatchContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(initialWorkSet)
				.input2(iteration.getDependencySet())
				.name("Join WorkSet with Dependency Set (Bulk)")
				.build();
		
		setUpDependencyMatchParams(udf, keyClass, keyColumn1, keyColumn2);

	}

	private void setUpDependencyMatchParams(Class<? extends MatchStub> udf,
			Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		
		dependencyUdf = udf;
		dependencyKeyClass = keyClass;
		dependencyKeyCol1 = keyColumn1;
		dependencyKeyCol2 = keyColumn2;
	}

	@Override
	public void setUpUpdateReduce(Class<? extends ReduceStub> udf, Class<? extends Key> keyClass, int keyColumn) {
		
		bulkUpdateReduce = ReduceContract.builder(udf, keyClass, keyColumn)
				.input(bulkDependenciesMatch)
				.name("Update Function (Bulk)")
				.build();	
		
		setUpUpdateReduceParams(udf, keyClass, keyColumn);
	}

	private void setUpUpdateReduceParams(Class<? extends ReduceStub> udf, Class<? extends Key> keyClass, int keyColumn) {
		
		updateUdf = udf;
		updateKeyClass = keyClass;
		updateKeyColumn = keyColumn;
	}

	@Override
	public void setUpComparisonMatch(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		
		bulkComparisonMatch = MatchContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(bulkUpdateReduce)
				.input2(initialSolutionSet)
				.name("Join with Solution Set (Bulk)")
				.build();	
		
		setUpComparisonMatchParam(udf, keyClass, keyColumn1, keyColumn2);

	}

	private void setUpComparisonMatchParam(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		
		comparisonUdf = udf;
		comparisonKeyClass = keyClass;
		comparisonKeyCol1 = keyColumn1;
		comparisonkeyCol2 = keyColumn2;		
	}

	@Override
	public void setUpUpdateSolutionSetMap(Class<? extends MapStub> udf) {
		
		bulkUpdateSolutionSetMap = MapContract.builder(udf)
				.input(bulkComparisonMatch)
				.name("Update SolutionSet Map (Bulk)")
				.build();
		
		// write intermediate output
		// create DataSinkContract for writing the intermediate ranks
		final String outputDelta = "file:///home/hinata/Desktop/intermediateSS.out";
		FileDataSink resultDelta = new FileDataSink(RecordOutputFormat.class, outputDelta, bulkUpdateSolutionSetMap, "Intermediate Ranks");
		RecordOutputFormat.configureRecordFormat(resultDelta)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactLong.class, 0)
			.field(PactDouble.class, 1);
		
		setUpSolutionSetMapParams(udf);	
	}

	private void setUpSolutionSetMapParams(Class<? extends MapStub> udf) {
		
		solutionSetMapUdf = udf;		
	}

	@Override
	public void setUpUpdateDeltaSetMap(Class<? extends MapStub> udf) {
		
		bulkUpdateDeltaSetMap = MapContract.builder(udf)
				.input(bulkComparisonMatch)
				.name("Update DeltaSet Map (Bulk)")
				.build();
		
		// write intermediate output
		// create DataSinkContract for writing the intermediate ranks
		final String outputDelta = "file:///home/hinata/Desktop/intermediateD.out";
		FileDataSink resultDelta = new FileDataSink(RecordOutputFormat.class, outputDelta, bulkUpdateDeltaSetMap, "Intermediate Ranks");
		RecordOutputFormat.configureRecordFormat(resultDelta)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactLong.class, 0)
			.field(PactDouble.class, 1);
		
		setUpWorkSetMapParams(udf);
		}

	private void setUpWorkSetMapParams(Class<? extends MapStub> udf) {
		
		workSetMapUdf = udf;		
	}

	/**
	 * connects the bulk iteration to the deltaIteration
	 * and updates the iteration parameters
	 * 
	 */
	@Override
	public void assemble() {
		
		iteration.setInitialSolutionSet(bulkUpdateSolutionSetMap);
		iteration.setInitialWorkset(bulkUpdateDeltaSetMap);
		
		//setup rest of the contracts
		dependenciesMatch = MatchContract.builder(dependencyUdf, dependencyKeyClass, dependencyKeyCol1, dependencyKeyCol2)
				.input1(iteration.getWorkset())
				.input2(iteration.getDependencySet())
				.name("Join WorkSet with Dependency Set")
				.build();
		
		updateReduce = ReduceContract.builder(updateUdf, updateKeyClass, updateKeyColumn)
				.input(dependenciesMatch)
				.name("Update Function")
				.build();
		
		comparisonMatch = MatchContract.builder(comparisonUdf, comparisonKeyClass, comparisonKeyCol1, comparisonkeyCol2)
				.input1(updateReduce)
				.input2(iteration.getSolutionSet())
				.name("Join with Solution Set")
				.build();
		
	/*	updateSolutionSetMap = MapContract.builder(solutionSetMapUdf)
				.input(comparisonMatch)
				.name("Update SolutionSet Map")
				.build(); */
		
		updateDeltaSetMap = MapContract.builder(workSetMapUdf)
				.input(comparisonMatch)
				.name("Update DeltaSet Map")
				.build();	
		
		iteration.setNextWorkset(updateDeltaSetMap);
		iteration.setSolutionSetDelta(comparisonMatch);	
		
		// set the input of the DataSink
		this.getDataSinks().iterator().next().addInput(this.iteration);		
	}
	
	private Contract getIteration() throws PlanException {
		if(iteration.isConfigured()) return iteration;
		else throw new PlanException("The Delta Iteration is not properly configured");
	}

	public void setMaxIterations(int maxIterations){
		iteration.setMaximumNumberOfIterations(maxIterations);
	}

}
