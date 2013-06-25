package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanException;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.incremental.contracts.IncrementalIterationContract;

/**
 *	This is the Plan for an Incremental (Optimized Workset) Iteration
 *	The internal plan of the DependencyIterationContract is built as follows:
 * 
 * 
 *	 	=========== *Match*: oldValueComparison
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
 *		|============================> W: workSet		D: dependencySet
 *
 */

public class IncrementalIterationPlan extends Plan implements IncrementalIterationPlanner {

	private IncrementalIterationContract iteration;
	private MatchContract dependenciesMatch;
	private ReduceContract updateReduce;
	private MatchContract comparisonMatch;
	
	public IncrementalIterationPlan(GenericDataSink sink, String jobName, int keyPosition) {
		super(sink, jobName);
		iteration = new IncrementalIterationContract(keyPosition, jobName);
	}

	public IncrementalIterationPlan(FileDataSink sink, String jobName) {
		super(sink, jobName);
		iteration = new IncrementalIterationContract(0, jobName);
	}

	@Override
	public void setUpIncrementalIteration(
			GenericDataSource<?> initialSolutionSet, GenericDataSource<?> initialWorkSet, GenericDataSource<?> dependencySet) {
		
		iteration.setDependencySet(dependencySet);
		iteration.setInitialSolutionSet(initialSolutionSet);
		iteration.setInitialWorkset(initialWorkSet);	
	}


	@Override
	public void setUpDependenciesMatch(Class<? extends MatchStub> udf,
			Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		dependenciesMatch = MatchContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(iteration.getWorkset())
				.input2(iteration.getDependencySet())
				.name("Join WorkSet with Dependency Set")
				.build();
		
	}

	
	@Override
	public void setUpUpdateReduce(Class<? extends ReduceStub> udf,
			Class<? extends Key> keyClass, int keyColumn) {
		updateReduce = ReduceContract.builder(udf, keyClass, keyColumn)
				.input(dependenciesMatch)
				.name("Update Function")
				.build();		
	}


	@Override
	public void setUpComparisonMatch(Class<? extends MatchStub> udf,
			Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		comparisonMatch = MatchContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(updateReduce)
				.input2(iteration.getSolutionSet())
				.name("Join with Solution Set")
				.build();		
	}


	@Override
	public void assemble() {
		iteration.setNextWorkset(comparisonMatch);
		iteration.setSolutionSetDelta(comparisonMatch);		
		this.getDataSinks().iterator().next().addInput(getIteration());
	}
	
	public Contract getIteration() throws PlanException {
		if(iteration.isConfigured()) return this.iteration;
		else throw new PlanException("The dependency Iteration is not properly configured -- Forgot to assemble?");
		
	}

	public void setMaxIterations(int maxIterations) {
		iteration.setMaximumNumberOfIterations(maxIterations);		
	}
}
