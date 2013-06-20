package eu.stratosphere.pact.incremental.plans;

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
import eu.stratosphere.pact.incremental.contracts.BulkIterationContract;

/**
 *	This is the Plan for a Bulk Iteration
 *	The internal plan of the BulkIterationContract is built as follows:
 * 
 * 
 * 		======== *Match*: oldValueComparison
 *		|				|		|
 *		|				|		|
 *		|				|	*Reduce*: valuesUpdate (aggregate update function)
 *		|				|					|
 *		|				|					|
 *		|				|	*Match*: dependencies computation
 *		|				|	|							|
 *		|				|	|							|
 *		=========> S: solutionSet					D: dependencySet
 *
 *
 *		
 *
 */

public class BulkIterationPlan extends Plan implements BulkIterationPlanner {
	
	private BulkIterationContract iteration;
	private MatchContract dependencyMatch;
	private ReduceContract updateReduce;
	private MatchContract comparisonMatch;

	public BulkIterationPlan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

	@Override
	public void setUpBulkIteration(GenericDataSource<?> initialSolutionSet,
			GenericDataSource<?> dependencySet, int keyPosition) {

		iteration = new BulkIterationContract(keyPosition);
		iteration.setDependencySet(dependencySet);
		iteration.setInitialSolutionSet(initialSolutionSet);	
	}

	@Override
	public void setUpDependenciesMatch(Class<? extends MatchStub> udf,
			Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		dependencyMatch = MatchContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(iteration.getPartialSolution())
				.input2(iteration.getDependencySet())
				.name("Join SolutionSet with Dependency Set")
				.build();		
	}

	@Override
	public void setUpUpdateReduce(Class<? extends ReduceStub> udf,
			Class<? extends Key> keyClass, int keyColumn) {
		updateReduce = ReduceContract.builder(udf, keyClass, keyColumn)
				.input(dependencyMatch)
				.name("Update Function")
				.build();	
	}

	@Override
	public void setUpComparisonMatch(Class<? extends MatchStub> udf,
			Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		comparisonMatch = MatchContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(updateReduce)
				.input2(iteration.getPartialSolution())
				.name("Comparison with Solution Set")
				.build();
	}

	@Override
	public void assemble() {
		iteration.setNextPartialSolution(comparisonMatch);		
	}
	
	public Contract getIteration() throws PlanException {
		if(iteration.isConfigured()) return iteration;
		else throw new PlanException("The Bulk Iteration is not properly configured -- Forgot to assemble?");
	}

}
