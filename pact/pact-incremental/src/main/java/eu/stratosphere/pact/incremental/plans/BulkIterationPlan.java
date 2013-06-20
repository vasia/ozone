package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.incremental.contracts.BulkIterationContract;

/**
 *	This is the Plan for a Bulk Iteration
 *	The internal plan of the BulkIterationContract is built as follows:
 * 
 * 
 * 		======== Match:	oldValueComparison
 *		|				|		|
 *		|				|		|
 *		|				|	Reduce:	valuesUpdate (aggregate update function)
 *		|				|					|
 *		|				|					|
 *		|				|	Match:	dependencies computation
 *		|				|	|							|
 *		|				|	|							|
 *		=========> S: solutionSet					D: dependencySet
 *
 *
 *		
 *
 */

public class BulkIterationPlan extends Plan {
	
	private BulkIterationContract iteration;
	private MatchContract dependencyMatch;
	private ReduceContract updateReduce;
	private MatchContract comparisonMatch;

	public BulkIterationPlan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

	public void setUpBulkIteration(GenericDataSource<?> initialSolutionSet,
			GenericDataSource<?> dependencySet, int keyPosition) {

		iteration = new BulkIterationContract(keyPosition);
		iteration.setDependencySet(dependencySet);
		iteration.setInitialSolutionSet(initialSolutionSet);	
	}
	
	/**
	 * @param dependencyMatch
	 */
	public void setUpDependenciesMatch(MatchContract dependencyMatch){
		;
	}
	
	/**
	 * @param updateReduce
	 */
	public void setUpUpdateReduce(ReduceContract updateReduce){
		;
	}
	
	
	/**
	 * @param comparisonMatch
	 */
	public void setUpComparisonMatch(MatchContract comparisonMatch){
		;
	}
	

}
