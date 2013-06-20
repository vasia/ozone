package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanException;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
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
	private MatchContract dependenciesMatch;
	private ReduceContract updateReduce;
	private MatchContract comparisonMatch;
	private MapContract updateSolutionSetMap;
	private MapContract updateDeltaSetMap;
	
	
	public DeltaIterationPlan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

	@Override
	public void setUpDeltaIteration(Contract initialSolutionSet,
			Contract initialWorkSet, GenericDataSource<?> dependencySet, int keyPosition, String jobName) {

		iteration = new DeltaIterationContract(keyPosition, jobName);
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
	public void setUpUpdateSolutionSetMap(Class<? extends MapStub> udf) {
		updateSolutionSetMap = MapContract.builder(udf)
				.input(comparisonMatch)
				.name("Update SolutionSet Map")
				.build();		
	}

	@Override
	public void setUpUpdateDeltaSetMap(Class<? extends MapStub> udf) {
		updateDeltaSetMap = MapContract.builder(udf)
				.input(comparisonMatch)
				.name("Update DeltaSet Map")
				.build();		}

	/**
	 * creates one bulk iteration to initialize the deltas
	 * and finalizes the deltaIteration
	 * 
	 */
	@Override
	public void assemble() {
		//TODO: one bulk + connection
	}
	
	public Contract getIteration() throws PlanException {
		if(this.iteration.isConfigured()) return iteration;
		else throw new PlanException("The dependency Iteration is not properly configured -- Forgot to assemble?");
		
	}

}
