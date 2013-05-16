package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.incremental.contracts.DeltaIteration;
import eu.stratosphere.pact.incremental.contracts.DependencyIteration;

/**
 * 
 * Delta Iteration Plan
 * In order to initialize the deltas
 * one bulk iteration is executed first
 * and then connected to the delta plan
 *
 */
public class DeltaIterationPlan extends Plan implements DeltaIterationPlanner {

	private DeltaIteration iteration;
	private MatchContract candidatesMatch;
	private CoGroupContract updateCoGroup;
	
	
	public DeltaIterationPlan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

	@Override
	public void setDeltaIteration(Contract initialSolutionSet,
			Contract initialWorkSet, GenericDataSource<?> dependencySet, int keyPosition, String jobName) {
		
		iteration = new DeltaIteration(keyPosition, jobName);
		iteration.setDependencySet(dependencySet);
		iteration.setInitialSolutionSet(initialSolutionSet);
		iteration.setInitialWorkset(initialWorkSet);	
		
	}

	@Override
	public void setCandidatesMatch(Class<? extends MatchStub> udf,
			Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		candidatesMatch = MatchContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(iteration.getWorkset())
				.input2(iteration.getDependencySet())
				.name("Join WorkSet with Dependency Set")
				.build();
	}

	@Override
	public void setUpCoGroup(Class<? extends CoGroupStub> udf,
			Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		updateCoGroup = CoGroupContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(candidatesMatch)
				.input2(iteration.getSolutionSet())
				.name("Update CoGroup")
				.build();	
	}

	/**
	 * creates one bulk iteration to initialize the deltas
	 * and finalizes the deltaIteration
	 * 
	 */
	@Override
	public void assemble() {
		//TODO: HowTo have 2 outputs from updateCoGroup?
		// One for the PS and one for the WS containing only deltas? 
	}

}
