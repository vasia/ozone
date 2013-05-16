package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.generic.contract.Contract;

/**
 * 
* interface that defines the methods to configure and assemble 
* a delta iteration plan
*
*/
public interface DeltaIterationPlanner {
	
	public void setDeltaIteration(Contract initialSolutionSet, Contract initialWorkSet, GenericDataSource<?> dependencySet, int keyPosition, String jobName);
	
	public void setCandidatesMatch(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2);
	
	public void setUpCoGroup(Class<? extends CoGroupStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2);
	
	// finalizes the plan
	public void assemble();

}
