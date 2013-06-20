package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.generic.contract.Contract;

/**
 * 
* interface that defines the methods to configure and assemble 
* a delta iteration plan
*
*/
public interface DeltaIterationPlanner {
	
	public void setUpDeltaIteration(Contract initialSolutionSet, Contract initialWorkSet, GenericDataSource<?> dependencySet, int keyPosition, String jobName);
	
	public void setUpDependenciesMatch(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2);

	public void setUpUpdateReduce(Class<? extends ReduceStub> udf, Class<? extends Key> keyClass, int keyColumn);
	
	public void setUpComparisonMatch(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2);

	public void setUpUpdateSolutionSetMap(Class<? extends MapStub> udf);
	
	public void setUpUpdateDeltaSetMap(Class<? extends MapStub> udf);
	
	// finalizes the plan
	public void assemble();

}
