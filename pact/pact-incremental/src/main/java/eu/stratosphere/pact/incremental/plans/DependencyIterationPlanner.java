package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;

/**
 * 
 * interface that defines the methods to configure and assemble 
 * a dependency iteration plan
 *
 */

public interface DependencyIterationPlanner {

	public void setUpDependencyIteration(GenericDataSource<?> initialSolutionSet, 
			GenericDataSource<?> initialWorkSet, GenericDataSource<?> dependencySet);
	
	public void setUpCandidatesMatch(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2);
	
	public void setUpCandidatesReduce(Class<? extends ReduceStub> udf, Class<? extends Key> keyClass, int keyColumn);
	
	public void setUpCandidatespDependenciesMatch(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2);
	
	public void setUpDependenciesMatch(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2);
	
	public void setUpUpdateReduce(Class<? extends ReduceStub> udf, Class<? extends Key> keyClass, int keyColumn);
	
	public void setUpComparisonMatch(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2);
	
	// finalizes the plan
	public void assemble();
}
