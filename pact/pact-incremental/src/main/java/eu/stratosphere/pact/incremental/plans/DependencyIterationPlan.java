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
import eu.stratosphere.pact.incremental.contracts.DependencyIterationContract;

/**
 *	This is the Plan for a Workset (Dependency) Iteration
 *	The internal plan of the DependencyterationContract is built as follows:
 * 
 * 
 *	 	=========== *Match*: oldValueComparison
 *		|	|			|		|
 *		|	|			|		|
 *		|	|			|	*Reduce*: valuesUpdate (aggregate update function)
 *		|	|			|					|
 *		|	|			|					|
 *		|	|			|	*Match*: dependencies computation			|
 *		|	|			|		|						|
 *		|	|			|		|						|
 *		|	=========> S: solutionSet					|
 *		|												|
 *		|												|
 *		|						*Match*: compute the dependencies of the Candidates
 *		|									|					|
 *		|									|					|
 *		|						*Reduce*: group Candidates		|
 *		|									|					|
 *		|									|					|
 *		|						*Match*: find Candidates 		|
 *		|								for re-computation		|
 *		|							|					|		|
 * 		|							|					|		|
 *		===================== W: workSet			D: dependencySet	
 *
 */

public class DependencyIterationPlan extends Plan implements DependencyIterationPlanner{

	private DependencyIterationContract iteration;
	private MatchContract candidatesMatch;
	private ReduceContract candidatesReduce;
	private MatchContract candidatesDependenciesMatch;
	private MatchContract dependenciesMatch;
	private ReduceContract updateReduce;
	private MatchContract comparisonMatch;
	
	public DependencyIterationPlan(GenericDataSink sink, String jobName, int keyPosition) {
		super(sink, jobName);
		iteration = new DependencyIterationContract(keyPosition, jobName);
	}

	public DependencyIterationPlan(FileDataSink sink, String jobName) {
		super(sink, jobName);
		iteration = new DependencyIterationContract(0, jobName);	//AVK: there is no constructor for the WorksetIteration Contract without a keyPosition arg. Is there a reason for that?
	}

	@Override
	public void setUpDependencyIteration(GenericDataSource<?> initialSolutionSet,
			GenericDataSource<?> initialWorkSet, GenericDataSource<?> dependencySet) {

		iteration.setDependencySet(dependencySet);
		iteration.setInitialSolutionSet(initialSolutionSet);
		iteration.setInitialWorkset(initialWorkSet);	
	}

	@Override
	public void setUpCandidatesMatch(Class<? extends MatchStub> udf,
			Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		candidatesMatch = MatchContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(iteration.getWorkset())
				.input2(iteration.getDependencySet())
				.name("Join WorkSet with Dependency Set")
				.build();
	}

	@Override
	public void setUpCandidatesReduce(Class<? extends ReduceStub> udf,
			Class<? extends Key> keyClass, int keyColumn) {
		candidatesReduce = ReduceContract.builder(udf, keyClass, keyColumn)
		.input(candidatesMatch)
		.name("Group Candidates")
		.build();
	}
	
	@Override
	public void setUpCandidatespDependenciesMatch(Class<? extends MatchStub> udf, Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		candidatesDependenciesMatch = MatchContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(candidatesReduce)
				.input2(iteration.getDependencySet())
				.name("Join Candidates with Dependency Set")
				.build();
	}

	@Override
	public void setUpDependenciesMatch(Class<? extends MatchStub> udf,
			Class<? extends Key> keyClass, int keyColumn1, int keyColumn2) {
		dependenciesMatch = MatchContract.builder(udf, keyClass, keyColumn1, keyColumn2)
				.input1(candidatesDependenciesMatch)
				.input2(iteration.getSolutionSet())
				.name("Join Solution Set with Candidates' Dependencies")
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
	
	public void setMaxIterations(int maxIterations){
		iteration.setMaximumNumberOfIterations(maxIterations);
	}

	public Contract getIteration() throws PlanException {
		if(iteration.isConfigured()) return iteration;
		else throw new PlanException("The dependency Iteration is not properly configured -- Forgot to assemble?");
		
	}

}
