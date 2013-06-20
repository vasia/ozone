package eu.stratosphere.pact.incremental.contracts;

import eu.stratosphere.pact.common.plan.PlanException;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.WorksetIteration;

/**
 *
 * Generic Workset / Dependency Iteration
 * for each element x in S that changes value, 
 * we add in the WorkList all the elements of S that depend on x
 * and recompute only these in the next iteration 
 * 
 */
public class DependencyIterationContract extends WorksetIteration {
	
	public DependencyIterationContract(int keyPosition, String name) {
		super(keyPosition, name);
	}

	private Contract dependencySet;	//the Dependency set input
	
	/**
	 * Sets the contract of the dependency set
	 * 
	 * @param delta The contract representing the dependencies / graph structure
	 */
	public void setDependencySet(Contract dependencies) {
		dependencySet = dependencies;
	}
	
	/**
	 * Gets the contract that has been set as the dependency set
	 * 
	 * @return The contract that has been set as the dependency set.
	 */
	public Contract getDependencySet() {
		return dependencySet;
	}

	/**
	 * checks if the dependency iteration has been configured properly
	 */
	public boolean isConfigured() {
		if (getDependencySet()== null)
			throw new PlanException("The dependency Set is empty");
		else if(getInitialWorkset() == null)
			throw new PlanException("The initial WorkSet is empty");
		else if(getInitialSolutionSet() == null)
			throw new PlanException("The initial SolutionSet is empty");
		else if(getNextWorkset() == null)
			throw new PlanException("Next WorkSet is empty");
		else if(getSolutionSetDelta() == null)
			throw new PlanException("SolutionSetDelta is empty");
		else
			return true;			
	}

}
