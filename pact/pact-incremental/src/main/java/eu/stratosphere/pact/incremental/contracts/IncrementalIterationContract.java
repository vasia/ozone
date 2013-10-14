package eu.stratosphere.pact.incremental.contracts;

import eu.stratosphere.pact.common.plan.PlanException;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.WorksetIteration;

/**
 * 
 * Contract for Incremental (Optimized Workset) Iterations
 *
 */

public class IncrementalIterationContract extends WorksetIteration {

	public IncrementalIterationContract(int keyPosition, String name) {
		super(keyPosition, name);
	}
	
	private Contract dependencySet;	//the Dependency set input
	
	/**
	 * Sets the contract of the dependency set
	 * 
	 * @param dependencies The contract representing the dependencies / graph structure
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
	 * @return true if properly configured
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
