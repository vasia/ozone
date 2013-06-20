package eu.stratosphere.pact.incremental.contracts;

import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.WorksetIteration;

/**
 * Delta Iteration
 * The functionality is identical to WorkSet Iteration
 * but only deltas are propagated 
 *
 */
public class DeltaIterationContract extends WorksetIteration {

	public DeltaIterationContract(int keyPosition, String name) {
		super(keyPosition, name);
	}

private Contract dependencySet;	//the Dependency set input
	
	/**
	 * Sets the contract of the dependency set
	 * 
	 * @param delta The contract representing the dependencies / graph structure
	 */
	public void setDependencySet(Contract dependencies) {
		this.dependencySet = dependencies;
	}
	
	/**
	 * Gets the contract that has been set as the dependency set
	 * 
	 * @return The contract that has been set as the dependency set.
	 */
	public Contract getDependencySet() {
		return this.dependencySet;
	}

}
