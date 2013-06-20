package eu.stratosphere.pact.incremental.contracts;

import eu.stratosphere.pact.common.plan.PlanException;
import eu.stratosphere.pact.common.stubs.aggregators.Aggregator;
import eu.stratosphere.pact.common.stubs.aggregators.ConvergenceCriterion;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.generic.contract.BulkIteration;
import eu.stratosphere.pact.generic.contract.Contract;


public class BulkIterationContract extends BulkIteration {
	
	private Contract dependencySet;
	private static String dependencySetKeyIndex = "DEPENDENCYSET_KEY_INDEX" ;

	public BulkIterationContract(String name){
		super(name);
	}
	
	public BulkIterationContract(int keyPosition) {
		dependencySet.setParameter(dependencySetKeyIndex, keyPosition); 
	}
	
	/**
	 * @param solutionSet
	 */
	public void setInitialSolutionSet(Contract solutionSet) {
		this.setInput(solutionSet);
	}
	
	/**
	 * @param depSet
	 * @param keyIndex
	 */
	public void setDependencySet(Contract depSet, int keyIndex) {
		dependencySet = depSet;
		dependencySet.setParameter(dependencySetKeyIndex, keyIndex);
	}
	

	/**
	 * @param dependencySet
	 */
	public void setDependencySet(Contract depSet) {
		dependencySet = depSet;
	}

	
	/**
	 * Gets the contract that has been set as the dependency set
	 * 
	 * @return The contract that has been set as the dependency set.
	 */
	public Contract getDependencySet() {
		return this.dependencySet;
	}
	
	/**
	 * @param <T>
	 * @param criterion
	 */
	public <T extends Value> void setConvergenceCriterion(String name, Class<? extends Aggregator<T>> aggregator,Class<? extends ConvergenceCriterion<T>> convergenceCheck) {
		this.getAggregators().registerAggregationConvergenceCriterion(name, aggregator, convergenceCheck);
	}

	/**
	 * checks if the bulk iteration has been configured properly
	 */
	public boolean isConfigured() {
		validate();	// validation of the parent class
		if (this.getDependencySet()== null)
			throw new PlanException("The dependency Set is empty");
		else
			return true;			
	}

}
