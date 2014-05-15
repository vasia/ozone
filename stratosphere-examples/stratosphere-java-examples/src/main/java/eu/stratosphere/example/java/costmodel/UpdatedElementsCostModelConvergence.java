package eu.stratosphere.example.java.costmodel;

import eu.stratosphere.api.common.aggregators.ConvergenceCriterion;
import eu.stratosphere.types.LongValue;


@SuppressWarnings("serial")
public class UpdatedElementsCostModelConvergence implements ConvergenceCriterion<LongValue> {

	/**
	 * value: updated elements during last iteration
	 * the cost model returns true if ( 3 * value / |SolutionSet| ) <= 1 / ( d + 1 )
	 * where d is the average node degree of the dependency graph
	 */
	@Override
	public boolean isConverged(int iteration, LongValue value) {		
		System.out.println("[convergence check] Elements: " + value.getValue());
		return (value.getValue() < 5);
	}

}
