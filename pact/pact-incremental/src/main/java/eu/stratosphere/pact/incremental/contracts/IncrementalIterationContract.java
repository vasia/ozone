package eu.stratosphere.pact.incremental.contracts;

import eu.stratosphere.pact.generic.contract.WorksetIteration;

public class IncrementalIterationContract extends WorksetIteration {

	public IncrementalIterationContract(int keyPosition) {
		super(keyPosition);
	}

}
