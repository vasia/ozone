package eu.stratosphere.pact.incremental.contracts;

import eu.stratosphere.pact.generic.contract.WorksetIteration;

public class IncrementalIteration extends WorksetIteration {

	public IncrementalIteration(int keyPosition) {
		super(keyPosition);
	}

}
