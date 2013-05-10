package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.plan.Plan;

public class IncrementalIterationPlan extends Plan {

	public IncrementalIterationPlan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

}
