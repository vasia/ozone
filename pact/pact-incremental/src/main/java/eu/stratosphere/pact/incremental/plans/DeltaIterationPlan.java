package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.plan.Plan;

public class DeltaIterationPlan extends Plan {

	public DeltaIterationPlan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

}
