package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.plan.Plan;

public class BulkIterationPlan extends Plan {

	public BulkIterationPlan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

}
