package eu.stratosphere.pact.incremental.plans;

import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.incremental.contracts.BulkIterationGeneric;

public class BulkIterationPlan extends Plan {

	public BulkIterationPlan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

	public BulkIterationPlan(GenericDataSink sink, String jobName, BulkIterationGeneric bulkIterationGeneric) {
		super(sink, jobName);
		//configure BulkIterationPlan
	}

}
