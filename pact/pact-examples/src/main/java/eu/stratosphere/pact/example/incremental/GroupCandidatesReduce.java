package eu.stratosphere.pact.example.incremental;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;

@ConstantFields(0)
public final class GroupCandidatesReduce extends ReduceStub {

	@Override
	public void reduce(Iterator<PactRecord> candidates, Collector<PactRecord> out) throws Exception {
		if (candidates.hasNext()) out.collect(candidates.next());
	}
}
