package eu.stratosphere.pact.example.incremental;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;

public final class GroupCandidatesReduce extends ReduceStub {

	@Override
	public void reduce(Iterator<PactRecord> candidates, Collector<PactRecord> out) throws Exception {
		out.collect(candidates.next());
	}
}
