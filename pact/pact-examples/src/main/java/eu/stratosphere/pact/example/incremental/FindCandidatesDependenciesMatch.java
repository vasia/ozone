package eu.stratosphere.pact.example.incremental;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;

public final class FindCandidatesDependenciesMatch extends MatchStub {
	
	@Override
	public void match(PactRecord wRecord, PactRecord dRecord, Collector<PactRecord> out) throws Exception {
		out.collect(dRecord);
	}

}
