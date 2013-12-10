package eu.stratosphere.pact.example.incremental;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecond;
import eu.stratosphere.pact.common.type.PactRecord;

@ConstantFieldsSecond({0, 1, 2})
public final class FindCandidatesDependenciesMatch extends MatchStub {
	
	@Override
	public void match(PactRecord wRecord, PactRecord dRecord, Collector<PactRecord> out) throws Exception {
		out.collect(dRecord);
	}

}
