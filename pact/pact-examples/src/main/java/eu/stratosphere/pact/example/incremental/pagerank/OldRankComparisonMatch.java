package eu.stratosphere.pact.example.incremental.pagerank;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.type.PactRecord;

@ConstantFieldsFirst(0)
public class OldRankComparisonMatch extends MatchStub {

	@Override
	public void match(PactRecord newRank, PactRecord oldRank, Collector<PactRecord> out) throws Exception {
		out.collect(newRank);
	}

}
