package eu.stratosphere.pact.example.incremental;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

// join on w.vid = d.srcId and emits d.trgId
public final class FindCandidatesMatchLong extends MatchStub {

	private final PactRecord result = new PactRecord();
		
	@Override
	public void match(PactRecord wRecord, PactRecord dRecord, Collector<PactRecord> out) throws Exception {
		this.result.setField(0, dRecord.getField(1, PactLong.class));
		out.collect(this.result);	
	}

}
