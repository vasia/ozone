package eu.stratosphere.pact.example.incremental.pagerank;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class PRDependenciesComputationMatch extends MatchStub {

	private final PactRecord result = new PactRecord();
	
	/*
	 * 
	 * (vId, rank) x (srcId, trgId, weight) => (trgId, weight, rank)
	 * 
	 */
	@Override
	public void match(PactRecord vertexWithRank, PactRecord edgeWithWeight, Collector<PactRecord> out) throws Exception {
		this.result.setField(0, edgeWithWeight.getField(1, PactLong.class));
		this.result.setField(1, edgeWithWeight.getField(2, PactLong.class));
		this.result.setField(2, vertexWithRank.getField(1, PactLong.class));
		out.collect(this.result);
		
	}

}
