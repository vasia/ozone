package eu.stratosphere.pact.example.incremental.pagerank;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

public class PRDependenciesComputationMatch extends MatchStub {

	private final PactRecord result = new PactRecord();
	private final PactDouble partRank = new PactDouble();
	
	/*
	 * 
	 * (vId, rank) x (srcId, trgId, weight) => (trgId, rank / weight)
	 * 
	 */
	@Override
	public void match(PactRecord vertexWithRank, PactRecord edgeWithWeight, Collector<PactRecord> out) throws Exception {
		result.setField(0, edgeWithWeight.getField(1, PactLong.class));
		final long outLinks = edgeWithWeight.getField(2, PactLong.class).getValue();
		final double rank = vertexWithRank.getField(1, PactDouble.class).getValue();
		partRank.setValue(rank / (double) outLinks);
		result.setField(1, partRank);
		out.collect(result);
	}
}
