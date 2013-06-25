package eu.stratosphere.pact.example.incremental.connectedcomponents;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public final class CCDependenciesComputationMatch extends MatchStub {

	private final PactRecord result = new PactRecord();

	@Override
	public void match(PactRecord vertexWithComponent, PactRecord edge, Collector<PactRecord> out) {
		this.result.setField(0, edge.getField(1, PactLong.class));
		this.result.setField(1, vertexWithComponent.getField(1, PactLong.class));
		out.collect(this.result);
	}
}
