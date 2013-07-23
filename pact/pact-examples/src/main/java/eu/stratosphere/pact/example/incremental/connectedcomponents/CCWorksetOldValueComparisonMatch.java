package eu.stratosphere.pact.example.incremental.connectedcomponents;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

@ConstantFieldsFirst(0)
public final class CCWorksetOldValueComparisonMatch extends MatchStub {

	@Override
	public void match(PactRecord newVertexWithComponent, PactRecord currentVertexWithComponent, Collector<PactRecord> out){

		long candidateComponentID = newVertexWithComponent.getField(1, PactLong.class).getValue();
		long currentComponentID = currentVertexWithComponent.getField(1, PactLong.class).getValue();

		if (candidateComponentID < currentComponentID)
			out.collect(newVertexWithComponent);
	}
}