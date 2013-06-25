package eu.stratosphere.pact.example.incremental;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

@Combinable
@ConstantFields(0)
public final class CCUpdateCmpIdReduce extends ReduceStub {

	private final PactRecord result = new PactRecord();
	private final PactLong vertexId = new PactLong();
	private final PactLong minComponentId = new PactLong();
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {

		final PactRecord first = records.next();
		final long vertexID = first.getField(0, PactLong.class).getValue();
		
		long minimumComponentID = first.getField(1, PactLong.class).getValue();

		while (records.hasNext()) {
			long candidateComponentID = records.next().getField(1, PactLong.class).getValue();
			if (candidateComponentID < minimumComponentID) {
				minimumComponentID = candidateComponentID;
			}
		}
		
		this.vertexId.setValue(vertexID);
		this.minComponentId.setValue(minimumComponentID);
		this.result.setField(0, this.vertexId);
		this.result.setField(1, this.minComponentId);
		out.collect(this.result);
	}
}