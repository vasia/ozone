package eu.stratosphere.pact.example.incremental.pagerank;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

@Combinable
@ConstantFields(0)
public class UpdateRankReduce extends ReduceStub {
	
	private final PactRecord result = new PactRecord();
	private final PactLong vertexId = new PactLong();
	private final PactDouble newRank = new PactDouble();
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {
		
		final PactRecord first = records.next();
		long vId = first.getField(0, PactLong.class).getValue();
		long firstOutLinks = first.getField(1, PactLong.class).getValue();
		double firstRank = first.getField(2, PactDouble.class).getValue();

		double rankSum = (double)(firstRank / firstOutLinks);
		
		while (records.hasNext()) {
			long outLinks = records.next().getField(1, PactLong.class).getValue();
			double rank = records.next().getField(2, PactDouble.class).getValue();
			
			rankSum += (double)(rank / outLinks);
		}
		
		this.newRank.setValue(rankSum);
		this.vertexId.setValue(vId);
		this.result.setField(0, vertexId);
		this.result.setField(1, newRank);
		out.collect(result);

	}

}
