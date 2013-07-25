package eu.stratosphere.pact.example.incremental.pagerank;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

@Combinable
@ConstantFields(0)
public class UpdateRankReduce extends ReduceStub {
	
	private final PactDouble newRank = new PactDouble();
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {
		
		double rankSum = 0.0;
		double rank;
		PactRecord rec = null;
		
		while (records.hasNext()) {
			rec = records.next();
			rank = rec.getField(1, PactDouble.class).getValue();
			
			rankSum += rank;
		}
		
		newRank.setValue(rankSum);
		rec.setField(1, newRank);
		out.collect(rec);

	}

}
