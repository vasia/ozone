package eu.stratosphere.pact.example.incremental.pagerank;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

public class InitialRankAndDeltaInputFormat extends TextInputFormat {
	
	private final PactLong vId = new PactLong();
	private final PactDouble initialRank = new PactDouble();
	private final PactDouble initialDelta = new PactDouble(0.0);
	
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
		String str = new String(bytes, offset, numBytes);
		String[] parts = str.split("\\s+");

		this.vId.setValue(Long.parseLong(parts[0]));
		this.initialRank.setValue(Double.parseDouble(parts[1]));
		
		target.setField(0, this.vId);
		target.setField(1, this.initialRank);
		target.setField(2, this.initialDelta);
		return true;
	}

}