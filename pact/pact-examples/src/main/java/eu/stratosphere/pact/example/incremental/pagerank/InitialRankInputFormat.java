package eu.stratosphere.pact.example.incremental.pagerank;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import java.util.regex.Pattern;

public class InitialRankInputFormat extends TextInputFormat {
	
private static final Pattern SEPARATOR = Pattern.compile("[,\t ]");
	
	private final PactLong vId = new PactLong();
	private final PactDouble initialRank = new PactDouble();
	
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
		String str = new String(bytes, offset, numBytes);
		String[] parts = SEPARATOR.split(str);

		this.vId.setValue(Long.parseLong(parts[0]));
		this.initialRank.setValue(Double.parseDouble(parts[1]));
		
		target.setField(0, this.vId);
		target.setField(1, this.initialRank);
		return true;
	}

}
