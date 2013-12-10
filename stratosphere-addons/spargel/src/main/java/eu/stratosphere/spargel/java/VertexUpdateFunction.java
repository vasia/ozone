/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.spargel.java;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.IterationRuntimeContext;
import eu.stratosphere.pact.common.stubs.aggregators.Aggregator;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

/**
 * 
 * <VertexKey> The vertex key type.
 * <VertexValue> The vertex value type.
 * <Message> The message type.
 */
public abstract class VertexUpdateFunction<VertexKey extends Key, VertexValue extends Value, Message extends Value> implements Serializable {

	// --------------------------------------------------------------------------------------------
	//  Public API Methods
	// --------------------------------------------------------------------------------------------
	
	public abstract void updateVertex(VertexKey vertexKey, VertexValue vertexValue, Iterator<Message> inMessages) throws Exception;
	
	public void setup(Configuration config) throws Exception {}
	
	public void preSuperstep() throws Exception {}
	
	public void postSuperstep() throws Exception {}
	
	public void setNewVertexValue(VertexValue newValue) {
		outVal.setField(1, newValue);
		out.collect(outVal);
	}
	
	public int getSuperstep() {
		return this.runtimeContext.getSuperstepNumber();
	}
	
	public <T extends Value> Aggregator<T> getIterationAggregator(String name) {
		return this.runtimeContext.<T>getIterationAggregator(name);
	}
	
	public <T extends Value> T getPreviousIterationAggregate(String name) {
		return this.runtimeContext.<T>getPreviousIterationAggregate(name);
	}
	
	// --------------------------------------------------------------------------------------------
	//  internal methods
	// --------------------------------------------------------------------------------------------
	
	private IterationRuntimeContext runtimeContext;
	
	private Collector<PactRecord> out;
	
	private PactRecord outVal;
	
	
	void init(IterationRuntimeContext context) {
		this.runtimeContext = context;
	}
	
	void setOutput(PactRecord val, Collector<PactRecord> out) {
		this.out = out;
		this.outVal = val;
	}
	
	// serializability
	private static final long serialVersionUID = 1L;
}
