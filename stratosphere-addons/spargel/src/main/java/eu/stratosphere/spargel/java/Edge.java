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


import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;


public final class Edge<VertexKey extends Key, EdgeValue extends Value> {
	
	private VertexKey target;
	private EdgeValue edgeValue;
	
	void set(VertexKey target, EdgeValue edgeValue) {
		this.target = target;
		this.edgeValue = edgeValue;
	}
	
	public VertexKey target() {
		return target;
	}
	
	public EdgeValue edgeValue() {
		return edgeValue;
	}
}