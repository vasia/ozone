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

package eu.stratosphere.pact.compiler.operators;

import java.util.List;

import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.plan.SingleInputNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;

/**
 * 
 */
public abstract class OperatorDescriptorSingle implements AbstractOperatorDescriptor {
	
	protected final FieldSet keys;			// the set of key fields
	protected final FieldList keyList;		// the key fields with ordered field positions
	
	private List<RequestedGlobalProperties> globalProps;
	private List<RequestedLocalProperties> localProps;
	
	
	protected OperatorDescriptorSingle() {
		this(null);
	}
	
	protected OperatorDescriptorSingle(FieldSet keys) {
		this.keys = keys;
		this.keyList = keys == null ? null : keys.toFieldList();
	}
	
	
	public List<RequestedGlobalProperties> getPossibleGlobalProperties() {
		if (this.globalProps == null) {
			this.globalProps = createPossibleGlobalProperties();
		}
		return this.globalProps;
	}
	
	public List<RequestedLocalProperties> getPossibleLocalProperties() {
		if (this.localProps == null) {
			this.localProps = createPossibleLocalProperties();
		}
		return this.localProps;
	}
	
	protected abstract List<RequestedGlobalProperties> createPossibleGlobalProperties();
	
	protected abstract List<RequestedLocalProperties> createPossibleLocalProperties();
	
	public abstract SingleInputPlanNode instantiate(Channel in, SingleInputNode node);
	
	public abstract GlobalProperties computeGlobalProperties(GlobalProperties in);
	
	public abstract LocalProperties computeLocalProperties(LocalProperties in);
}
