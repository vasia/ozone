/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler.operators;

import java.util.Collections;
import java.util.List;

import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.dag.SinkJoiner;
import eu.stratosphere.compiler.dag.TwoInputNode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.SinkJoinerPlanNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 *
 */
public class UtilSinkJoinOpDescriptor extends OperatorDescriptorDual {
	
	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.BINARY_NO_OP;
	}
	
	@Override
	protected List<GlobalPropertiesPair> createPossibleGlobalProperties() {
		// all properties are possible
		return Collections.singletonList(new GlobalPropertiesPair(
			new RequestedGlobalProperties(), new RequestedGlobalProperties()));
	}

	@Override
	protected List<LocalPropertiesPair> createPossibleLocalProperties() {
		// all properties are possible
		return Collections.singletonList(new LocalPropertiesPair(
			new RequestedLocalProperties(), new RequestedLocalProperties()));
	}
	
	@Override
	public boolean areCoFulfilled(RequestedLocalProperties requested1, RequestedLocalProperties requested2,
			LocalProperties produced1, LocalProperties produced2)
	{
		return true;
	}

	@Override
	public DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node) {
		if (node instanceof SinkJoiner) {
			return new SinkJoinerPlanNode((SinkJoiner) node, in1, in2);
		} else {
			throw new CompilerException();
		}
	}

	@Override
	public LocalProperties computeLocalProperties(LocalProperties in1, LocalProperties in2) {
		return new LocalProperties();
	}

	@Override
	public GlobalProperties computeGlobalProperties(GlobalProperties in1, GlobalProperties in2) {
		return GlobalProperties.combine(in1, in2);
	}
}
