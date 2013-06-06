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

package eu.stratosphere.pact.compiler.postpass;

import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;

/**
 * Interface for visitors that process the optimizer's plan. Typical post processing applications are schema
 * finalization or the generation/parameterization of utilities for the actual data model.
 */
public interface OptimizerPostPass {
	
	/**
	 * Central post processing function. Invoked by the optimizer after the best plan has
	 * been determined.
	 * 
	 * @param plan The plan to be post processed.
	 */
	void postPass(OptimizedPlan plan);
}
