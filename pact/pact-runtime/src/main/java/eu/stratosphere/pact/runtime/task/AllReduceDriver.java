/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.stub.GenericReducer;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.MutableToRegularIteratorWrapper;

/**
 * Reduce task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a ReduceStub
 * implementation.
 * <p>
 * The ReduceTask creates a iterator over all records from its input. The iterator returns all records grouped by their
 * key. The iterator is handed to the <code>reduce()</code> method of the ReduceStub.
 * 
 * @see ReduceStub
 */
public class AllReduceDriver<IT, OT> implements PactDriver<GenericReducer<IT, OT>, OT> {
	
	private static final Log LOG = LogFactory.getLog(AllReduceDriver.class);

	private PactTaskContext<GenericReducer<IT, OT>, OT> taskContext;
	
	private MutableObjectIterator<IT> input;

	private TypeSerializer<IT> serializer;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GenericReducer<IT, OT>, OT> context) {
		this.taskContext = context;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GenericReducer<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericReducer<IT, OT>> clazz = (Class<GenericReducer<IT, OT>>) (Class<?>) GenericReducer.class;
		return clazz;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void prepare() throws Exception {
		final TaskConfig config = this.taskContext.getTaskConfig();
		if (config.getDriverStrategy() != DriverStrategy.ALL_GROUP) {
			throw new Exception("Unrecognized driver strategy for AllReduce driver: " + config.getDriverStrategy().name());
		}
		this.serializer = this.taskContext.getInputSerializer(0);
		this.input = this.taskContext.getInput(0);
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("AllReduce preprocessing done. Running Reducer code."));
		}

		final GenericReducer<IT, OT> stub = this.taskContext.getStub();
		final Collector<OT> output = this.taskContext.getOutputCollector();
		final MutableToRegularIteratorWrapper<IT> inIter = new MutableToRegularIteratorWrapper<IT>(this.input, this.serializer);
		
		// single UDF call with the single group
		stub.reduce(inIter, output);
	}

	@Override
	public void cleanup() throws Exception {
	}

	@Override
	public void cancel() {
	}
}