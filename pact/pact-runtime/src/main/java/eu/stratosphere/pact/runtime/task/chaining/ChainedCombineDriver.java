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

package eu.stratosphere.pact.runtime.task.chaining;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.stub.GenericReducer;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.sort.AsynchronousPartialSorterCollector;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger.InputDataCollector;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

public class ChainedCombineDriver<T> extends ChainedDriver<T, T> {
	
	private InputDataCollector<T> inputCollector;
	
	private volatile Exception exception;
	
	private GenericReducer<T, ?> combiner;
	
	private AsynchronousPartialSorterCollector<T> sorter;
	
	private CombinerThread combinerThread;
	
	private AbstractInvokable parent;
	
	private ClassLoader userCodeClassLoader;
	
	private volatile boolean canceled;
	
	// --------------------------------------------------------------------------------------------

    @Override
    public void setup(AbstractInvokable parent, ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = userCodeClassLoader;
		this.parent = parent;

		@SuppressWarnings("unchecked")
		final GenericReducer<T, ?> combiner = RegularPactTask.instantiateUserCode(config, userCodeClassLoader, GenericReducer.class);
        combiner.setRuntimeContext(getRuntimeContext(parent, taskName));
        this.combiner = combiner;
    }

    @Override
	public void openTask() throws Exception {
		// open the stub first
		final Configuration stubConfig = this.config.getStubParameters();
		RegularPactTask.openUserCode(this.combiner, stubConfig);
		
		// ----------------- Set up the asynchronous sorter -------------------------
		
		final long availableMemory = this.config.getMemoryDriver();
		final DriverStrategy ds = this.config.getDriverStrategy();
		
		final MemoryManager memoryManager = this.parent.getEnvironment().getMemoryManager();
		
		// instantiate the serializer / comparator
		final TypeSerializerFactory<T> serializerFactory = this.config.getInputSerializer(0, this.userCodeClassLoader);
		final TypeComparatorFactory<T> comparatorFactory = this.config.getDriverComparator(0, this.userCodeClassLoader);
		final TypeSerializer<T> serializer = serializerFactory.getSerializer();
		final TypeComparator<T> comparator = comparatorFactory.createComparator();

		switch (ds) {
			// local strategy is COMBININGSORT
			// The Input is combined using a sort-merge strategy. Before spilling on disk, the data volume is reduced using
			// the combine() method of the ReduceStub.
			// An iterator on the sorted, grouped, and combined pairs is created and returned
			case PARTIAL_GROUP:
				this.sorter = new AsynchronousPartialSorterCollector<T>(memoryManager, this.parent,
						serializer, comparator.duplicate(), availableMemory);
				this.inputCollector = this.sorter.getInputCollector();
				break;
			default:
				throw new RuntimeException("Invalid local strategy provided for CombineTask.");
		}
		
		// ----------------- Set up the combiner thread -------------------------
		
		this.combinerThread = new CombinerThread(this.sorter, serializer, comparator, this.combiner, this.outputCollector);
		this.combinerThread.start();
		if (this.parent != null) {
			this.parent.userThreadStarted(this.combinerThread);
		}
	}

	@Override
	public void closeTask() throws Exception {
		// wait for the thread that runs the combiner to finish
		while (!this.canceled && this.combinerThread.isAlive()) {
			try {
				this.combinerThread.join();
			}
			catch (InterruptedException iex) {
				cancelTask();
				throw iex;
			}
		}
		
		if (this.parent != null && this.combinerThread != null) {
			this.parent.userThreadFinished(this.combinerThread);
		}
		
		if (this.exception != null) {
			throw new ExceptionInChainedStubException(this.taskName, this.exception);
		}
		
		this.sorter.close();
		
		if (this.canceled)
			return;
		
		RegularPactTask.closeUserCode(this.combiner);
	}

	@Override
	public void cancelTask() {
		this.canceled = true;
		this.exception = new Exception("Task has been canceled");
		
		this.combinerThread.cancel();
		this.inputCollector.close();
		this.sorter.close();
		
		try {
			this.combinerThread.join();
		} catch (InterruptedException iex) {
			// do nothing, just leave
		} finally {
			if (this.parent != null && this.combinerThread != null) {
				this.parent.userThreadFinished(this.combinerThread);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------

	public Stub getStub() {
		return this.combiner;
	}

	public String getTaskName() {
		return this.taskName;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public void collect(T record) {
		if (this.exception != null)
			throw new RuntimeException("The combiner failed due to an exception.", 
				this.exception.getCause() == null ? this.exception : this.exception.getCause());
		
		this.inputCollector.collect(record);
	}

	@Override
	public void close() {
		this.inputCollector.close();
		
		if (this.exception != null)
			throw new RuntimeException("The combiner failed due to an exception.", 
				this.exception.getCause() == null ? this.exception : this.exception.getCause());
	}
	
	// --------------------------------------------------------------------------------------------
	
	private final class CombinerThread extends Thread {
		
		private final AsynchronousPartialSorterCollector<T> sorter;
		
		private final TypeSerializer<T> serializer;
		
		private final TypeComparator<T> comparator;
		
		private final GenericReducer<T, ?> stub;
		
		private final Collector<T> output;
		
		private volatile boolean running;
		
		
		private CombinerThread(AsynchronousPartialSorterCollector<T> sorter,
				TypeSerializer<T> serializer, TypeComparator<T> comparator, 
				GenericReducer<T, ?> stub, Collector<T> output)
		{
			super("Combiner Thread");
			setDaemon(true);
			
			this.sorter = sorter;
			this.serializer = serializer;
			this.comparator = comparator;
			this.stub = stub;
			this.output = output;
			this.running = true;
		}

		public void run() {
			try {
				MutableObjectIterator<T> iterator = null;
				while (iterator == null) {
					try {
						iterator = this.sorter.getIterator();
					}
					catch (InterruptedException iex) {
						if (!this.running)
							return;
					}
				}
				
				final KeyGroupedIterator<T> keyIter = new KeyGroupedIterator<T>(iterator, this.serializer, this.comparator);
				
				// cache references on the stack
				final GenericReducer<T, ?> stub = this.stub;
				final Collector<T> output = this.output;

				// run stub implementation
				while (this.running && keyIter.nextKey()) {
					stub.combine(keyIter.getValues(), output);
				}
			}
			catch (Throwable t) {
				ChainedCombineDriver.this.exception = new Exception("The combiner failed due to an exception.", t);
			}
		}
		
		public void cancel() {
			this.running = false;
			this.interrupt();
		}
	}
}