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

package eu.stratosphere.pact.generic.contract;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.Visitor;

/**
 * Abstract contract superclass for for all contracts that have two inputs, like "match" or "cross".
 */
public abstract class DualInputContract<T extends Stub> extends AbstractPact<T> {
	
	/**
	 * The contract producing the first input.
	 */
	protected final List<Contract> input1 = new ArrayList<Contract>();
	/**
	 * The contract producing the second input.
	 */
	protected final List<Contract> input2 = new ArrayList<Contract>();

	/**
	 * The positions of the keys in the tuples of the first input.
	 */
	private final int[] keyFields1;
	
	/**
	 * The positions of the keys in the tuples of the second input.
	 */
	private final int[] keyFields2;
	
	// --------------------------------------------------------------------------------------------

	public DualInputContract() {
		this(null, null);
	}
	
	/**
	 * Creates a new abstract dual-input Pact with the given name wrapping the given user function.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param stubClass The class containing the user function.
	 */
	protected DualInputContract(UserCodeWrapper<T> stub, String name) {
		super(stub, name);
		this.keyFields1 = this.keyFields2 = new int[0];
	}
	
	/**
	 * Creates a new abstract dual-input Pact with the given name wrapping the given user function.
	 * This constructor is specialized only for Pacts that require no keys for their processing.
	 * 
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 * @param keyTypes The classes of the data types that act as keys in this stub.
	 * @param stubClass The class containing the user function.
	 */
	protected DualInputContract(UserCodeWrapper<T> stub, int[] keyPositions1, int[] keyPositions2, String name) {
		super(stub, name);
		this.keyFields1 = keyPositions1;
		this.keyFields2 = keyPositions2;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the first input, or null, if none is set.
	 * 
	 * @return The contract's first input.
	 */
	public List<Contract> getFirstInputs() {
		return this.input1;
	}
	
	/**
	 * Returns the second input, or null, if none is set.
	 * 
	 * @return The contract's second input.
	 */
	public List<Contract> getSecondInputs() {
		return this.input2;
	}
	
	/**
	 * Removes all inputs from this contract's first input.
	 */
	public void clearFirstInputs() {
		this.input1.clear();
	}
	
	/**
	 * Removes all inputs from this contract's second input.
	 */
	public void clearSecondInputs() {
		this.input2.clear();
	}

	/**
	 * Connects the first input to the task wrapped in this contract.
	 * 
	 * @param input The contract will be set as the first input.
	 */
	public void addFirstInput(Contract ... input) {
		for (Contract c : input) {
			if (c == null) {
				throw new IllegalArgumentException("The input may not contain null elements.");
			} else {
				this.input1.add(c);
			}
		}
	}
	
	/**
	 * Connects the second input to the task wrapped in this contract.
	 * 
	 * @param input The contract will be set as the second input.
	 */
	public void addSecondInput(Contract ... input) {
		for (Contract c : input) {
			if (c == null) {
				throw new IllegalArgumentException("The input may not contain null elements.");
			} else {
				this.input2.add(c);
			}
		}
	}

	/**
	 * Connects the first inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that is connected as the first inputs.
	 */
	public void addFirstInputs(List<Contract> inputs) {
		for (Contract c : inputs) {
			if (c == null) {
				throw new IllegalArgumentException("The input may not contain null elements.");
			} else {
				this.input1.add(c);
			}
		}
	}

	/**
	 * Connects the second inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that is connected as the second inputs.
	 */
	public void addSecondInputs(List<Contract> inputs) {
		for (Contract c : inputs) {
			if (c == null) {
				throw new IllegalArgumentException("The input may not contain null elements.");
			} else {
				this.input2.add(c);
			}
		}
	}
	
	/**
	 * Clears all previous connections and connects the first input to the task wrapped in this contract
	 * 
	 * @param firstInput The contract that is connected as the first input.
	 */
	public void setFirstInput(Contract input) {
		this.input1.clear();
		addFirstInput(input);
	}

	/**
	 * Clears all previous connections and connects the second input to the task wrapped in this contract
	 * 
	 * @param secondInput The contract that is connected as the second input.
	 */
	public void setSecondInput(Contract input) {
		this.input2.clear();
		addSecondInput(input);
	}
	
	/**
	 * Clears all previous connections and connects the first input to the task wrapped in this contract
	 * 
	 * @param firstInput The contract that is connected as the first input.
	 */
	public void setFirstInput(Contract ... input) {
		this.input1.clear();
		addFirstInput(input);
	}

	/**
	 * Clears all previous connections and connects the second input to the task wrapped in this contract
	 * 
	 * @param secondInput The contract that is connected as the second input.
	 */
	public void setSecondInput(Contract ... input) {
		this.input2.clear();
		addSecondInput(input);
	}
	
	/**
	 * Clears all previous connections and connects the first inputs to the task wrapped in this contract
	 * 
	 * @param inputs The contracts that is connected as the first inputs.
	 */
	public void setFirstInputs(List<Contract> inputs) {
		this.input1.clear();
		addFirstInputs(inputs);
	}

	/**
	 * Clears all previous connections and connects the second inputs to the task wrapped in this contract
	 * 
	 * @param secondInput The contracts that is connected as the second inputs.
	 */
	public void setSecondInputs(List<Contract> inputs) {
		this.input2.clear();
		addSecondInputs(inputs);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public final int getNumberOfInputs() {
		return 2;
	}

	@Override
	public int[] getKeyColumns(int inputNum) {
		if (inputNum == 0) {
			return this.keyFields1;
		}
		else if (inputNum == 1) {
			return this.keyFields2;
		}
		else throw new IndexOutOfBoundsException();
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		boolean descend = visitor.preVisit(this);
		if (descend) {
			for (Contract c : this.input1) {
				c.accept(visitor);
			}
			for (Contract c : this.input2) {
				c.accept(visitor);
			}
			visitor.postVisit(this);
		}
	}
}
