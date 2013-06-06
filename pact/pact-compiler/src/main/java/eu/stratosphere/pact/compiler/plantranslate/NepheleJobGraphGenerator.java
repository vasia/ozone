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

package eu.stratosphere.pact.compiler.plantranslate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobOutputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.pact.common.stubs.aggregators.AggregatorWithName;
import eu.stratosphere.pact.common.stubs.aggregators.ConvergenceCriterion;
import eu.stratosphere.pact.common.stubs.aggregators.LongSumAggregator;
import eu.stratosphere.pact.common.util.PactConfigConstants;
import eu.stratosphere.pact.common.util.Visitor;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.plan.TempMode;
import eu.stratosphere.pact.compiler.plan.candidate.BulkIterationPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.IterationPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.BulkPartialSolutionPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SolutionSetPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SourcePlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.UnionPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.WorksetIterationPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.WorksetPlanNode;
import eu.stratosphere.pact.generic.contract.AggregatorRegistry;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion;
import eu.stratosphere.pact.runtime.iterative.io.FakeOutputTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationSynchronizationSinkTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.JoinWithSolutionSetMatchDriver.SolutionSetFirstJoinDriver;
import eu.stratosphere.pact.runtime.task.MatchDriver;
import eu.stratosphere.pact.runtime.task.NoOpDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.JoinWithSolutionSetMatchDriver.SolutionSetSecondJoinDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedDriver;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * This component translates the optimizer's resulting plan a nephele job graph. The
 * translation is a one to one mapping. All decisions are made by the optimizer, this class
 * simply creates nephele data structures and descriptions corresponding to the optimizer's
 * result.
 * <p>
 * The basic method of operation is a top down traversal over the plan graph. On the way down, tasks are created
 * for the plan nodes, on the way back up, the nodes connect their predecessor.
 */
public class NepheleJobGraphGenerator implements Visitor<PlanNode> {
	
	public static final String MERGE_ITERATION_AUX_TASKS_KEY = "pact.compiler.merge-iteration-aux";
	
	private static final boolean mergeIterationAuxTasks = GlobalConfiguration.getBoolean(
		MERGE_ITERATION_AUX_TASKS_KEY, true);
	
	private static final Log LOG = LogFactory.getLog(NepheleJobGraphGenerator.class);
	
	// ------------------------------------------------------------------------

	private JobGraph jobGraph; // the job that is currently built

	private Map<PlanNode, AbstractJobVertex> vertices; // a map from optimizer nodes to nephele vertices
	
	private Map<PlanNode, TaskInChain> chainedTasks; // a map from optimizer nodes to nephele vertices
	
	private Map<IterationPlanNode, IterationDescriptor> iterations;
	
	private List<TaskInChain> chainedTasksInSequence;
	
	private List<AbstractJobVertex> auxVertices; // auxiliary vertices which are added during job graph generation

	private AbstractJobVertex maxDegreeVertex; // the vertex with the highest degree of parallelism
	
	private final int defaultMaxFan;
	
	private final float defaultSortSpillingThreshold;
	
	private int iterationIdEnumerator = 1;
	
	private IterationPlanNode currentIteration;	// hack: as long as no nesting is possible, remember the enclosing iteration
	
	// ------------------------------------------------------------------------

	/**
	 * Creates a new job graph generator that uses the default values for its resource configuration.
	 */
	public NepheleJobGraphGenerator() {
		this.defaultMaxFan = PactConfigConstants.DEFAULT_SPILLING_MAX_FAN;
		this.defaultSortSpillingThreshold = PactConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD;
	}
	
	public NepheleJobGraphGenerator(Configuration config) {
		this.defaultMaxFan = config.getInteger(PactConfigConstants.DEFAULT_SPILLING_MAX_FAN_KEY, 
				PactConfigConstants.DEFAULT_SPILLING_MAX_FAN);
		this.defaultSortSpillingThreshold = config.getFloat(PactConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD_KEY,
			PactConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD);
	}

	/**
	 * Translates a {@link eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan} into a
	 * {@link eu.stratosphere.nephele.jobgraph.JobGraph}.
	 * This is an 1-to-1 mapping. No optimization whatsoever is applied.
	 * 
	 * @param pactPlan
	 *        Optimized PACT plan that is translated into a JobGraph.
	 * @return JobGraph generated from PACT plan.
	 */
	public JobGraph compileJobGraph(OptimizedPlan pactPlan) {
		this.jobGraph = new JobGraph(pactPlan.getJobName());
		this.vertices = new HashMap<PlanNode, AbstractJobVertex>();
		this.chainedTasks = new HashMap<PlanNode, TaskInChain>();
		this.chainedTasksInSequence = new ArrayList<TaskInChain>();
		this.auxVertices = new ArrayList<AbstractJobVertex>();
		this.iterations = new HashMap<IterationPlanNode, IterationDescriptor>();
		this.maxDegreeVertex = null;
		
		// generate Nephele job graph
		pactPlan.accept(this);
		
		// now that the traversal is done, we have the chained tasks write their configs into their
		// parents' configurations
		for (int i = 0; i < this.chainedTasksInSequence.size(); i++) {
			TaskInChain tic = this.chainedTasksInSequence.get(i);
			TaskConfig t = new TaskConfig(tic.getContainingVertex().getConfiguration());
			t.addChainedTask(tic.getChainedTask(), tic.getTaskConfig(), tic.getTaskName());
		}
		
		// finalize the iterations
		for (IterationDescriptor iteration : this.iterations.values()) {
			if (iteration.getIterationNode() instanceof BulkIterationPlanNode) {
				finalizeBulkIteration(iteration);
			} else if (iteration.getIterationNode() instanceof WorksetIterationPlanNode) {
				finalizeWorksetIteration(iteration);
			} else {
				throw new CompilerException();
			}
		}

		// now that all have been created, make sure that all share their instances with the one
		// with the highest degree of parallelism
		if (pactPlan.getInstanceTypeName() != null) {
			this.maxDegreeVertex.setInstanceType(pactPlan.getInstanceTypeName());
		} else {
			LOG.warn("No instance type assigned to Nephele JobVertex.");
		}
		for (AbstractJobVertex vertex : this.vertices.values()) {
			if (vertex != this.maxDegreeVertex) {
				vertex.setVertexToShareInstancesWith(this.maxDegreeVertex);
			}
		}
		
		for (AbstractJobVertex vertex : this.auxVertices) {
			if (vertex != this.maxDegreeVertex) {
				vertex.setVertexToShareInstancesWith(this.maxDegreeVertex);
			}
		}

		JobGraph graph = this.jobGraph;

		// release all references again
		this.maxDegreeVertex = null;
		this.vertices = null;
		this.chainedTasks = null;
		this.chainedTasksInSequence = null;
		this.auxVertices = null;
		this.iterations = null;
		this.jobGraph = null;

		// return job graph
		return graph;
	}
	
	/**
	 * This methods implements the pre-visiting during a depth-first traversal. It create the job vertex and
	 * sets local strategy.
	 * 
	 * @param node
	 *        The node that is currently processed.
	 * @return True, if the visitor should descend to the node's children, false if not.
	 * @see eu.stratosphere.pact.common.util.Visitor#preVisit(eu.stratosphere.pact.common.plan.Visitable)
	 */
	@Override
	public boolean preVisit(PlanNode node) {
		// check if we have visited this node before. in non-tree graphs, this happens
		if (this.vertices.containsKey(node) || this.chainedTasks.containsKey(node)) {
			// return false to prevent further descend
			return false;
		}

		// the vertex to be created for the current node
		final AbstractJobVertex vertex;
		try {
			if (node instanceof SinkPlanNode) {
				vertex = createDataSinkVertex((SinkPlanNode) node);
			}
			else if (node instanceof SourcePlanNode) {
				vertex = createDataSourceVertex((SourcePlanNode) node);
			}
			else if (node instanceof BulkIterationPlanNode) {
				BulkIterationPlanNode iterationNode = (BulkIterationPlanNode) node;
				// for the bulk iteration, we skip creating anything for now. we create the graph
				// for the step function in the post visit.
				
				// check that the root of the step function has the same DOP as the iteration.
				// because the tail must have the same DOP as the head, we can only merge the last
				// operator with the tail, if they have the same DOP. not merging is currently not
				// implemented
				PlanNode root = iterationNode.getRootOfStepFunction();
				if (root.getDegreeOfParallelism() != node.getDegreeOfParallelism() || 
						root.getSubtasksPerInstance() != node.getSubtasksPerInstance()) 
				{
					throw new CompilerException("It is currently not supported that the final operator of the step " +
							"function has a different degree of parallelism than the iteration operator itself.");
				}
				
				IterationDescriptor descr = new IterationDescriptor(iterationNode, this.iterationIdEnumerator++);
				this.iterations.put(iterationNode, descr);
				vertex = null;
			}
			else if (node instanceof WorksetIterationPlanNode) {
				WorksetIterationPlanNode iterationNode = (WorksetIterationPlanNode) node;

				// we have the same constraints as for the bulk iteration
				PlanNode nextWorkSet = iterationNode.getNextWorkSetPlanNode();
				PlanNode solutionSetDelta  = iterationNode.getSolutionSetDeltaPlanNode();
				
				if (nextWorkSet.getDegreeOfParallelism() != node.getDegreeOfParallelism() || 
					nextWorkSet.getSubtasksPerInstance() != node.getSubtasksPerInstance())
				{
					throw new CompilerException("It is currently not supported that the final operator of the step " +
							"function has a different degree of parallelism than the iteration operator itself.");
				}
				if (solutionSetDelta.getDegreeOfParallelism() != node.getDegreeOfParallelism() || 
					solutionSetDelta.getSubtasksPerInstance() != node.getSubtasksPerInstance())
				{
					throw new CompilerException("It is currently not supported that the final operator of the step " +
							"function has a different degree of parallelism than the iteration operator itself.");
				}
				
				IterationDescriptor descr = new IterationDescriptor(iterationNode, this.iterationIdEnumerator++);
				this.iterations.put(iterationNode, descr);
				vertex = null;
			}
			else if (node instanceof SingleInputPlanNode) {
				vertex = createSingleInputVertex((SingleInputPlanNode) node);
			}
			else if (node instanceof DualInputPlanNode) {
				vertex = createDualInputVertex((DualInputPlanNode) node);
			}
			else if (node instanceof UnionPlanNode) {
				// skip the union for now
				vertex = null;
			}
			else if (node instanceof BulkPartialSolutionPlanNode) {
				// create a head node (or not, if it is merged into its successor)
				vertex = createBulkIterationHead((BulkPartialSolutionPlanNode) node);
			}
			else if (node instanceof SolutionSetPlanNode) {
				// skip the solution set place holder. we create the head at the workset place holder
				vertex = null;
			}
			else if (node instanceof WorksetPlanNode) {
				// create the iteration head here
				vertex = createWorksetIterationHead((WorksetPlanNode) node);
			}
			else {
				throw new CompilerException("Unrecognized node type: " + node.getClass().getName());
			}
		}
		catch (Exception e) {
			throw new CompilerException("Error translating node '" + node + "': " + e.getMessage(), e);
		}
		
		// check if a vertex was created, or if it was chained or skipped
		if (vertex != null) {
			// set degree of parallelism
			int pd = node.getDegreeOfParallelism();
			vertex.setNumberOfSubtasks(pd);
	
			// check whether this is the vertex with the highest degree of parallelism
			if (this.maxDegreeVertex == null || this.maxDegreeVertex.getNumberOfSubtasks() < pd) {
				this.maxDegreeVertex = vertex;
			}
	
			// set the number of tasks per instance
			if (node.getSubtasksPerInstance() >= 1) {
				vertex.setNumberOfSubtasksPerInstance(node.getSubtasksPerInstance());
			}
			
			// check whether this vertex is part of an iteration step function
			if (this.currentIteration != null) {
				// check that the task has the same DOP as the iteration as such
				PlanNode iterationNode = (PlanNode) this.currentIteration;
				if (iterationNode.getDegreeOfParallelism() != pd) {
					throw new CompilerException("Error: All functions that are part of an iteration must have the same degree-of-parallelism as that iteration.");
				}
				if (iterationNode.getSubtasksPerInstance() != node.getSubtasksPerInstance()) {
					throw new CompilerException("Error: All functions that are part of an iteration must have the same subtasks-per-instance as that iteration.");
				}
				
				// store the id of the iterations the step functions participate in
				IterationDescriptor descr = this.iterations.get(this.currentIteration);
				new TaskConfig(vertex.getConfiguration()).setIterationId(descr.getId());
			}
	
			// store in the map
			this.vertices.put(node, vertex);
		}

		// returning true causes deeper descend
		return true;
	}

	/**
	 * This method implements the post-visit during the depth-first traversal. When the post visit happens,
	 * all of the descendants have been processed, so this method connects all of the current node's
	 * predecessors to the current node.
	 * 
	 * @param node
	 *        The node currently processed during the post-visit.
	 * @see eu.stratosphere.pact.common.util.Visitor#postVisit(eu.stratosphere.pact.common.plan.Visitable)
	 */
	@Override
	public void postVisit(PlanNode node) {
		try {
			// --------- check special cases for which we handle post visit differently ----------
			
			// skip data source node (they have no inputs)
			// also, do nothing for union nodes, we connect them later when gathering the inputs for a task
			// solution sets have no input. the initial solution set input is connected when the iteration node is in its postVisit
			if (node instanceof SourcePlanNode || node instanceof UnionPlanNode) {
				return;
			}
			
			// check if we have an iteration. in that case, translate the step function now
			if (node instanceof IterationPlanNode) {
				// for now, prevent nested iterations
				if (this.currentIteration != null) {
					throw new CompilerException("Nested Iterations are not possible at the moment!");
				}
				this.currentIteration = (IterationPlanNode) node;
				this.currentIteration.acceptForStepFunction(this);
				this.currentIteration = null;
				
				// inputs for initial bulk partial solution or initial workset are already connected to the iteration head in the head's post visit.
				// connect the initial solution set now.
				if (node instanceof WorksetIterationPlanNode) {
					// connect the initial solution set
					WorksetIterationPlanNode wsNode = (WorksetIterationPlanNode) node;
					AbstractJobVertex headVertex = this.iterations.get(wsNode).getHeadTask();
					TaskConfig headConfig = new TaskConfig(headVertex.getConfiguration());
					int inputIndex = headConfig.getDriverStrategy().getNumInputs();
					headConfig.setIterationHeadSolutionSetInputIndex(inputIndex);
					translateChannel(wsNode.getInitialSolutionSetInput(), inputIndex, headVertex, headConfig);
				}
				
				return;
			} else if (node instanceof SolutionSetPlanNode) {
				// this represents an access into the solution set index.
				// add the necessary information to all nodes that access the index
				if (node.getOutgoingChannels().size() != 1) {
					throw new CompilerException("Currently, only one join with the solution set is allowed.");
				}
				
				Channel c = node.getOutgoingChannels().get(0);
				DualInputPlanNode target = (DualInputPlanNode) c.getTarget();
				AbstractJobVertex accessingVertex = this.vertices.get(target);
				TaskConfig conf = new TaskConfig(accessingVertex.getConfiguration());
				int inputNum = c == target.getInput1() ? 0 : c == target.getInput2() ? 1 : -1;
				
				// sanity checks
				if (inputNum == -1) {
					throw new CompilerException();
				}
				if (!conf.getDriver().equals(MatchDriver.class)) {
					throw new CompilerException("Found join with solution set using incompatible operator.");
				}
				
				// adjust the driver and set the serializer / comparator information
				conf.setDriver(inputNum == 0 ? SolutionSetFirstJoinDriver.class : SolutionSetSecondJoinDriver.class);
				conf.setSolutionSetSerializer(((SolutionSetPlanNode) node).getContainingIterationNode().getSolutionSetSerializer());
				
				// hack: for now, we need the prober in the workset iteration head task
				IterationDescriptor iter = this.iterations.get(((SolutionSetPlanNode) node).getContainingIterationNode());
				TaskConfig headConf = iter.getHeadConfig();
				
				TypeSerializerFactory<?> otherSerializer;
				TypeComparatorFactory<?> otherComparator;
				if (inputNum == 0) {
					otherSerializer = target.getInput2().getSerializer();
					otherComparator = target.getComparator2();
				} else {
					otherSerializer = target.getInput1().getSerializer();
					otherComparator = target.getComparator1();
				}
				headConf.setSolutionSetProberSerializer(otherSerializer);
				headConf.setSolutionSetProberComparator(otherComparator);
				headConf.setSolutionSetPairComparator(target.getPairComparator());
				
				return;
			}
			
			// --------- Main Path: Translation of channels ----------
			// 
			// There are two paths of translation: One for chained tasks (or merged tasks in general),
			// which do not have their own task vertex. The other for tasks that have their own vertex,
			// or are the primary task in a vertex (to which the others are chained).
			
			final AbstractJobVertex targetVertex = this.vertices.get(node);
			
			// check whether this node has its own task, or is merged with another one
			if (targetVertex == null) {
				// node's task is merged with another task. it is either chained, of a merged head vertex
				// from an iteration
				final TaskInChain chainedTask;
				if ((chainedTask = this.chainedTasks.get(node)) != null) {
					// Chained Task. Sanity check first...
					final Iterator<Channel> inConns = node.getInputs();
					if (!inConns.hasNext()) {
						throw new CompilerException("Bug: Found chained task with no input.");
					}
					final Channel inConn = inConns.next();
					
					if (inConns.hasNext()) {
						throw new CompilerException("Bug: Found a chained task with more than one input!");
					}
					if (inConn.getLocalStrategy() != null && inConn.getLocalStrategy() != LocalStrategy.NONE) {
						throw new CompilerException("Bug: Found a chained task with an input local strategy.");
					}
					if (inConn.getShipStrategy() != null && inConn.getShipStrategy() != ShipStrategyType.FORWARD) {
						throw new CompilerException("Bug: Found a chained task with an input ship strategy other than FORWARD.");
					}
	
					AbstractJobVertex container = chainedTask.getContainingVertex();
					
					if (container == null) {
						final PlanNode sourceNode = inConn.getSource();
						container = this.vertices.get(sourceNode);
						if (container == null) {
							// predecessor is itself chained
							container = this.chainedTasks.get(sourceNode).getContainingVertex();
							if (container == null)
								throw new IllegalStateException("Bug: Chained task predecessor has not been assigned its containing vertex.");
						} else {
							// predecessor is a proper task job vertex and this is the first chained task. add a forward connection entry.
							new TaskConfig(container.getConfiguration()).addOutputShipStrategy(ShipStrategyType.FORWARD);
						}
						chainedTask.setContainingVertex(container);
					}
					
					// add info about the input serializer type
					chainedTask.getTaskConfig().setInputSerializer(inConn.getSerializer(), 0);
					
					this.chainedTasksInSequence.add(chainedTask);
					return;
				}
				else if (node instanceof BulkPartialSolutionPlanNode ||
						 node instanceof WorksetPlanNode)
				{
					// merged iteration head task. the task that the head is merged with will take care of it
					return;
				} else {
					throw new CompilerException("Bug: Unrecognized merged task vertex.");
				}
			}
			
			// -------- Here, we translate non-chained tasks -------------
			
			// create the config that will contain all the description of the inputs
			final TaskConfig targetVertexConfig = new TaskConfig(targetVertex.getConfiguration());
						
			// get the inputs. if this node is the head of an iteration, we obtain the inputs from the
			// enclosing iteration node, because the inputs are the initial inputs to the iteration.
			final Iterator<Channel> inConns;
			if (node instanceof BulkPartialSolutionPlanNode) {
				inConns = ((BulkPartialSolutionPlanNode) node).getContainingIterationNode().getInputs();
				// because the partial solution has its own vertex, is has only one (logical) input.
				// note this in the task configuration
				targetVertexConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(0);
			} else if (node instanceof WorksetPlanNode) {
				WorksetPlanNode wspn = (WorksetPlanNode) node;
				// input that is the initial workset
				inConns = Collections.singleton(wspn.getContainingIterationNode().getInput2()).iterator();
				
				// because we have a stand-alone (non-merged) workset iteration head, the initial workset will
				// be input 0 and the solution set will be input 1
				targetVertexConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(0);
				targetVertexConfig.setIterationHeadSolutionSetInputIndex(1);
			} else {
				inConns = node.getInputs();
			}
			if (!inConns.hasNext()) {
				throw new CompilerException("Bug: Found a non-source task with no input.");
			}
			
			for (int inputIndex = 0; inConns.hasNext(); inputIndex++) {
				Channel input = inConns.next();
				translateChannel(input, inputIndex, targetVertex,targetVertexConfig);
			}
		} catch (Exception e) {
			throw new CompilerException(
				"An error occurred while translating the optimized plan to a nephele JobGraph: " + e.getMessage(), e);
		}
	}
	
	private void translateChannel(Channel input, int inputIndex, AbstractJobVertex targetVertex,
			TaskConfig targetVertexConfig) throws Exception
	{
		final PlanNode inputPlanNode = input.getSource();
		final Iterator<Channel> allInChannels;
		
		if (inputPlanNode instanceof UnionPlanNode) {
			allInChannels = ((UnionPlanNode) inputPlanNode).getListOfInputs().iterator();
		}
		else if (inputPlanNode instanceof BulkPartialSolutionPlanNode) {
			if (this.vertices.get(inputPlanNode) == null) {
				// merged iteration head
				final BulkPartialSolutionPlanNode pspn = (BulkPartialSolutionPlanNode) inputPlanNode;
				final BulkIterationPlanNode iterationNode = pspn.getContainingIterationNode();
				
				// check if the iteration's input is a union
				if (iterationNode.getInput().getSource() instanceof UnionPlanNode) {
					allInChannels = ((UnionPlanNode) iterationNode.getInput().getSource()).getInputs();
				} else {
					allInChannels = Collections.singletonList(iterationNode.getInput()).iterator();
				}
				
				// also, set the index of the gate with the partial solution
				targetVertexConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(inputIndex);
			} else {
				// standalone iteration head
				allInChannels = Collections.singletonList(input).iterator();
			}
		} else if (inputPlanNode instanceof WorksetPlanNode) {
			if (this.vertices.get(inputPlanNode) == null) {
				// merged iteration head
				final WorksetPlanNode wspn = (WorksetPlanNode) inputPlanNode;
				final WorksetIterationPlanNode iterationNode = wspn.getContainingIterationNode();
				
				// check if the iteration's input is a union
				if (iterationNode.getInput2().getSource() instanceof UnionPlanNode) {
					allInChannels = ((UnionPlanNode) iterationNode.getInput2().getSource()).getInputs();
				} else {
					allInChannels = Collections.singletonList(iterationNode.getInput2()).iterator();
				}
				
				// also, set the index of the gate with the partial solution
				targetVertexConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(inputIndex);
			} else {
				// standalone iteration head
				allInChannels = Collections.singletonList(input).iterator();
			}
		} else if (inputPlanNode instanceof SolutionSetPlanNode) {
			// for now, skip connections with the solution set node, as this is a local index access (later to be parameterized here)
			// rather than a vertex connection
			return;
		} else {
			allInChannels = Collections.singletonList(input).iterator();
		}
		
		// check that the type serializer is consistent
		TypeSerializerFactory<?> typeSerFact = null;
		
		// accounting for channels on the dynamic path
		int numChannelsTotal = 0;
		int numChannelsDynamicPath = 0;
		int numDynamicSenderTasksTotal = 0;
		

		// expand the channel to all the union channels, in case there is a union operator at its source
		while (allInChannels.hasNext()) {
			final Channel inConn = allInChannels.next();
			
			// sanity check the common serializer
			if (typeSerFact == null) {
				typeSerFact = inConn.getSerializer();
			} else if (!typeSerFact.equals(inConn.getSerializer())) {
				throw new CompilerException("Conflicting types in union operator.");
			}
			
			final PlanNode sourceNode = inConn.getSource();
			AbstractJobVertex sourceVertex = this.vertices.get(sourceNode);
			TaskConfig sourceVertexConfig;

			if (sourceVertex == null) {
				// this predecessor is chained to another task or an iteration
				final TaskInChain chainedTask;
				final IterationDescriptor iteration;
				if ((chainedTask = this.chainedTasks.get(sourceNode)) != null) {
					// push chained task
					if (chainedTask.getContainingVertex() == null)
						throw new IllegalStateException("Bug: Chained task has not been assigned its containing vertex when connecting.");
					sourceVertex = chainedTask.getContainingVertex();
					sourceVertexConfig = chainedTask.getTaskConfig();
				} else if ((iteration = this.iterations.get(sourceNode)) != null) {
					// predecessor is an iteration
					sourceVertex = iteration.getHeadTask();
					sourceVertexConfig = iteration.getHeadFinalResultConfig();
				} else {
					throw new CompilerException("Bug: Could not resolve source node for a channel.");
				}
			} else {
				// predecessor is its own vertex
				sourceVertexConfig = new TaskConfig(sourceVertex.getConfiguration());
			}
			DistributionPattern pattern = connectJobVertices(
				inConn, inputIndex, sourceVertex, sourceVertexConfig, targetVertex, targetVertexConfig);
			
			// accounting on channels and senders
			numChannelsTotal++;
			if (inConn.isOnDynamicPath()) {
				numChannelsDynamicPath++;
				numDynamicSenderTasksTotal += getNumberOfSendersPerReceiver(pattern,
					sourceVertex.getNumberOfSubtasks(), targetVertex.getNumberOfSubtasks());
			}
		}
		
		// for the iterations, check that the number of dynamic channels is the same as the number
		// of channels for this logical input. this condition is violated at the moment, if there
		// is a union between nodes on the static and nodes on the dynamic path
		if (numChannelsDynamicPath > 0 && numChannelsTotal != numChannelsDynamicPath) {
			throw new CompilerException("Error: It is currently not supported to union between dynamic and static path in an iteration.");
		}
		if (numDynamicSenderTasksTotal > 0) {
			targetVertexConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(inputIndex, numDynamicSenderTasksTotal);
		}
		
		// the local strategy is added only once. in non-union case that is the actual edge,
		// in the union case, it is the edge between union and the target node
		addLocalInfoFromChannelToConfig(input, targetVertexConfig, inputIndex);
	}
	
	private int getNumberOfSendersPerReceiver(DistributionPattern pattern, int numSenders, int numReceivers) {
		if (pattern == DistributionPattern.BIPARTITE) {
			return numSenders;
		} else if (pattern == DistributionPattern.POINTWISE) {
			if (numSenders != numReceivers) {
				throw new CompilerException("Error: A changing degree of parallelism is currently " +
						"not supported between tasks within an iteration.");
			} else {
				return 1;
			}
		} else {
			throw new CompilerException("Unknown distribution pattern for channels: " + pattern);
		}
	}
	
	// ------------------------------------------------------------------------
	// Methods for creating individual vertices
	// ------------------------------------------------------------------------
	
	private JobTaskVertex createSingleInputVertex(SingleInputPlanNode node) throws CompilerException {
		final String taskName = node.getPactContract().getName();
		final DriverStrategy ds = node.getDriverStrategy();
		
		// check, whether chaining is possible
		boolean chaining = false;
		{
			Channel inConn = node.getInput();
			PlanNode pred = inConn.getSource();
			chaining = ds.getPushChainDriverClass() != null &&
					!(pred instanceof UnionPlanNode) &&	// union requires a union gate
					!(pred instanceof BulkPartialSolutionPlanNode) &&	// partial solution merges anyways
					inConn.getShipStrategy() == ShipStrategyType.FORWARD &&
					inConn.getLocalStrategy() == LocalStrategy.NONE &&
					pred.getOutgoingChannels().size() == 1 &&
					node.getDegreeOfParallelism() == pred.getDegreeOfParallelism() && 
					node.getSubtasksPerInstance() == pred.getSubtasksPerInstance();
		}
		
		final JobTaskVertex vertex;
		final TaskConfig config;
		
		if (chaining) {
			vertex = null;
			config = new TaskConfig(new Configuration());
			this.chainedTasks.put(node, new TaskInChain(ds.getPushChainDriverClass(), config, taskName));
		} else {
			// create task vertex
			vertex = new JobTaskVertex(taskName, this.jobGraph);
			vertex.setTaskClass(this.currentIteration == null ? RegularPactTask.class : IterationIntermediatePactTask.class);
			config = new TaskConfig(vertex.getConfiguration());
			config.setDriver(ds.getDriverClass());
		}
		
		// set user code
		config.setStubClass(node.getPactContract().getUserCodeClass());
		config.setStubParameters(node.getPactContract().getParameters());
		
		// set the driver strategy
		config.setDriverStrategy(ds);
		if (node.getComparator() != null) {
			config.setDriverComparator(node.getComparator(), 0);
		}
		
		// assign memory, file-handles, etc.
		assignDriverResources(node, config);
		return vertex;
	}

	private JobTaskVertex createDualInputVertex(DualInputPlanNode node) throws CompilerException {
		final String taskName = node.getPactContract().getName();
		final DriverStrategy ds = node.getDriverStrategy();
		final JobTaskVertex vertex = new JobTaskVertex(taskName, this.jobGraph);
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());
		vertex.setTaskClass(this.currentIteration == null ? RegularPactTask.class : IterationIntermediatePactTask.class);
		
		// set user code
		config.setStubClass(node.getPactContract().getUserCodeClass());
		config.setStubParameters(node.getPactContract().getParameters());
		
		// set the driver strategy
		config.setDriver(ds.getDriverClass());
		config.setDriverStrategy(ds);
		if (node.getComparator1() != null) {
			config.setDriverComparator(node.getComparator1(), 0);
		}
		if (node.getComparator2() != null) {
			config.setDriverComparator(node.getComparator2(), 1);
		}
		if (node.getPairComparator() != null) {
			config.setDriverPairComparator(node.getPairComparator());
		}
		
		// assign memory, file-handles, etc.
		assignDriverResources(node, config);
		return vertex;
	}

	private JobInputVertex createDataSourceVertex(SourcePlanNode node) throws CompilerException {
		final JobInputVertex vertex = new JobInputVertex(node.getPactContract().getName(), this.jobGraph);
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());
		
		// set task class
		@SuppressWarnings("unchecked")
		final Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>) (Class<?>) DataSourceTask.class;
		vertex.setInputClass(clazz);

		// set user code
		config.setStubClass(node.getPactContract().getUserCodeClass());
		config.setStubParameters(node.getPactContract().getParameters());
		
		config.setOutputSerializer(node.getSerializer());
		return vertex;
	}

	private AbstractJobOutputVertex createDataSinkVertex(SinkPlanNode node) throws CompilerException {
		final JobOutputVertex vertex = new JobOutputVertex(node.getPactContract().getName(), this.jobGraph);
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());
		
		vertex.setOutputClass(DataSinkTask.class);
		vertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, node.getDegreeOfParallelism());
		
		// set user code
		config.setStubClass(node.getPactContract().getUserCodeClass());
		config.setStubParameters(node.getPactContract().getParameters());
		
		return vertex;
	}
	
	private JobTaskVertex createBulkIterationHead(BulkPartialSolutionPlanNode pspn) {
		// get the bulk iteration that corresponds to this partial solution node
		final BulkIterationPlanNode iteration = pspn.getContainingIterationNode();
		
		// check whether we need an individual vertex for the partial solution, or whether we
		// attach ourselves to the vertex of the parent node. We can combine the head with a node of 
		// the step function, if
		// 1) There is one parent that the partial solution connects to via a forward pattern and no
		//    local strategy
		// 2) DOP and the number of subtasks per instance does not change
		// 3) That successor is not a union
		// 4) That successor is not itself the last node of the step function
		
		final boolean merge;
		if (mergeIterationAuxTasks && pspn.getOutgoingChannels().size() == 1) {
			final Channel c = pspn.getOutgoingChannels().get(0);
			final PlanNode successor = c.getTarget();
			merge = c.getShipStrategy() == ShipStrategyType.FORWARD &&
					c.getLocalStrategy() == LocalStrategy.NONE &&
					c.getTempMode() == TempMode.NONE &&
					successor.getDegreeOfParallelism() == pspn.getDegreeOfParallelism() &&
					successor.getSubtasksPerInstance() == pspn.getSubtasksPerInstance() &&
					!(successor instanceof UnionPlanNode) &&
					successor != iteration.getRootOfStepFunction();
		} else {
			merge = false;
		}
		
		// create or adopt the head vertex
		final JobTaskVertex toReturn;
		final JobTaskVertex headVertex;
		final TaskConfig headConfig;
		if (merge) {
			final PlanNode successor = pspn.getOutgoingChannels().get(0).getTarget();
			headVertex = (JobTaskVertex) this.vertices.get(successor);
			
			if (headVertex == null) {
				throw new CompilerException(
					"Bug: Trying to merge solution set with its sucessor, but successor has not been created.");
			}
			
			// reset the vertex type to iteration head
			headVertex.setTaskClass(IterationHeadPactTask.class);
			headConfig = new TaskConfig(headVertex.getConfiguration());
			toReturn = null;
		} else {
			// instantiate the head vertex and give it a no-op driver as the driver strategy.
			// everything else happens in the post visit, after the input (the initial partial solution)
			// is connected.
			headVertex = new JobTaskVertex(iteration.getPactContract().getName() + " - Partial Solution", this.jobGraph);
			headVertex.setTaskClass(IterationHeadPactTask.class);
			headConfig = new TaskConfig(headVertex.getConfiguration());
			headConfig.setDriver(NoOpDriver.class);
			toReturn = headVertex;
		}
		
		// create the iteration descriptor and the iteration to it
		IterationDescriptor descr = this.iterations.get(iteration);
		if (descr == null) {
			throw new CompilerException("Bug: Iteration descriptor was not created at when translating the iteration node.");
		}
		descr.setHeadTask(headVertex, headConfig);
		
		return toReturn;
	}
	
	private JobTaskVertex createWorksetIterationHead(WorksetPlanNode wspn) {
		// get the bulk iteration that corresponds to this partial solution node
		final WorksetIterationPlanNode iteration = wspn.getContainingIterationNode();
		
		// check whether we need an individual vertex for the partial solution, or whether we
		// attach ourselves to the vertex of the parent node. We can combine the head with a node of 
		// the step function, if
		// 1) There is one parent that the partial solution connects to via a forward pattern and no
		//    local strategy
		// 2) DOP and the number of subtasks per instance does not change
		// 3) That successor is not a union
		// 4) That successor is not itself the last node of the step function
		
		final boolean merge;
		if (mergeIterationAuxTasks && wspn.getOutgoingChannels().size() == 1) {
			final Channel c = wspn.getOutgoingChannels().get(0);
			final PlanNode successor = c.getTarget();
			merge = c.getShipStrategy() == ShipStrategyType.FORWARD &&
					c.getLocalStrategy() == LocalStrategy.NONE &&
					c.getTempMode() == TempMode.NONE &&
					successor.getDegreeOfParallelism() == wspn.getDegreeOfParallelism() &&
					successor.getSubtasksPerInstance() == wspn.getSubtasksPerInstance() &&
					!(successor instanceof UnionPlanNode) &&
					successor != iteration.getNextWorkSetPlanNode();
		} else {
			merge = false;
		}
		
		// create or adopt the head vertex
		final JobTaskVertex toReturn;
		final JobTaskVertex headVertex;
		final TaskConfig headConfig;
		if (merge) {
			final PlanNode successor = wspn.getOutgoingChannels().get(0).getTarget();
			headVertex = (JobTaskVertex) this.vertices.get(successor);
			
			if (headVertex == null) {
				throw new CompilerException(
					"Bug: Trying to merge solution set with its sucessor, but successor has not been created.");
			}
			
			// reset the vertex type to iteration head
			headVertex.setTaskClass(IterationHeadPactTask.class);
			headConfig = new TaskConfig(headVertex.getConfiguration());
			toReturn = null;
		} else {
			// instantiate the head vertex and give it a no-op driver as the driver strategy.
			// everything else happens in the post visit, after the input (the initial partial solution)
			// is connected.
			headVertex = new JobTaskVertex(iteration.getPactContract().getName() + " - Workset Iteration Head", this.jobGraph);
			headVertex.setTaskClass(IterationHeadPactTask.class);
			headConfig = new TaskConfig(headVertex.getConfiguration());
			headConfig.setDriver(NoOpDriver.class);
			toReturn = headVertex;
		}
		
		// create the iteration descriptor and the iteration to it
		IterationDescriptor descr = this.iterations.get(iteration);
		if (descr == null) {
			throw new CompilerException("Bug: Iteration descriptor was not created at when translating the iteration node.");
		}
		descr.setHeadTask(headVertex, headConfig);
		
		return toReturn;
	}
	
	private void assignDriverResources(PlanNode node, TaskConfig config) {
		final long mem = node.getMemoryPerSubTask();
		if (mem > 0) {
			config.setMemoryDriver(mem);
			config.setFilehandlesDriver(this.defaultMaxFan);
			config.setSpillingThresholdDriver(this.defaultSortSpillingThreshold);
		}
	}
	
	private void assignLocalStrategyResources(Channel c, TaskConfig config, int inputNum) {
		if (c.getMemoryLocalStrategy() > 0) {
			config.setMemoryInput(inputNum, c.getMemoryLocalStrategy());
			config.setFilehandlesInput(inputNum, this.defaultMaxFan);
			config.setSpillingThresholdInput(inputNum, this.defaultSortSpillingThreshold);
		}
	}

	// ------------------------------------------------------------------------
	// Connecting Vertices
	// ------------------------------------------------------------------------

	/**
	 * NOTE: The channel for global and local strategies are different if we connect a union. The global strategy
	 * channel is then the channel into the union node, the local strategy channel the one from the union to the
	 * actual target operator.
	 * 
	 * @param channelForGlobalStrategy
	 * @param channelForLocalStrategy
	 * @param inputNumber
	 * @param sourceVertex
	 * @param sourceConfig
	 * @param targetVertex
	 * @param targetConfig
	 * @throws JobGraphDefinitionException
	 * @throws CompilerException
	 */
	private DistributionPattern connectJobVertices(Channel channel, int inputNumber,
			final AbstractJobVertex sourceVertex, final TaskConfig sourceConfig,
			final AbstractJobVertex targetVertex, final TaskConfig targetConfig)
	throws JobGraphDefinitionException, CompilerException
	{
		// ------------ connect the vertices to the job graph --------------
		final ChannelType channelType;
		final DistributionPattern distributionPattern;

		switch (channel.getShipStrategy()) {
			case FORWARD:
			case PARTITION_LOCAL_HASH:
				distributionPattern = DistributionPattern.POINTWISE;
				channelType = ChannelType.NETWORK;
				break;
			case PARTITION_RANDOM:
			case BROADCAST:
			case PARTITION_HASH:
			case PARTITION_RANGE:
				distributionPattern = DistributionPattern.BIPARTITE;
				channelType = ChannelType.NETWORK;
				break;
			default:
				throw new RuntimeException("Unknown runtime ship strategy: " + channel.getShipStrategy());
		}
		
		sourceVertex.connectTo(targetVertex, channelType, CompressionLevel.NO_COMPRESSION, distributionPattern);

		// -------------- configure the source task's ship strategy strategies in task config --------------
		final int outputIndex = sourceConfig.getNumOutputs();
		sourceConfig.addOutputShipStrategy(channel.getShipStrategy());
		if (outputIndex == 0) {
			sourceConfig.setOutputSerializer(channel.getSerializer());
		}
		if (channel.getShipStrategyComparator() != null) {
			sourceConfig.setOutputComparator(channel.getShipStrategyComparator(), outputIndex);
		}
		
		// TODO: Re-enable range partitioner distribution
		if (channel.getShipStrategy() == ShipStrategyType.PARTITION_RANGE) {
			throw new CompilerException("Range Partitioner is currently not enabled.");
		}
//		if (targetContract instanceof GenericDataSink) {
//			final DataDistribution distri = ((GenericDataSink) targetContract).getDataDistribution();
//			if (distri != null) {
//				configForOutputShipStrategy.setOutputDataDistribution(distri);
//			}
//		}
		
		// ---------------- configure the receiver -------------------
		targetConfig.addInputToGroup(inputNumber);
		return distributionPattern;
	}
	
	private void addLocalInfoFromChannelToConfig(Channel channel, TaskConfig config, int inputNum) {
		// serializer
		config.setInputSerializer(channel.getSerializer(), inputNum);
		
		// local strategy
		if (channel.getLocalStrategy() != LocalStrategy.NONE) {
			config.setInputLocalStrategy(inputNum, channel.getLocalStrategy());
			if (channel.getLocalStrategyComparator() != null) {
				config.setInputComparator(channel.getLocalStrategyComparator(), inputNum);
			}
		}
		
		assignLocalStrategyResources(channel, config, inputNum);
		
		// materialization / caching
		if (channel.getTempMode() != null) {
			final TempMode tm = channel.getTempMode();

			boolean needsMemory = false;
			if (tm.breaksPipeline()) {
				config.setInputAsynchronouslyMaterialized(inputNum, true);
				needsMemory = true;
			}
			if (tm.isCached()) {
				config.setInputCached(inputNum, true);
				needsMemory = true;
			}
			
			if (needsMemory) {
				// sanity check
				if (tm == null || tm == TempMode.NONE || channel.getTempMemory() < 1) {
					throw new CompilerException("Bug in compiler: Inconsistent description of input materialization.");
				}
				config.setInputMaterializationMemory(inputNum, channel.getTempMemory());
			}
		}
	}
	
	private void finalizeBulkIteration(IterationDescriptor descr) {
		
		final BulkIterationPlanNode bulkNode = (BulkIterationPlanNode) descr.getIterationNode();
		final JobTaskVertex headVertex = descr.getHeadTask();
		final TaskConfig headConfig = new TaskConfig(headVertex.getConfiguration());
		final TaskConfig headFinalOutputConfig = descr.getHeadFinalResultConfig();
		
		// ------------ finalize the head config with the final outputs and the sync gate ------------
		final int numStepFunctionOuts = headConfig.getNumOutputs();
		final int numFinalOuts = headFinalOutputConfig.getNumOutputs();
		headConfig.setIterationHeadFinalOutputConfig(headFinalOutputConfig);
		headConfig.setIterationHeadIndexOfSyncOutput(numStepFunctionOuts + numFinalOuts);
		final long memForBackChannel = bulkNode.getMemoryPerSubTask();
		if (memForBackChannel <= 0) {
			throw new CompilerException("Bug: No memory has been assigned to the iteration back channel.");
		}
		headConfig.setBackChannelMemory(memForBackChannel);
		
		// --------------------------- create the sync task ---------------------------
		final JobOutputVertex sync = new JobOutputVertex("Bulk-Iteration Sync (" +
					bulkNode.getPactContract().getName() + ")", this.jobGraph);
		sync.setOutputClass(IterationSynchronizationSinkTask.class);
		sync.setNumberOfSubtasks(1);
		this.auxVertices.add(sync);
		
		final TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, headVertex.getNumberOfSubtasks());

		// set the number of iteration / convergence criterion for the sync
		final int maxNumIterations = bulkNode.getIterationNode().getIterationContract().getMaximumNumberOfIterations();
		if (maxNumIterations < 1) {
			throw new CompilerException("Cannot create bulk iteration with unspecified maximum number of iterations.");
		}
		syncConfig.setNumberOfIterations(maxNumIterations);
		
		// connect the sync task
		try {
			headVertex.connectTo(sync, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
		} catch (JobGraphDefinitionException e) {
			throw new CompilerException("Bug: Cannot connect head vertex to sync task.");
		}
		
		
		// ----------------------------- create the iteration tail ------------------------------
		final PlanNode rootOfStepFunction = bulkNode.getRootOfStepFunction();
		final TaskConfig tailConfig;
		JobTaskVertex rootOfStepFunctionVertex = (JobTaskVertex) this.vertices.get(rootOfStepFunction);
		if (rootOfStepFunctionVertex == null) {
			// last op is chained
			final TaskInChain taskInChain = this.chainedTasks.get(rootOfStepFunction);
			if (taskInChain == null) {
				throw new CompilerException("Bug: Tail of step function not found as vertex or chained task.");
			}
			rootOfStepFunctionVertex = (JobTaskVertex) taskInChain.getContainingVertex();
			tailConfig = taskInChain.getTaskConfig();
		} else {
			tailConfig = new TaskConfig(rootOfStepFunctionVertex.getConfiguration());
		}
		rootOfStepFunctionVertex.setTaskClass(IterationTailPactTask.class);
		tailConfig.setOutputSerializer(bulkNode.getSerializerForIterationChannel());
		
		// create the fake output task
		JobOutputVertex fakeTail = new JobOutputVertex("Fake Tail", this.jobGraph);
		fakeTail.setOutputClass(FakeOutputTask.class);
		fakeTail.setNumberOfSubtasks(headVertex.getNumberOfSubtasks());
		fakeTail.setNumberOfSubtasksPerInstance(headVertex.getNumberOfSubtasksPerInstance());
		this.auxVertices.add(fakeTail);
		
		// connect the fake tail
		try {
			rootOfStepFunctionVertex.connectTo(fakeTail, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION, 
				DistributionPattern.POINTWISE);
		} catch (JobGraphDefinitionException e) {
			throw new CompilerException("Bug: Cannot connect iteration tail vertex fake tail task");
		}
		tailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		// the fake channel is statically typed to pact record. no data is sent over this channel anyways.
		
		// ------------------- register the aggregators -------------------
		AggregatorRegistry aggs = bulkNode.getIterationNode().getIterationContract().getAggregators();
		Collection<AggregatorWithName<?>> allAggregators = aggs.getAllRegisteredAggregators();
		
		headConfig.addIterationAggregators(allAggregators);
		syncConfig.addIterationAggregators(allAggregators);
		
		String convAggName = aggs.getConvergenceCriterionAggregatorName();
		Class<? extends ConvergenceCriterion<?>> convCriterion = aggs.getConvergenceCriterion();
		
		if (convCriterion != null || convAggName != null) {
			if (convCriterion == null) {
				throw new CompilerException("Error: Convergence criterion aggregator set, but criterion is null.");
			}
			if (convAggName == null) {
				throw new CompilerException("Error: Aggregator convergence criterion set, but aggregator is null.");
			}
			
			syncConfig.setConvergenceCriterion(convAggName, convCriterion);
		}
	}
	
	private void finalizeWorksetIteration(IterationDescriptor descr) {
		final WorksetIterationPlanNode iterNode = (WorksetIterationPlanNode) descr.getIterationNode();
		final JobTaskVertex headVertex = descr.getHeadTask();
		final TaskConfig headConfig = new TaskConfig(headVertex.getConfiguration());
		final TaskConfig headFinalOutputConfig = descr.getHeadFinalResultConfig();
		
		// ------------ finalize the head config with the final outputs and the sync gate ------------
		{
			final int numStepFunctionOuts = headConfig.getNumOutputs();
			final int numFinalOuts = headFinalOutputConfig.getNumOutputs();
			headConfig.setIterationHeadFinalOutputConfig(headFinalOutputConfig);
			headConfig.setIterationHeadIndexOfSyncOutput(numStepFunctionOuts + numFinalOuts);
			final long mem = iterNode.getMemoryPerSubTask();
			if (mem <= 0) {
				throw new CompilerException("Bug: No memory has been assigned to the workset iteration.");
			}
			
			headConfig.setWorksetIteration();
			headConfig.setBackChannelMemory(mem / 2);
			headConfig.setSolutionSetMemory(mem / 2);
			
			// set the solution set serializer and comparator
			headConfig.setSolutionSetSerializer(iterNode.getSolutionSetSerializer());
			headConfig.setSolutionSetComparator(iterNode.getSolutionSetComparator());
		}
		
		// --------------------------- create the sync task ---------------------------
		final TaskConfig syncConfig;
		{
			final JobOutputVertex sync = new JobOutputVertex("Workset-Iteration Sync (" +
						iterNode.getPactContract().getName() + ")", this.jobGraph);
			sync.setOutputClass(IterationSynchronizationSinkTask.class);
			sync.setNumberOfSubtasks(1);
			this.auxVertices.add(sync);
			
			syncConfig = new TaskConfig(sync.getConfiguration());
			syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, headVertex.getNumberOfSubtasks());
	
			// set the number of iteration / convergence criterion for the sync
			final int maxNumIterations = iterNode.getIterationNode().getIterationContract().getMaximumNumberOfIterations();
			if (maxNumIterations < 1) {
				throw new CompilerException("Cannot create workset iteration with unspecified maximum number of iterations.");
			}
			syncConfig.setNumberOfIterations(maxNumIterations);
			
			// connect the sync task
			try {
				headVertex.connectTo(sync, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
			} catch (JobGraphDefinitionException e) {
				throw new CompilerException("Bug: Cannot connect head vertex to sync task.");
			}
		}
		
		// ----------------------------- create the iteration tail ------------------------------
		{
			final PlanNode nextWorksetNode = iterNode.getNextWorkSetPlanNode();
			final TaskConfig tailConfig;
			JobTaskVertex nextWorksetVertex = (JobTaskVertex) this.vertices.get(nextWorksetNode);
			if (nextWorksetVertex == null) {
				// last op is chained
				final TaskInChain taskInChain = this.chainedTasks.get(nextWorksetNode);
				if (taskInChain == null) {
					throw new CompilerException("Bug: Tail of step function not found as vertex or chained task.");
				}
				nextWorksetVertex = (JobTaskVertex) taskInChain.getContainingVertex();
				tailConfig = taskInChain.getTaskConfig();
			} else {
				tailConfig = new TaskConfig(nextWorksetVertex.getConfiguration());
			}
			nextWorksetVertex.setTaskClass(IterationTailPactTask.class);
			
			tailConfig.setOutputSerializer(iterNode.getWorksetSerializer());
			
			// create the fake output task
			JobOutputVertex fakeTail = new JobOutputVertex("Fake Tail", this.jobGraph);
			fakeTail.setOutputClass(FakeOutputTask.class);
			fakeTail.setNumberOfSubtasks(headVertex.getNumberOfSubtasks());
			fakeTail.setNumberOfSubtasksPerInstance(headVertex.getNumberOfSubtasksPerInstance());
			this.auxVertices.add(fakeTail);
			
			// connect the fake tail
			try {
				nextWorksetVertex.connectTo(fakeTail, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION, 
					DistributionPattern.POINTWISE);
			} catch (JobGraphDefinitionException e) {
				throw new CompilerException("Bug: Cannot connect iteration tail vertex fake tail task");
			}
			tailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			// the fake channel is statically typed to pact record. no data is sent over this channel anyways.
			
			// mark the iteration tail as a workset iteration, such that it instantiates the workset element count aggregator
			tailConfig.setWorksetIteration();
		}
		
		// ------------------- mark the solution set delta node as solution set updating -------------------
		{
			final PlanNode solutionDeltaNode = iterNode.getSolutionSetDeltaPlanNode();
			final TaskConfig solutionDeltaConfig;
			JobTaskVertex solutionDeltaVertex = (JobTaskVertex) this.vertices.get(solutionDeltaNode);
			if (solutionDeltaVertex == null) {
				// last op is chained
				final TaskInChain taskInChain = this.chainedTasks.get(solutionDeltaNode);
				if (taskInChain == null) {
					throw new CompilerException("Bug: Solution Set Delta not found as vertex or chained task.");
				}
				solutionDeltaVertex = (JobTaskVertex) taskInChain.getContainingVertex();
				solutionDeltaConfig = taskInChain.getTaskConfig();
			} else {
				solutionDeltaConfig = new TaskConfig(solutionDeltaVertex.getConfiguration());
			}
			solutionDeltaConfig.setUpdateSolutionSet();
			
			// hack!!! for now, we support only immediate updates
			solutionDeltaConfig.setUpdateSolutionSetWithoutReprobe();
		}
		
		// ------------------- register the aggregators -------------------
		AggregatorRegistry aggs = iterNode.getIterationNode().getIterationContract().getAggregators();
		Collection<AggregatorWithName<?>> allAggregators = aggs.getAllRegisteredAggregators();
		
		for (AggregatorWithName<?> agg : allAggregators) {
			if (agg.getName().equals(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME)) {
				throw new CompilerException("User defined aggregator used the same name as built-in workset " +
						"termination check aggregator: " + WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME);
			}
		}
		
		headConfig.addIterationAggregators(allAggregators);
		syncConfig.addIterationAggregators(allAggregators);
		
		String convAggName = aggs.getConvergenceCriterionAggregatorName();
		Class<? extends ConvergenceCriterion<?>> convCriterion = aggs.getConvergenceCriterion();
		
		if (convCriterion != null || convAggName != null) {
			throw new CompilerException("Error: Cannot use custom convergence criterion with workset iteration. Workset iterations have implicit convergence criterion where workset is empty.");
		}
		
		headConfig.addIterationAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, LongSumAggregator.class);
		syncConfig.addIterationAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, LongSumAggregator.class);
		syncConfig.setConvergenceCriterion(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, WorksetEmptyConvergenceCriterion.class);
	}

	// -------------------------------------------------------------------------------------
	// Descriptors for tasks / configurations that are chained or merged with other tasks
	// -------------------------------------------------------------------------------------
	
	/**
	 * Utility class that describes a task in a sequence of chained tasks. Chained tasks are tasks that run
	 * together in one thread.
	 */
	private static final class TaskInChain {
		
		private final Class<? extends ChainedDriver<?, ?>> chainedTask;
		
		private final TaskConfig taskConfig;
		
		private final String taskName;
		
		private AbstractJobVertex containingVertex;

		@SuppressWarnings("unchecked")
		TaskInChain(@SuppressWarnings("rawtypes") Class<? extends ChainedDriver> chainedTask, TaskConfig taskConfig, String taskName) {
			this.chainedTask = (Class<? extends ChainedDriver<?, ?>>) chainedTask;
			this.taskConfig = taskConfig;
			this.taskName = taskName;
		}
		
		public Class<? extends ChainedDriver<?, ?>> getChainedTask() {
			return this.chainedTask;
		}
		
		public TaskConfig getTaskConfig() {
			return this.taskConfig;
		}
		
		public String getTaskName() {
			return this.taskName;
		}
		
		public AbstractJobVertex getContainingVertex() {
			return this.containingVertex;
		}
		
		public void setContainingVertex(AbstractJobVertex containingVertex) {
			this.containingVertex = containingVertex;
		}
	}
	
	private static final class IterationDescriptor {
		
		private final IterationPlanNode iterationNode;
		
		private JobTaskVertex headTask;
		
		private TaskConfig headConfig;
		
		private TaskConfig  headFinalResultConfig;
		
		private final int id;

		public IterationDescriptor(IterationPlanNode iterationNode, int id) {
			this.iterationNode = iterationNode;
			this.id = id;
		}
		
		public IterationPlanNode getIterationNode() {
			return iterationNode;
		}
		
		public void setHeadTask(JobTaskVertex headTask, TaskConfig headConfig) {
			this.headTask = headTask;
			this.headFinalResultConfig = new TaskConfig(new Configuration());
			this.headConfig = headConfig;
		}
		
		public JobTaskVertex getHeadTask() {
			return headTask;
		}
		
		public TaskConfig getHeadConfig() {
			return headConfig;
		}
		
		public TaskConfig getHeadFinalResultConfig() {
			return headFinalResultConfig;
		}
		
		public int getId() {
			return this.id;
		}
	}
}
