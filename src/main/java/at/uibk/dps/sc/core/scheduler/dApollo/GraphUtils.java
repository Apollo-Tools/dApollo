package at.uibk.dps.sc.core.scheduler.dApollo;

import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.properties.PropertyServiceData;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class GraphUtils {

    /**
     * Get the leaf nodes of an enactment graph.
     *
     * @param eGraph the graph node to get leaf nodes from.
     * @return collection of leaf nodes.
     */
    private static Collection<Task> getEntryNodes(EnactmentGraph eGraph) {
        return eGraph.getVertices().stream().filter(task -> task instanceof Communication && PropertyServiceData.isRoot(task))
            .collect(Collectors.toList());
    }

    /**
     * Get the root task nodes of an enactment graph.
     *
     * @param eGraph the graph node to get root task nodes from.
     * @return collection of root task nodes.
     */
    public static List<Task> getEntryTaskNodes(EnactmentGraph eGraph) {
        List<Task> entryTaskNode = new ArrayList<>();

        Collection<Task> entryCommNodes = getEntryNodes(eGraph); // O(n^2)

        // Iterate over all root communication nodes
        for (Task entryCommNode : entryCommNodes) { // O(n^2)
            entryTaskNode.addAll(eGraph.getSuccessors(entryCommNode)); // O(n)
        }

        entryTaskNode = entryTaskNode.stream().distinct().collect(Collectors.toList()); // O(n)
        return entryTaskNode;
    }

    /**
     * Get all immediate successor task nodes of a specific task.
     *
     * @param eGraph the graph to look for successors.
     * @param node   the task node to check for immediate task successors.
     * @return the immediate successor task nodes.
     */
    public static Collection<Task> getSuccessorTaskNodes(EnactmentGraph eGraph, Task node) {
        if (node instanceof Communication) {

            // The successor of a communication node is a task node
            return eGraph.getSuccessors(node);
        } else {

            // The successor of a task node is a communication node (which successor is a task node)
            Collection<Task> successorTasks = new ArrayList<>();
            eGraph.getSuccessors(node).forEach((pS) -> successorTasks.addAll(eGraph.getSuccessors(pS)));
            return successorTasks;
        }
    }

    /**
     * Get all immediate predecessor task nodes of a specific task.
     *
     * @param eGraph the graph to look for predecessors.
     * @param node   the task node to check for immediate task predecessors.
     * @return the immediate predecessor task nodes.
     */
    public static Collection<Task> getPredecessorTaskNodes(EnactmentGraph eGraph, Task node) {
        if (node instanceof Communication) {

            // The predecessor of a communication node is a task node
            return eGraph.getPredecessors(node);
        } else {

            // The predecessor of a task node is a communication node (which predecessor is a task node)
            Collection<Task> predecessorTasks = new ArrayList<>();
            eGraph.getPredecessors(node).forEach((pS) -> predecessorTasks.addAll(eGraph.getPredecessors(pS)));
            return predecessorTasks;
        }
    }

    /**
     * Get the leaf nodes of an enactment graph.
     *
     * @param eGraph the graph node to get leaf nodes from.
     * @return collection of leaf nodes.
     */
    private static Collection<Task> getExitNodes(EnactmentGraph eGraph) {
        return eGraph.getVertices().stream().filter(task -> task instanceof Communication && PropertyServiceData.isLeaf(task))
            .collect(Collectors.toList());
    }

    /**
     * Get the leaf task nodes of an enactment graph.
     *
     * @param eGraph the graph node to get leaf task nodes from.
     * @return collection of leaf task nodes.
     */
    public static List<Task> getExitTaskNodes(EnactmentGraph eGraph) {
        List<Task> leafTaskNode = new ArrayList<>();
        Collection<Task> leafCommNodes = getExitNodes(eGraph); // O(n^2)

        // Iterate over all leaf communication nodes
        for (Task leafCommNode : leafCommNodes) { // O(n^2)
            leafTaskNode.addAll(eGraph.getPredecessors(leafCommNode)); // O(n)
        }

        leafTaskNode = leafTaskNode.stream().distinct().collect(Collectors.toList()); // O(n)
        return leafTaskNode;
    }

    /**
     * Get all task nodes of a graph.
     *
     * @param eGraph the graph node to get task nodes from.
     * @return collection of task nodes.
     */
    public static List<Task> getTasks(EnactmentGraph eGraph) {
        List<Task> tasks = new ArrayList<>();

        // Iterate over all leaf communication nodes
        for (Task node : eGraph.getVertices()) { // O(n^2)
            if(!(node instanceof Communication)) {
                tasks.add(node);
            }
        }
        return tasks;
    }
}
