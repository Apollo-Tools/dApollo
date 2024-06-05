package at.uibk.dps.sc.core.scheduler;

import at.uibk.dps.ee.guice.starter.VertxProvider;
import at.uibk.dps.ee.model.graph.SpecificationProvider;
import at.uibk.dps.sc.core.capacity.CapacityCalculator;
import at.uibk.dps.sc.core.scheduler.dApollo.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import java.util.*;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
public class SchedulerDApollo extends SchedulerAbstract {

    /**
     * Default logger for the {@link SchedulerDApollo} class.
     */
    private static final Logger LOGGER = Logger.getLogger(SchedulerDApollo.class.getName());

    /**
     * The specification of the workflow to be scheduled.
     */
    private final SpecificationProvider specificationProvider;

    /**
     * The input to the scheduler.
     */
    private final SchedulerInput schedulerInput;

    /**
     * Represents the current schedule (a specific mapping for each task).
     */
    private final HashMap<Task, Mapping_> currentSchedule;

    /**
     * Proposals to map tasks to other resources.
     */
    private List<Proposal> proposals;

    /**
     * The cost of the current schedule.
     */
    private double cost;

    /**
     * Exclude cost for data transfer.
     */
    private final boolean EXCLUDE_DATA_TRANSFER_COST;

    /**
     * Determines whether proposals need to be updated.
     */
    private boolean proposalsUpdated;

    /**
     * Tolerance value used to handle rounding error.
     */
    private static double TOLERANCE = 0.00001;

    /**
     * Attributes for tasks, resources and mappings.
     */
    enum Attributes {

        // Tasks
        INPUT_MB,
        OUTPUT_MB,

        // Resources
        COST_PER_HOUR,
        BANDWIDTH,
        ACQUISITION_DELAY,

        // Mappings
        RUNTIME
    }

    /**
     * Default constructor to set-up the scheduler.
     * [O(n^3)]
     *
     * @param specProvider       specification provider.
     * @param capacityCalculator capacity calculator.
     * @param vertProv           vertex provider.
     */
    @Inject public SchedulerDApollo(SpecificationProvider specProvider, final JsonObject jsonInput, boolean EXCLUDE_DATA_TRANSFER_COST,
        CapacityCalculator capacityCalculator, VertxProvider vertProv) {
        super(specProvider, capacityCalculator, vertProv);
        this.specificationProvider = specProvider;
        this.EXCLUDE_DATA_TRANSFER_COST = EXCLUDE_DATA_TRANSFER_COST;
        this.proposalsUpdated = false;

        // Set format and logging level
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %5$s %n");
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.OFF);
        LOGGER.addHandler(handler);
        LOGGER.setUseParentHandlers(false);
        LOGGER.setLevel(Level.OFF);

        // Setup the schedulerInput for the scheduler.
        this.schedulerInput = getSchedulerInput(jsonInput);

        // Line 1: setup initial schedule and compute cost of current schedule [O(n^2)].
        this.currentSchedule = new HashMap<>();
        for(Task t: GraphUtils.getTasks(specProvider.getEnactmentGraph())) {
            currentSchedule.put(t, new Mapping_(t, getCheapestResource(t, schedulerInput.getLocationRS()), schedulerInput.getLocationRS()));
        }
        double currentRSCost = getRuntime(new ArrayList<>(currentSchedule.values()), 0.0) * (schedulerInput.getResourcesRSInstances().get(schedulerInput.getLocationRS()).get(Attributes.COST_PER_HOUR.name()) / 3600);
        this.cost = getCost(new ArrayList<>(currentSchedule.values()), currentRSCost);

        // Line 2: Setup initial proposals [O(n^3)].
        this.proposals = new ArrayList<>();

        // Calculate est and lft-
        List<Task> allTasks = GraphUtils.getTasks(specificationProvider.getEnactmentGraph());
        List<Task> exitTasks = GraphUtils.getExitTaskNodes(specificationProvider.getEnactmentGraph());
        List<Task> entryTasks = GraphUtils.getEntryTaskNodes(specificationProvider.getEnactmentGraph());
        List<Task> remainingTasks = allTasks.stream().filter(t -> !exitTasks.contains(t) && !entryTasks.contains(t)).collect(Collectors.toList());
        HashMap<Task, Double> est = calculateEST(entryTasks, Stream.concat(exitTasks.stream(), remainingTasks.stream()).collect(Collectors.toList()));
        HashMap<Task, Double> lft = calculateLFT(est, Stream.concat(entryTasks.stream(), remainingTasks.stream()).collect(Collectors.toList()), exitTasks);
        for(Task task: GraphUtils.getTasks(specProvider.getEnactmentGraph())) {
            List<Proposal> taskProposals = new ArrayList<>();

            // Iterate over all possible mappings of the task.
            for(Mapping<Task, Resource> taskMappingSpec: specificationProvider.getMappings().getMappings(task)) {

                // Check if currently not on this resource
                if(!taskMappingSpec.getTarget().equals(currentSchedule.get(task).getResource())) {

                    // Proposal for mapping only the task on another resource
                    Mapping_ proposalTaskMapping = new Mapping_(task, taskMappingSpec.getTarget(), schedulerInput.getLocationRS());
                    double ts_1 = getRuntime(currentSchedule.get(task)) - getRuntime(proposalTaskMapping);
                    double additionalRSCost_1 = (- 1.0) * ts_1 * (schedulerInput.getResourcesRSInstances().get(schedulerInput.getLocationRS()).get(Attributes.COST_PER_HOUR.name()) / 3600.0);
                    double ac_1 = getCost(proposalTaskMapping) - getCost(currentSchedule.get(task)) + additionalRSCost_1;
                    Proposal proposalTask = new Proposal(Collections.singletonList(proposalTaskMapping), ts_1, ac_1);
                    proposalTask.setTaskIncludes(Collections.singletonList(task));
                    taskProposals.add(proposalTask);

                    // Proposal for mapping the task and its successors
                    if(!GraphUtils.getExitTaskNodes(specProvider.getEnactmentGraph()).contains(task)) {
                        String rs = resourceMinimizingCommunication(taskMappingSpec.getTarget());
                        List<Mapping_> currentScheduleTaskAndSuccessors = new ArrayList<>(Collections.singletonList(currentSchedule.get(task)));
                        double maxOutputSuccessors = 0.0;
                        for(Task s: GraphUtils.getSuccessorTaskNodes(specProvider.getEnactmentGraph(), task)) {
                            currentScheduleTaskAndSuccessors.add(currentSchedule.get(s));
                            maxOutputSuccessors = Math.max(maxOutputSuccessors, s.getAttribute(Attributes.OUTPUT_MB.name()));
                        }
                        List<Mapping_> proposalTaskAndSuccessorMappings = new ArrayList<>(Collections.singletonList(new Mapping_(task, taskMappingSpec.getTarget(), rs)));
                        for(Task s: GraphUtils.getSuccessorTaskNodes(specProvider.getEnactmentGraph(), task)) {
                            double minDurationSuccessor = Double.MAX_VALUE;
                            for(Mapping<Task, Resource> m: specificationProvider.getMappings().getMappings(s)) {
                                minDurationSuccessor = Math.min(minDurationSuccessor, getRuntime(new Mapping_(s, m.getTarget(), rs)));
                            }
                            double hasTime = lft.get(s) - est.get(s) - (getRuntime(currentSchedule.get(s)) - minDurationSuccessor);
                            double minCost = Double.MAX_VALUE;
                            Mapping_ bestMapping = null;
                            for(Mapping<Task, Resource> m: specificationProvider.getMappings().getMappings(s)) {
                                if(getRuntime(new Mapping_(s, m.getTarget(), rs)) <= hasTime + TOLERANCE && getCost(new Mapping_(s, m.getTarget(), rs)) < minCost)  {
                                    minCost = getCost(new Mapping_(s, m.getTarget(), rs));
                                    bestMapping = new Mapping_(s, m.getTarget(), rs);
                                }
                            }
                            proposalTaskAndSuccessorMappings.add(bestMapping);
                        }

                        double additionalRSDataTransfer = getTransferTime(task.getAttribute(Attributes.INPUT_MB.name()), schedulerInput.getLocationRS(), rs) + getTransferTime(maxOutputSuccessors, schedulerInput.getLocationRS(), rs);
                        double ts_2 = getRuntime(currentScheduleTaskAndSuccessors, 0.0) - getRuntime(proposalTaskAndSuccessorMappings, additionalRSDataTransfer);
                        double additionalRSCost_2 = getRuntime(proposalTaskAndSuccessorMappings, additionalRSDataTransfer) * (schedulerInput.getResourcesRSInstances().get(rs).get(Attributes.COST_PER_HOUR.name()) / 3600.0);
                        double ac_2 = getCost(proposalTaskAndSuccessorMappings, additionalRSCost_2) - getCost(currentScheduleTaskAndSuccessors, 0.0);
                        Proposal pTaskAndSuccessors = new Proposal(proposalTaskAndSuccessorMappings, ts_2, ac_2);
                        pTaskAndSuccessors.setTsPlain(getRuntime(proposalTaskAndSuccessorMappings, 0.0));
                        pTaskAndSuccessors.setAcPlain(additionalRSCost_2);
                        pTaskAndSuccessors.setTaskIncludes(Stream.concat(Stream.of(task), GraphUtils.getSuccessorTaskNodes(specProvider.getEnactmentGraph(), task).stream()).collect(Collectors.toList()));
                        taskProposals.add(pTaskAndSuccessors);
                    }
                }
            }
            proposals.addAll(taskProposals);
        }

        // Line 3: adjust proposals [O(n^2)].
        adjustProposals(proposals);

        // Line 4: calculate cheapest schedule [O(n^2)].
        double cheapestCost = getCheapestScheduleCost(currentRSCost);

        // Line 5: check if cost limit is sufficient [O(1)].
        if(cheapestCost > schedulerInput.getCostLimit()) {

            // Line 6: exit if cost limit is not sufficient [O(1)].
            LOGGER.log(Level.INFO, "No suitable schedule meeting cost restriction.");
        }

        LOGGER.log(Level.FINER, "Current Cost = " + this.cost);
    }

    /**
     * Schedule a task.
     * [O(n^2)]
     *
     * @param taskToSchedule task to schedule.
     */
    public void schedule(Task taskToSchedule) {

        Queue<Task> toSchedule = new LinkedList<>(Collections.singletonList(taskToSchedule));

        // Iterate over each task that should be scheduled in this step (each task will be scheduled exactly once).
        while(!toSchedule.isEmpty()) {
            Task task = toSchedule.poll();

            if(currentSchedule.get(task).isFinalized()) {
                LOGGER.log(Level.FINER, "Task " + task.getId() + " already scheduled on " + getMappingString(Collections.singletonList(currentSchedule.get(task))));
                return;
            }
            LOGGER.log(Level.FINEST, "Starting Scheduling of task " + task.getId());

            // Schedule for task will be finalized
            currentSchedule.get(task).setFinalized(true);

            // Line 8: update runtime of finished tasks [O(n)].
            updateRuntimeOfFinishedTasks();

            // Line 9: adjust proposals [O(n^2)].
            if(proposalsUpdated) {
                adjustProposals(proposals);
            }

            // Line 10: sort proposals by their ts / ac value [O(n*log(n))].
            proposals.sort(Comparator.comparing(Proposal::getTradeoff).reversed());

            // Line 11: identify subset of proposals [O(n^2)].
            List<Proposal> subset = new ArrayList<>();
            List<Proposal> involvedProposals = new ArrayList<>();
            double tmpCost = cost;
            for (Proposal proposal : proposals) {
                if ((proposal.getAc() + tmpCost <= schedulerInput.getCostLimit() || proposal.getAc() < 0) && !involvedProposals.contains(proposal)) {
                    subset.add(proposal);
                    involvedProposals.add(proposal);
                    involvedProposals.addAll(proposal.getIncludesAll());
                    tmpCost += proposal.getAc();
                }
            }
            Proposal validProposal = null;
            double maxTS = 0.0;
            for (Proposal proposal : subset) { // O(n*r*2)
                for(Mapping_ m: Stream.concat(proposal.getMappings().stream(), proposal.getIncludes().stream().flatMap(p2 -> p2.getMappings().stream()).collect(Collectors.toList()).stream()).collect(Collectors.toList())) { // O(n*r*2)
                    if (m.getTask().equals(task) && maxTS < proposal.getTs()) {
                        validProposal = proposal;
                        maxTS = proposal.getTs();
                        break;
                    }
                }
            }

            // Line 12: if there is a valid proposal [O(1)].
            if(validProposal != null) {

                // Line 13: apply proposal and removed handled proposals [O(n^2)].
                List<Mapping_> validMappings = new ArrayList<>(validProposal.getMappings());
                for (Proposal proposal : validProposal.getIncludes()) {
                    validMappings.addAll(proposal.getMappings());
                }
                List<Mapping_> toRemove = new ArrayList<>();
                for(int i = 0; i < validMappings.size(); i++) {
                    for(int j = i + 1; j < validMappings.size(); j++) {
                        if(validMappings.get(i).getTask().equals(validMappings.get(j).getTask()) && validMappings.get(i).getResource().equals(validMappings.get(j).getResource())) {
                            toRemove.add(validMappings.get(j));
                        }
                    }
                }
                validMappings.removeAll(toRemove);
                for(Mapping_ mappingToApply: validMappings) {
                    mappingToApply.setRSInstanceResource(validMappings.get(0).getRSInstanceResource());
                    currentSchedule.replace(mappingToApply.getTask(), mappingToApply);
                    currentSchedule.get(mappingToApply.getTask()).setSetByOtherProposal(true);
                }
                List<Proposal> toKeep = new ArrayList<>();
                for (Proposal p : proposals) {
                    for(Task t: GraphUtils.getTasks(specificationProvider.getEnactmentGraph())) {
                        if (!currentSchedule.get(t).isSetByOtherProposal() && p.getTaskIncludes().contains(t)/*helper.get(p).containsKey(t)*/) {
                            toKeep.add(p);
                            break;
                        }
                    }
                    for(Mapping_ m: p.getMappings()) {
                        if(currentSchedule.get(m.getTask()).isSetByOtherProposal() && !currentSchedule.get(m.getTask()).getRSInstanceResource().equals(m.getRSInstanceResource())) {
                            toKeep.remove(p);
                        }
                    }
                }
                proposals = toKeep;
                proposalsUpdated = true;

                // Line 14: adjust cost [O(1)].
                double costSavingsRSInstance = validProposal.getTs() * (schedulerInput.getResourcesRSInstances().get(schedulerInput.getLocationRS()).get(Attributes.COST_PER_HOUR.name()) / 3600);
                cost += validProposal.getAc() - costSavingsRSInstance;

                LOGGER.log(Level.FINER, "Scheduled <" + getMappingString(validMappings) + ", " + validProposal.getTs() + "," + validProposal.getAc() + ">.");

                // Line 15: check if proposals contain other RS instances [O(1)].
                if(validProposal.getMappings().size() > 1) {

                    // Line 16: schedule also successor tasks [O(n)].
                    for(int i = 1; i < validMappings.size(); i++) {
                        LOGGER.log(Level.FINEST, "\t" + task.getId() + " includes scheduling of task " + validMappings.get(i).getTask());
                        toSchedule.add(validMappings.get(i).getTask());
                    }
                }
            } else {
                proposalsUpdated = false;
                LOGGER.log(Level.FINER, "Keep " + getMappingString(Collections.singletonList(currentSchedule.get(task))) + ".");
            }

            LOGGER.log(Level.FINER, "Current Cost = " + this.cost);
        }
    }

    /**
     * Adjust proposals (Algorithm 2).
     * [O(n^2)]
     *
     * @param proposals to be adjusted
     */
    private void adjustProposals(List<Proposal> proposals) {

        // Alg. 2 - Line 1: calculate est and lft of the whole workflow O[n^2].
        List<Task> allTasks = GraphUtils.getTasks(specificationProvider.getEnactmentGraph());
        List<Task> exitTasks = GraphUtils.getExitTaskNodes(specificationProvider.getEnactmentGraph());
        List<Task> entryTasks = GraphUtils.getEntryTaskNodes(specificationProvider.getEnactmentGraph());
        List<Task> remainingTasks = allTasks.stream().filter(t -> !exitTasks.contains(t) && !entryTasks.contains(t)).collect(Collectors.toList());
        HashMap<Task, Double> est = calculateEST(entryTasks, Stream.concat(remainingTasks.stream(), exitTasks.stream()).collect(Collectors.toList()));
        HashMap<Task, Double> lft = calculateLFT(est, Stream.concat(remainingTasks.stream(), entryTasks.stream()).collect(Collectors.toList()), exitTasks);

        // Alg. 2 - Line 2: iterate over all proposals [O(n)].
        for(int i = 0; i < proposals.size(); i++) {
            double ts = proposals.get(i).getTsPlain();
            double ac = proposals.get(i).getAcPlain();

            // Calculate data transfer time of affected RS instances.
            if(proposals.get(i).getMappings().size() > 1) {
                String rs = proposals.get(i).getMappings().get(0).getRSInstanceResource();
                Task t = proposals.get(i).getMappings().get(0).getTask();
                double requiredDataTransferTimePredecessors = 0.0;
                for(Task p: GraphUtils.getPredecessorTaskNodes(specificationProvider.getEnactmentGraph(), t)) {
                    if(!currentSchedule.get(p).getRSInstanceResource().equals(rs)) {
                        requiredDataTransferTimePredecessors = Math.max(requiredDataTransferTimePredecessors, getTransferTime(t.getAttribute(Attributes.INPUT_MB.name()), currentSchedule.get(p).getRSInstanceResource(), rs));
                    }
                }
                double requiredDataTransferTimeSuccessors = 0.0;
                for(Task s: GraphUtils.getSuccessorTaskNodes(specificationProvider.getEnactmentGraph(), t)) {
                    requiredDataTransferTimeSuccessors = Math.max(requiredDataTransferTimeSuccessors, getTransferTime(s.getAttribute(Attributes.OUTPUT_MB.name()), currentSchedule.get(s).getRSInstanceResource(), rs));
                }
                ac = (proposals.get(i).getTsPlain() + requiredDataTransferTimePredecessors + requiredDataTransferTimeSuccessors) * (schedulerInput.getResourcesRSInstances().get(rs).get(Attributes.COST_PER_HOUR.name()) / 3600.0);
                if(!currentSchedule.get(t).getResource().equals(proposals.get(i).getMappings().get(0).getResource())) {
                    double spareTime = lft.get(t) - est.get(t) - getRuntime(currentSchedule.get(proposals.get(i).getMappings().get(0).getTask()));
                    ts = getRuntime(currentSchedule.get(t)) - (getRuntime(proposals.get(i).getMappings().get(0)) + requiredDataTransferTimePredecessors + requiredDataTransferTimeSuccessors) - spareTime;
                }
            }

            List<Proposal> includes = new ArrayList<>();
            List<Proposal> includesAll = new ArrayList<>();
            HashMap<Task, Mapping_> considered = new HashMap<>();

            double ts_o = 0.0;

            // Alg. 2 - Line 3: for every possible overlapping proposal [O(n)].
            for(int o = 0; o < proposals.size(); o++) {

                Task t_o = proposals.get(o).getMappings().get(0).getTask();
                Resource r_o = proposals.get(o).getMappings().get(0).getResource();
                if(i != o && proposals.get(o).getMappings().size() == 1 &&
                        currentSchedule.get(t_o).getResource() != r_o) {

                    double spareTime = getLFT(lft, proposals.get(o)) - getEST(est, proposals.get(o)) - getRuntime(currentSchedule.get(proposals.get(o).getMappings().get(0).getTask()));

                    // Alg. 2 - Line 4: check if proposals overlap and if spare time is not sufficient [O(1)].
                    if(getEST(est, proposals.get(i)) < getLFT(lft, proposals.get(o)) - TOLERANCE && getLFT(lft, proposals.get(i)) - TOLERANCE >
                        getEST(est, proposals.get(o)) && spareTime < proposals.get(o).getTs() ) {

                        // Alg. 2 - Line 5: adjust ac and ts based on overlapping proposals [O(1)].
                        Mapping_ overlappingMapping = new Mapping_(t_o, r_o, proposals.get(i).getMappings().get(0).getRSInstanceResource());
                        if(t_o != proposals.get(i).getMappings().get(0).getTask()) {
                            ts_o = Math.max(ts_o, getRuntime(currentSchedule.get(t_o)) - getRuntime(overlappingMapping) - spareTime);
                        }

                        // If there are more than one proposal available for a task, take the proposal that maximizes runtime saving [O(1)].
                        if(considered.containsKey(t_o)) {
                            ac = ac - getCost(considered.get(t_o)) - getCost(currentSchedule.get(t_o));
                        }
                        ac = ac + getCost(overlappingMapping) - getCost(currentSchedule.get(t_o));
                        includes.add(proposals.get(o));
                        considered.put(t_o, overlappingMapping);
                    }
                }

                if(getEST(est, proposals.get(i)) <= getEST(est, proposals.get(o)) && getLFT(lft, proposals.get(i)) <= getLFT(lft, proposals.get(o))) {
                    includesAll.add(proposals.get(o));
                }
            }

            proposals.get(i).setAc(ac);
            proposals.get(i).setTs(proposals.get(i).getMappings().size() > 1 ? ts + ts_o : Math.max(ts, ts_o));
            proposals.get(i).setIncludes(includes);
            proposals.get(i).setIncludesAll(includesAll);
            proposals.get(i).calculateTradeOff();
        }
    }

    /**
     * Parse the input of the scheduler.
     * [O(n)]
     *
     * @param input input json object.
     */
    private SchedulerInput getSchedulerInput(JsonObject input) {

        SchedulerInput schedulerInput = new SchedulerInput();

        // Parse the input as JSON object
        schedulerInput.setCostLimit(input.get("costLimit").getAsDouble());
        schedulerInput.setLocationRS(input.get("locationRS").getAsString());

        // Iterate over all tasks
        for (JsonElement task : input.get("tasks").getAsJsonArray()) {

            // Get the task as Json object
            JsonObject taskObj = task.getAsJsonObject();

            // Set the input and output data sizes
            Task taskSpec = specificationProvider.getEnactmentGraph().getVertex(taskObj.get("id").getAsString());
            taskSpec.setAttribute(Attributes.INPUT_MB.name(), taskObj.get("inputMB").getAsDouble());
            taskSpec.setAttribute(Attributes.OUTPUT_MB.name(), taskObj.get("outputMB").getAsDouble());

            // Iterate over all resources of a task
            for (JsonElement resources : taskObj.get("resourceTypes").getAsJsonArray()) {

                // Get the resource as Json object
                JsonObject resourceObj = resources.getAsJsonObject();

                // Set the runtime duration
                Objects.requireNonNull(findSpecMapping(taskSpec, resourceObj.get("id").getAsString()))
                    .setAttribute(Attributes.RUNTIME.name(), resourceObj.get("runtime").getAsDouble());
            }
        }

        // Iterate over all task resource types
        for (JsonElement taskResourceType : input.get("taskResourceTypes").getAsJsonArray()) {

            // Get the taskResourceType as Json object
            JsonObject taskResourceTypeObj = taskResourceType.getAsJsonObject();

            // Set resource configurations
            Resource resourceSpec = specificationProvider.getResourceGraph().getVertex(taskResourceTypeObj.get("id").getAsString());
            resourceSpec.setAttribute(Attributes.COST_PER_HOUR.name(), taskResourceTypeObj.get("costPerHour").getAsDouble());
            resourceSpec.setAttribute(Attributes.BANDWIDTH.name(), taskResourceTypeObj.get("bandwidth").getAsDouble());
            resourceSpec.setAttribute(Attributes.ACQUISITION_DELAY.name(), taskResourceTypeObj.get("acquisitionDelay").getAsDouble());
        }

        // Iterate over all RS resource types
        HashMap<String, HashMap<String, Double>> resourcesRSInstances = new HashMap<>();
        for (JsonElement taskResourceType : input.get("RSResourceTypes").getAsJsonArray()) {

            // Get the RSResourceType as Json object
            JsonObject RSResourceTypeObj = taskResourceType.getAsJsonObject();

            // Set resource configurations
            HashMap<String, Double> resourcesRSInstanceDetail = new HashMap<>();
            resourcesRSInstanceDetail.put(Attributes.COST_PER_HOUR.name(), RSResourceTypeObj.get("costPerHour").getAsDouble());
            resourcesRSInstanceDetail.put(Attributes.BANDWIDTH.name(), RSResourceTypeObj.get("bandwidth").getAsDouble());
            resourcesRSInstanceDetail.put(Attributes.ACQUISITION_DELAY.name(), RSResourceTypeObj.get("acquisitionDelay").getAsDouble());
            resourcesRSInstances.put(RSResourceTypeObj.get("id").getAsString(), resourcesRSInstanceDetail);
        }
        schedulerInput.setResourcesRSInstances(resourcesRSInstances);
        return schedulerInput;
    }

    /**
     * Get the rs instance resource that minimizes communication time.
     * [O(1)]
     *
     * @param reference resource.
     *
     * @return resource that minimizes communication time.
     */
    private String resourceMinimizingCommunication(Resource reference){

        // Get resources above bandwidth threshold
        double referenceBandwidth = reference.getAttribute(Attributes.BANDWIDTH.name());
        List<String> resourcesAboveThreshold = new ArrayList<>();
        for(String rs: schedulerInput.getResourcesRSInstances().keySet()) {
            if(schedulerInput.getResourcesRSInstances().get(rs).get(Attributes.BANDWIDTH.name()) >= referenceBandwidth){
                resourcesAboveThreshold.add(rs);
            }
        }

        // Find resource with lowest acquisition delay
        List<String> resourcesMinAcquisitionDelay = new ArrayList<>();
        double minAcquisitionDelay = Double.MAX_VALUE;
        for(String r: resourcesAboveThreshold) {
            minAcquisitionDelay = Math.min(minAcquisitionDelay, schedulerInput.getResourcesRSInstances().get(r).get(Attributes.ACQUISITION_DELAY.name()));
        }
        for(String r: resourcesAboveThreshold) {
            if(schedulerInput.getResourcesRSInstances().get(r).get(Attributes.ACQUISITION_DELAY.name()).equals(minAcquisitionDelay)) {
                resourcesMinAcquisitionDelay.add(r);
            }
        }

        // Find resource with minimum cost
        List<String> resourcesMinCost = new ArrayList<>();
        double minCost = Double.MAX_VALUE;
        for(String r: resourcesMinAcquisitionDelay) {
            minCost = Math.min(minCost, schedulerInput.getResourcesRSInstances().get(r).get(Attributes.COST_PER_HOUR.name()));
        }
        for(String r: resourcesMinAcquisitionDelay) {
            if(schedulerInput.getResourcesRSInstances().get(r).get(Attributes.COST_PER_HOUR.name()).equals(minCost)) {
                resourcesMinCost.add(r);
            }
        }

        return resourcesMinCost.get(0);
    }

    /**
     * Get the cost of the cheapest schedule.
     * [O(n^2)]
     *
     * @return cheapest schedule.
     */
    private double getCheapestScheduleCost(double currentRSCost) {

        double cost = 0.0;

        // Iterate over all tasks.
        for(Task t: GraphUtils.getTasks(specificationProvider.getEnactmentGraph())) {

            // Find proposal of task with minimal additional cost.
            double minAC = Double.MAX_VALUE;
            for (Proposal p: proposals) {
                if(p.getMappings().get(0).getTask().equals(t)) {
                    minAC = Math.min(minAC, p.getAc());
                }
            }

            cost += Math.min(getCost(currentSchedule.get(t)), getCost(currentSchedule.get(t)) + minAC);
        }

        return cost + currentRSCost;
    }

    /**
     * Find the specification mapping of a task and a resource.
     * [O(r)]
     *
     * @param task to find mapping.
     * @param resourceId identifier of the resource.
     *
     * @return mapping if found, null otherwise.
     */
    private Mapping<Task, Resource> findSpecMapping(Task task, String resourceId) {

        // Iterate over specification mappings
        for(Mapping<Task, Resource> taskMappingSpec: specificationProvider.getMappings().getMappings(task)) {

            // Set the runtime duration
            if(resourceId.equals(taskMappingSpec.getTarget().getId())) {
                return taskMappingSpec;
            }
        }
        return null;
    }

    /**
     * Get the transfer time of a mapping.
     * [O(1)]
     *
     * @param mapping to get transfer time from.
     *
     * @return transfer time.
     */
    private double getTransferTime(Mapping_ mapping, boolean considerInput, boolean considerOutput) {

        double inputMB = considerInput ? specificationProvider.getEnactmentGraph().getVertex(mapping.getTask().getId()).getAttribute(Attributes.INPUT_MB.name()) : 0.0;
        double outputMB = considerOutput ? specificationProvider.getEnactmentGraph().getVertex(mapping.getTask().getId()).getAttribute(Attributes.OUTPUT_MB.name()) : 0.0;
        double bandwidthResource = specificationProvider.getResourceGraph().getVertex(mapping.getResource().getId()).getAttribute(Attributes.BANDWIDTH.name());
        double bandwidthRSResource = schedulerInput.getResourcesRSInstances().get(mapping.getRSInstanceResource()).get(Attributes.BANDWIDTH.name());

        double transferTimeIn = inputMB / Math.min(bandwidthResource / 8.0, bandwidthRSResource / 8.0);
        double transferTimeOut = outputMB / Math.min(bandwidthResource / 8.0, bandwidthRSResource / 8.0);
        return transferTimeIn + transferTimeOut;
    }

    /**
     * Get the transfer time to transfer a specific amount of data from one RS instance to another.
     * [O(1)]
     *
     * @param dataSize the amount of data.
     * @param rs1 resource from.
     * @param rs2 resource to.
     *
     * @return transfer time.
     */
    private double getTransferTime(double dataSize, String rs1, String rs2) {

        double bandwidthRS1 = schedulerInput.getResourcesRSInstances().get(rs1).get(Attributes.BANDWIDTH.name());
        double bandwidthRS2 = schedulerInput.getResourcesRSInstances().get(rs2).get(Attributes.BANDWIDTH.name());

        return dataSize / Math.min(bandwidthRS1 / 8.0, bandwidthRS2 / 8.0);
    }

    /**
     * Get the runtime of a mapping.
     * [O(1)]
     *
     * @param mapping to get the runtime from.
     *
     * @return runtime.
     */
    private double getRuntime(Mapping_ mapping) {

        double acquisitionDelay = specificationProvider.getResourceGraph().getVertex(mapping.getResource().getId()).getAttribute(Attributes.ACQUISITION_DELAY.name());
        double runtimeTask = Objects.requireNonNull(findSpecMapping(mapping.getTask(), mapping.getResource().getId())).getAttribute(Attributes.RUNTIME.name());

        return acquisitionDelay + runtimeTask + getTransferTime(mapping, true, true);
    }

    /**
     * Get the cost of a mapping.
     * [O(1)]
     *
     * @param mapping to get the cost from.
     *
     * @return cost.
     */
    private double getCost(Mapping_ mapping) {

        double acquisitionDelay = specificationProvider.getResourceGraph().getVertex(mapping.getResource().getId()).getAttribute(Attributes.ACQUISITION_DELAY.name());
        double costPerHour = specificationProvider.getResourceGraph().getVertex(mapping.getResource().getId()).getAttribute(Attributes.COST_PER_HOUR.name());

        if(!EXCLUDE_DATA_TRANSFER_COST) {
            return (getRuntime(mapping) - acquisitionDelay) * (costPerHour / 3600);
        } else {
            double runtimeTask = Objects.requireNonNull(findSpecMapping(mapping.getTask(), mapping.getResource().getId())).getAttribute(Attributes.RUNTIME.name());
            return runtimeTask * (costPerHour / 3600);
        }
    }

    /**
     * Get the cheapest resource of a task, while data is transfered trough a RS instance on resource rs.
     * [O(1)]
     *
     * @param task to get cheapest resource.
     * @param rs resource of the RS instance handling data transfer.
     *
     * @return resource.
     */
    private Resource getCheapestResource(Task task, String rs) {

        Resource cheapest = null;
        double minCost = Double.MAX_VALUE;
        double minRuntime = Double.MAX_VALUE;

        // Iterate over all possible mappings of the task.
        for(Mapping<Task, Resource> taskMappingSpec: specificationProvider.getMappings().getMappings(task)) {

            // Get cheapest resource.
            Mapping_ tmpMapping = new Mapping_(task, taskMappingSpec.getTarget(), rs);
            double cost = getCost(tmpMapping);
            if(cost < minCost) {
                minCost = cost;
                minRuntime = getRuntime(tmpMapping);
                cheapest = taskMappingSpec.getTarget();
            }
            // If multiple resource have the same cost, take the faster one.
            else if(cost == minCost && getRuntime(tmpMapping) < minRuntime) {
                minRuntime = getRuntime(tmpMapping);
                cheapest = taskMappingSpec.getTarget();
            }
        }

        return cheapest;
    }

    /**
     * Calculate the EST (Earliest Start Time) of each task in the workflow.
     * [O(n^2)]
     *
     * @return EST of each task.
     */
    private HashMap<Task, Double> calculateEST(List<Task> entryTasks, List<Task> remaining){

        HashMap<Task, Double> est = new HashMap<>();

        // Initialize queue to keep track of task nodes to look for
        Queue<Task> queue = new LinkedList<>();
        List<Task> tasksToConsider = Stream.concat(entryTasks.stream(), remaining.stream()).collect(Collectors.toList());

        for (Task t : entryTasks) { // O(n)
            est.put(t, getTransferTime(new Mapping_(currentSchedule.get(t).getTask(), currentSchedule.get(t).getResource(), schedulerInput.getLocationRS()), true, false));
            queue.addAll(GraphUtils.getSuccessorTaskNodes(specification.getEnactmentGraph(), t).stream().filter(tasksToConsider::contains).collect(Collectors.toList())); // O(n)
        }

        // While there are still est values to calculate
        computeESTLoop:
        while (!queue.isEmpty()) { // O(n)

            // Get the task to calculate the est value
            Task t = queue.poll();

            // Check if all predecessors have already computed EST
            for (Task p : GraphUtils.getPredecessorTaskNodes(specification.getEnactmentGraph(), t).stream().filter(tasksToConsider::contains).collect(Collectors.toList())) { // O(n)
                if (!est.containsKey(p)) {
                    queue.add(t);
                    continue computeESTLoop;
                }
            }

            // Calculate the EST value
            est.put(t, 0.0);
            for (Task p : GraphUtils.getPredecessorTaskNodes(specification.getEnactmentGraph(), t).stream().filter(tasksToConsider::contains).collect(Collectors.toList())) { // O(n)
                double additionalRSDataTransfer = 0.0;
                if(!currentSchedule.get(t).getRSInstanceResource().equals(currentSchedule.get(p).getRSInstanceResource())) {
                    additionalRSDataTransfer += getTransferTime(t.getAttribute(Attributes.INPUT_MB.name()), currentSchedule.get(t).getRSInstanceResource(), currentSchedule.get(p).getRSInstanceResource());
                }
                est.replace(t, Math.max(est.get(p) + getRuntime(currentSchedule.get(p)) + additionalRSDataTransfer, est.get(t)));
            }

            // Continue with the successor nodes, if those are not exit tasks
            queue.addAll(GraphUtils.getSuccessorTaskNodes(specification.getEnactmentGraph(), t).stream().filter(tasksToConsider::contains).collect(Collectors.toList())); // O(n)
        }

        return est;
    }

    /**
     * Calculate the LFT (Latest Finish Time) of each task in the workflow.
     * [O(n^2)]
     *
     * @return LFT of each task.
     */
    private HashMap<Task, Double> calculateLFT(HashMap<Task, Double> est, List<Task> remaining, List<Task> exitTasks){
        HashMap<Task, Double> lft = new HashMap<>();

        // Initialize queue to keep track of task nodes to look for
        Queue<Task> queue = new LinkedList<>();
        List<Task> tasksToConsider = Stream.concat(remaining.stream(), exitTasks.stream()).collect(Collectors.toList());

        for (Task t : exitTasks) { // O(n)
            lft.put(t, 0.0);
            for (Task ti : tasksToConsider) {
                lft.replace(t, Math.max(est.get(ti) + getRuntime(currentSchedule.get(ti)), lft.get(t)));
            }
            queue.addAll(GraphUtils.getPredecessorTaskNodes(specification.getEnactmentGraph(), t).stream().filter(tasksToConsider::contains).collect(Collectors.toList())); // O(n)
        }

        // While there are still est values to calculate
        computeLFTLoop:
        while (!queue.isEmpty()) { // O(n)

            // Get the task to calculate the est value
            Task t = queue.poll();

            // Check if all predecessors have already computed LFT
            for (Task s : GraphUtils.getSuccessorTaskNodes(specification.getEnactmentGraph(), t).stream().filter(tasksToConsider::contains).collect(Collectors.toList())) { // O(n)
                if (!lft.containsKey(s)) {
                    queue.add(t);
                    continue computeLFTLoop;
                }
            }

            // Calculate the LFT value
            lft.put(t, Double.MAX_VALUE);
            for (Task s : GraphUtils.getSuccessorTaskNodes(specification.getEnactmentGraph(), t).stream().filter(tasksToConsider::contains).collect(Collectors.toList())) { // O(n)
                lft.replace(t, Math.min(lft.get(s) - getRuntime(currentSchedule.get(s)), lft.get(t)));
            }

            // Continue with the successor nodes
            queue.addAll(GraphUtils.getPredecessorTaskNodes(specification.getEnactmentGraph(), t).stream().filter(tasksToConsider::contains).collect(Collectors.toList())); // O(n)
        }

        return lft;
    }

    /**
     * Get the runtime of a list of mappings.
     * [O(n^2)]
     *
     * @param mappings to get the runtime from.
     * @param epsilon additional runtime for data transfer between RS instances
     *
     * @return runtime.
     */
    private double getRuntime(List<Mapping_> mappings, double epsilon) {

        // Temporarily set mappings
        List<Mapping_> backup = new ArrayList<>();
        for(Mapping_ mapping: mappings) {
            backup.add(currentSchedule.get(mapping.getTask()));
            currentSchedule.replace(mapping.getTask(), mapping);
        }

        // calculate est and lft
        List<Task> tasksToConsider = mappings.stream().map(Mapping_::getTask).collect(Collectors.toList());
        ArrayList<Task> exitTasks = new ArrayList<>();
        ArrayList<Task> entryTasks = new ArrayList<>();
        ArrayList<Task> remainingTasks = new ArrayList<>();
        for(Task t: tasksToConsider) {
            if(GraphUtils.getSuccessorTaskNodes(specification.getEnactmentGraph(), t).stream().noneMatch(tasksToConsider::contains)){
                exitTasks.add(t);
            } else if (GraphUtils.getPredecessorTaskNodes(specification.getEnactmentGraph(), t).stream().noneMatch(tasksToConsider::contains)){
                entryTasks.add(t);
            } else {
                remainingTasks.add(t);
            }
        }
        HashMap<Task, Double> est = calculateEST(entryTasks, Stream.concat(exitTasks.stream(), remainingTasks.stream()).collect(Collectors.toList()));
        HashMap<Task, Double> lft = calculateLFT(est, Stream.concat(entryTasks.stream(), remainingTasks.stream()).collect(Collectors.toList()), exitTasks);

        // Undo temporal mappings
        for(Mapping_ mapping: backup) {
            currentSchedule.replace(mapping.getTask(), mapping);
        }

        double maxLft = 0.0;
        for (Mapping_ mapping: mappings) {
            maxLft = Math.max(lft.get(mapping.getTask()), maxLft);
        }

        double minEst = Double.MAX_VALUE;
        for (Mapping_ mapping: mappings) {
            minEst = Math.min(est.get(mapping.getTask()), minEst);
        }

        return maxLft - minEst + epsilon;
    }

    /**
     * Get the cost of a list of mappings.
     * [O(n)]
     *
     * @param mappings to get the cost from.
     * @param epsilon additional cost of affected RS instances
     *
     * @return cost.
     */
    private double getCost(List<Mapping_> mappings, double epsilon) {

        double sum = 0.0;

        for(Mapping_ mapping: mappings) {
            sum += getCost(mapping);
        }

        return sum + epsilon;
    }

    /**
     * Get EST for a given proposal.
     * [O(1)]
     *
     * @param estValues the calculated EST values.
     * @param proposal the proposal for which to get EST.
     *
     * @return EST value.
     */
    private double getEST(HashMap<Task, Double> estValues, Proposal proposal) {
        return estValues.get(proposal.getMappings().get(0).getTask());
    }

    /**
     * Get LFT for a given proposal.
     * [O(n)]
     *
     * @param lftValues the calculated LFT values.
     * @param proposal the proposal for which to get LFT.
     *
     * @return LFT value.
     */
    private double getLFT(HashMap<Task, Double> lftValues, Proposal proposal) {
        double maxLft = 0;
        for(Mapping_ mapping: proposal.getMappings()){
            maxLft = Math.max(maxLft, lftValues.get(mapping.getTask()));
        }
        return maxLft;
    }

    /**
     * Update the runtime of already finished tasks to represent an accurate value of the actual execution.
     * [O(n)]
     */
    private void updateRuntimeOfFinishedTasks() {
        // Omitted - since tasks are not executed in this simulation.
    }

    /**
     * Schedule a task on a resource (Line 7).
     * [O(n)]
     *
     * @param task the given task
     * @param mappingOptions all mapping options for the given task
     *
     * @return the final task-resource mapping for the task.
     */
    @Override protected Set<Mapping<Task, Resource>> chooseMappingSubset(Task task, Set<Mapping<Task, Resource>> mappingOptions) {

        // not implemented in the simulator (consider the schedule function)
        return null;
    }

    /**
     * Create a string containing all mappings (for illustration purpose only).
     * [O(n)]
     *
     * @param validMappings mappings to include.
     *
     * @return mapping string.
     */
    private StringBuilder getMappingString(List<Mapping_> validMappings) {
        StringBuilder mappingsString = new StringBuilder();
        for(Mapping_ mappingToApply: validMappings) {
            mappingsString.append("<").append(mappingToApply.getTask()).append(",").append(mappingToApply.getResource()).append(",").append(mappingToApply.getRSInstanceResource()).append(">");
        }
        return mappingsString;
    }

    /**
     * Get statistics of the current schedule.
     * [O(n^2)]
     *
     * @return statistics of the current schedule.
     */
    Statistics getStatistics() {
        Statistics statistics = new Statistics();

        // Calculate the finish time of the workflow
        List<Task> allTasks = GraphUtils.getTasks(specificationProvider.getEnactmentGraph());
        List<Task> exitTasks = GraphUtils.getExitTaskNodes(specificationProvider.getEnactmentGraph());
        List<Task> entryTasks = GraphUtils.getEntryTaskNodes(specificationProvider.getEnactmentGraph());
        List<Task> remainingTasks = allTasks.stream().filter(t -> !exitTasks.contains(t) && !entryTasks.contains(t)).collect(Collectors.toList());
        HashMap<Task, Double> est = calculateEST(entryTasks, Stream.concat(remainingTasks.stream(), exitTasks.stream()).collect(Collectors.toList()));
        HashMap<Task, Double> lft = calculateLFT(est, Stream.concat(remainingTasks.stream(), entryTasks.stream()).collect(Collectors.toList()), exitTasks);
        double runtime = 0.0;
        for(Task t: lft.keySet()) {
            runtime = Math.max(runtime, lft.get(t));
        }

        // Set statistics
        statistics.setCost(cost);
        statistics.setRuntime(runtime);

        LOGGER.log(Level.INFO, "Workflow results: cost=" + statistics.getCost() + ", runtime=" + statistics.getRuntime());

        return statistics;
    }
}
