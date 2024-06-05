package at.uibk.dps.sc.core.scheduler;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import at.uibk.dps.ee.guice.starter.VertxProvider;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.graph.SpecificationProvider;
import at.uibk.dps.ee.model.properties.PropertyServiceFunction;
import at.uibk.dps.ee.model.properties.PropertyServiceFunction.UsageType;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUser;
import at.uibk.dps.ee.model.properties.PropertyServiceResource;
import at.uibk.dps.sc.core.ConstantsScheduling;
import at.uibk.dps.sc.core.capacity.CapacityCalculator;
import at.uibk.dps.sc.core.capacity.CapacityLimitException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * Abstract class to define the general scheduling process which is based on the
 * processing of the {@link EnactmentSpecification}.
 * 
 * @author Fedor Smirnov
 */
public abstract class SchedulerAbstract implements Scheduler {

  protected final EnactmentSpecification specification;
  protected final CapacityCalculator capacityCalculator;
  protected final Vertx vertx;

  /**
   * Default constructor
   * 
   * @param specProvider specification provider
   */
  public SchedulerAbstract(final SpecificationProvider specProvider,
      final CapacityCalculator capacityCalculator, final VertxProvider vertProv) {
    this.specification = specProvider.getSpecification();
    this.capacityCalculator = capacityCalculator;
    this.vertx = vertProv.getVertx();
  }

  @Override
  public Future<Set<Mapping<Task, Resource>>> scheduleTask(final Task task) {
    final Promise<Set<Mapping<Task, Resource>>> resultPromise = Promise.promise();
    final Set<Mapping<Task, Resource>> result = new HashSet<>();
    if (PropertyServiceFunction.getUsageType(task).equals(UsageType.User)) {
      // user task -> scheduled based on mappings
      final Task taskKey = getOriginalTask(task);
      final Set<Mapping<Task, Resource>> specMappings =
          specification.getMappings().getMappings(taskKey);
      if (specMappings.isEmpty()) {
        throw new IllegalStateException(
            "No mapping options provided for the task " + taskKey.getId());
      }
      // synchronized capacity look up + task placement
      this.vertx.sharedData().getLock(ConstantsScheduling.lockCapacityQuery, lockRes -> {
        if (lockRes.succeeded()) {
          final Lock capacityLock = lockRes.result();
          final Set<Mapping<Task, Resource>> validMappings =
              specMappings.stream().filter(m -> isValidMapping(m)).collect(Collectors.toSet());
          if (validMappings.isEmpty()) {
            placeTaskInWaitList(task, resultPromise);
          } else {
            scheduleSuccess(validMappings, task, resultPromise);
          }
          capacityLock.release();
        } else {
          throw new IllegalStateException("Failed to get capacity query lock");
        }
      });
    } else {
      // not a user task -> no scheduling
      resultPromise.complete(result);
    }
    return resultPromise.future();
  }

  /**
   * Places the task (which is unschedulable for now due to capacity limits) into
   * the waiting list
   * 
   * @param task the scheduled task
   * @param promise the promise made to the scheduling verticle
   */
  protected void placeTaskInWaitList(final Task task,
      final Promise<Set<Mapping<Task, Resource>>> promise) {
    promise.fail(new CapacityLimitException(task));
  }

  /**
   * Called to inform the scheduling verticle of the chosen schedule.
   * 
   * @param schedule the chosen schedule
   * @param task the task being scheduled
   * @param promise the promise made to the scheduling verticle
   */
  protected void scheduleSuccess(final Set<Mapping<Task, Resource>> schedule, final Task task,
      final Promise<Set<Mapping<Task, Resource>>> promise) {
    final Set<Mapping<Task, Resource>> result = new HashSet<>();
    result.addAll(chooseMappingSubset(task, schedule));
    result.forEach(m -> PropertyServiceResource.addUsingTask(task, m.getTarget()));
    promise.complete(result);
  }

  /**
   * Returns true if the given mapping can be used at the current moment (used to
   * consider resource capacity by default)
   * 
   * @param mapping the given mapping
   * @return true iff the given mapping can be used at the given moment
   */
  protected boolean isValidMapping(final Mapping<Task, Resource> mapping) {
    final Resource targetRes = mapping.getTarget();
    final Set<String> alreadyOnResource = PropertyServiceResource.getUsingTaskIds(targetRes);
    final double unavailableCapacity = alreadyOnResource.stream()
        .map(taskId -> specification.getEnactmentGraph().getVertex(taskId))
        .mapToDouble(task -> capacityCalculator.getCapacityFraction(task, targetRes)).sum();
    final double requiredCapacity =
        capacityCalculator.getCapacityFraction(mapping.getSource(), targetRes);
    return requiredCapacity + unavailableCapacity <= 1.0;
  }

  /**
   * Returns the original task defined in the {@link EnactmentGraph} of the
   * specification (as opposed to, e.g., the reproductions created during the
   * parallel for distribution).
   * 
   * @param task the given task
   * @return the original task from the spec (either the given task or its
   *         (grand)parent)
   */
  protected Task getOriginalTask(final Task task) {
    if (task.getParent() == null) {
      if (PropertyServiceFunctionUser.isWhileReplica(task)) {
        return specification.getEnactmentGraph()
            .getVertex(PropertyServiceFunctionUser.getWhileRef(task));
      } else {
        return task;
      }
    } else {
      return getOriginalTask((Task) task.getParent());
    }
  }

  /**
   * Method provided with a mapping set representing all possible bindings of the
   * given task. Returns a subset of these mappings representing an actual
   * schedule.
   * 
   * @param task the given task
   * @param mappingOptions all mapping options for the given task
   * @return a mapping subset representing a schedule
   */
  protected abstract Set<Mapping<Task, Resource>> chooseMappingSubset(final Task task,
      final Set<Mapping<Task, Resource>> mappingOptions);
}
