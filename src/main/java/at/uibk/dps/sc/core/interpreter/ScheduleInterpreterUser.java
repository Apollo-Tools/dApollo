package at.uibk.dps.sc.core.interpreter;

import java.util.Set;
import java.util.stream.Collectors;
import at.uibk.dps.ee.core.function.EnactmentFunction;
import at.uibk.dps.ee.enactables.FactoryInputUser;
import at.uibk.dps.ee.enactables.FunctionFactoryUser;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * The {@link ScheduleInterpreterUser} is used to schedule user tasks.
 * 
 * @author Fedor Smirnov
 *
 */
public abstract class ScheduleInterpreterUser implements ScheduleInterpreter {

  protected final Set<FunctionFactoryUser> userFactories;

  /**
   * Default constructor.
   * 
   * @param functionFactoryLocal the factory for the creation of
   *        {@link EnactmentFunction}s performing local calculation
   * @param functionFactoryDemo the factory for the creation of demo functions
   *        implemented natively
   * @param functionFactorySl the factory for the creation of serverless functions
   */
  public ScheduleInterpreterUser(final Set<FunctionFactoryUser> userFactories) {
    this.userFactories = userFactories;
  }

  @Override
  public final EnactmentFunction interpretSchedule(final Task task,
      final Set<Mapping<Task, Resource>> scheduleModel) {
    checkSchedule(task, scheduleModel);
    return interpretScheduleUser(task, scheduleModel);
  }

  /**
   * Checks the schedule before processing. Throws an exception if the schedule
   * does not comply with the interpreter's expectations.
   * 
   * @param task the scheduled task
   * @param scheduleModel the schedule model
   */
  protected void checkSchedule(final Task task, final Set<Mapping<Task, Resource>> scheduleModel) {
    if (scheduleModel.isEmpty()) {
      throw new IllegalArgumentException("A user task must be scheduled to at least one mapping.");
    }
  }

  /**
   * Returns the enactment function corresponding to the provided mapping edge.
   * 
   * @param task the task for which the function is created
   * @param mapping the provided mapping edge
   * @return the enactment function corresponding to the provided mapping edge
   */
  protected EnactmentFunction getFunctionForMapping(final Task task,
      final Mapping<Task, Resource> mapping) {
    final FactoryInputUser factoryInput = new FactoryInputUser(task, mapping);
    final Set<FunctionFactoryUser> applicableFactories = userFactories.stream(). //
        filter(uFactory -> uFactory.isApplicable(factoryInput)). //
        collect(Collectors.toSet());
    if (applicableFactories.size() != 1) {
      throw new IllegalStateException(
          "Not exactly one factory for task " + task.getId() + "; mapping " + mapping.getId()
              + ". Number of factories: " + applicableFactories.size());
    }
    final FunctionFactoryUser factory = applicableFactories.iterator().next();
    return factory.makeFunction(factoryInput);
  }

  /**
   * Method doing the actual interpreting for the given user task.
   * 
   * @param task the user task
   * @param scheduleModel the schedule model
   * @return the enactment function resulting from the schedule.
   */
  protected abstract EnactmentFunction interpretScheduleUser(final Task task,
      final Set<Mapping<Task, Resource>> scheduleModel);
}
