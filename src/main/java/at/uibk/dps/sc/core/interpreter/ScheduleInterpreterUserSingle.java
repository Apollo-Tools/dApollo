package at.uibk.dps.sc.core.interpreter;

import java.util.Set;
import com.google.inject.Inject;
import at.uibk.dps.ee.core.function.EnactmentFunction;
import at.uibk.dps.ee.enactables.FunctionFactoryUser;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * The {@link ScheduleInterpreterUserSingle} expects to get a schedule with
 * exactly one mapping.
 * 
 * @author Fedor Smirnov
 *
 */
public class ScheduleInterpreterUserSingle extends ScheduleInterpreterUser {

  /**
   * Injection constructor.
   * 
   */
  @Inject
  public ScheduleInterpreterUserSingle(final Set<FunctionFactoryUser> userFactories) {
    super(userFactories);
  }

  @Override
  protected EnactmentFunction interpretScheduleUser(final Task task,
      final Set<Mapping<Task, Resource>> scheduleModel) {
    return getFunctionForMapping(task, scheduleModel.iterator().next());
  }

  @Override
  protected void checkSchedule(final Task task, final Set<Mapping<Task, Resource>> scheduleModel) {
    super.checkSchedule(task, scheduleModel);
    if (scheduleModel.size() != 1) {
      throw new IllegalArgumentException(
          "The configured schedule interpreter user expects exactly one mapping. Task with problem: "
              + task.getId());
    }
  }
}
