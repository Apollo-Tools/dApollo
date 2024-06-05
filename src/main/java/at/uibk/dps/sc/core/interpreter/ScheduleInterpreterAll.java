package at.uibk.dps.sc.core.interpreter;

import java.util.Set;
import com.google.inject.Inject;
import at.uibk.dps.ee.core.function.EnactmentFunction;
import at.uibk.dps.ee.model.properties.PropertyServiceFunction;
import at.uibk.dps.ee.model.properties.PropertyServiceFunction.UsageType;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * The {@link ScheduleInterpreterAll} is the interpreter receiving all tasks
 * which are to be scheduled.
 * 
 * @author Fedor Smirnov
 *
 */
public class ScheduleInterpreterAll implements ScheduleInterpreter {

  protected final ScheduleInterpreterEE interpreterEE;
  protected final ScheduleInterpreterUser interpreterUser;

  /**
   * Injection constructor.
   * 
   * @param interpreterEE the interpreter for the functions required for EE
   *        functionalities
   * @param interpreterUser the interpreter for the functions calculating user
   *        functions
   */
  @Inject
  public ScheduleInterpreterAll(final ScheduleInterpreterEE interpreterEE,
      final ScheduleInterpreterUser interpreterUser) {
    this.interpreterEE = interpreterEE;
    this.interpreterUser = interpreterUser;
  }

  @Override
  public EnactmentFunction interpretSchedule(final Task task,
      final Set<Mapping<Task, Resource>> scheduleModel) {
    final UsageType usage = PropertyServiceFunction.getUsageType(task);
    if (usage.equals(UsageType.User)) {
      return interpreterUser.interpretSchedule(task, scheduleModel);
    } else if (usage.equals(UsageType.DataFlow) || usage.equals(UsageType.Utility)) {
      return interpreterEE.interpretSchedule(task, scheduleModel);
    } else {
      throw new IllegalArgumentException("Unknown usage type " + usage.name());
    }
  }
}
