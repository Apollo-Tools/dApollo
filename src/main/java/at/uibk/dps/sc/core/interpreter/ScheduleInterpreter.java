package at.uibk.dps.sc.core.interpreter;

import java.util.Set;
import com.google.inject.ImplementedBy;
import at.uibk.dps.ee.core.function.EnactmentFunction;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * The {@link ScheduleInterpreter} translates the model of a task schedule (a
 * set of mappings) into an actual {@link EnactmentFunction}.
 * 
 * @author Fedor Smirnov
 *
 */
@ImplementedBy(ScheduleInterpreterAll.class)
public interface ScheduleInterpreter {

  /**
   * Translates the given schedule model into an actual {@link EnactmentFunction}.
   * 
   * @param task the task to be scheduled
   * @param scheduleModel the schedule model
   * @return the {@link EnactmentFunction} resulting from the schedule model
   */
  EnactmentFunction interpretSchedule(Task task, Set<Mapping<Task, Resource>> scheduleModel);
}
