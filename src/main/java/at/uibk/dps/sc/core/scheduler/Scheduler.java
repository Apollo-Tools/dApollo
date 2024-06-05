package at.uibk.dps.sc.core.scheduler;

import java.util.Set;
import io.vertx.core.Future;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * Interface for the classes making the scheduling decisions.
 * 
 * @author Fedor Smirnov
 */
public interface Scheduler {

  /**
   * Schedules the given function node. Returns a future for a set of (annotated)
   * mapping edges defining the task schedule.
   * 
   * @param task the function node to schedule
   * @return a future for a set of (annotated) mapping edges defining the task
   *         schedule
   */
  Future<Set<Mapping<Task, Resource>>> scheduleTask(Task task);
}
