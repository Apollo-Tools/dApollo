package at.uibk.dps.sc.core.arbitration;

import java.util.List;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * Interface for all classes used to arbitrate resources between multiple tasks.
 * 
 * @author Fedor Smirnov
 *
 */
public interface ResourceArbiter {

  /**
   * Returns a list where the tasks from the given waiting list are arranged
   * according to their priority (the scheduler will TRY to execute the tasks at
   * the start of the list first)
   * 
   * @param candidates the list of candidate tasks (ordered following the order of
   *        arrival -- tasks waiting for a longer time are at closer to the start
   *        of the list)
   * @param res the resource the tasks are waiting for
   * @return the list prioritizing the tasks
   */
  List<Task> prioritizeTasks(List<Task> candidates, Resource res);
}
