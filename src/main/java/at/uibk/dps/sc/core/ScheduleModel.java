package at.uibk.dps.sc.core;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.google.inject.Singleton;
import at.uibk.dps.ee.model.properties.PropertyServiceResource;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * Interface for the class used to maintain the information that the EE has on
 * the scheduling of its tasks.
 * 
 * @author Fedor Smirnov
 */
@Singleton
public class ScheduleModel {

  protected final ConcurrentHashMap<Task, Set<Mapping<Task, Resource>>> scheduleMap =
      new ConcurrentHashMap<>();

  /**
   * Returns true if the task is already scheduled.
   * 
   * @param functionTask the requested task
   * @return true if the task is already scheduled
   */
  public boolean isScheduled(final Task functionTask) {
    return scheduleMap.containsKey(functionTask);
  }

  /**
   * Resets the entire schedule.
   */
  public void resetSchedule() {
    scheduleMap.clear();
  }

  /**
   * Removes the given task from the schedule.
   * 
   * @param task the given task
   */
  public void resetTaskSchedule(final Task task) {
    scheduleMap.remove(task);
  }

  /**
   * Sets the schedule for the given task.
   * 
   * @param task the given task
   * @param schedule set of mappings set as a schedule
   */
  public void setTaskSchedule(final Task task, final Set<Mapping<Task, Resource>> schedule) {
    scheduleMap.put(task, schedule);
    schedule.forEach(m -> PropertyServiceResource.addUsingTask(task, m.getTarget()));
  }

  /**
   * Returns the schedule annotated for the requested task.
   * 
   * @param task the requested task
   * @return the task schedule, in the form of (annotated) mapping edges
   */
  public Set<Mapping<Task, Resource>> getTaskSchedule(final Task task) {
    if (!isScheduled(task)) {
      throw new IllegalArgumentException(
          "Request for the schedule of unscheduled task " + task.getId());
    }
    return scheduleMap.get(task);
  }
}
