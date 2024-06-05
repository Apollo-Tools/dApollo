package at.uibk.dps.sc.core.capacity;

import net.sf.opendse.model.Task;

/**
 * The {@link CapacityLimitException} is used to signal (to the scheduling
 * verticle) that a task could (at the moment) not be scheduled due to the
 * limited capacity of resources.
 * 
 * @author Fedor Smirnov
 *
 */
public class CapacityLimitException extends Exception {

  private static final long serialVersionUID = 1L;

  protected final Task unscheduledTask;

  /**
   * Constructor
   * 
   * @param unscheduledTask the task which could not be scheduled
   */
  public CapacityLimitException(final Task unscheduledTask) {
    this.unscheduledTask = unscheduledTask;
  }

  public Task getUnscheduledTask() {
    return unscheduledTask;
  }
}
