package at.uibk.dps.sc.core.capacity;

import com.google.inject.ImplementedBy;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * The {@link CapacityCalculator} is used to determine the fraction of the
 * capacity that a given task occupies on a given resource.
 * 
 * @author Fedor Smirnov
 */
@ImplementedBy(CapacityCalculatorNone.class)
public interface CapacityCalculator {

  /**
   * Returns the fraction of the capacity (double between 0.0 and 1.0) that the
   * given task occupies on a given resource.
   * 
   * @param task the given task
   * @param resource the given resource
   * @return the fraction of the capacity (double between 0.0 and 1.0) that the
   *         given task occupies on a given resource
   */
  double getCapacityFraction(Task task, Resource resource);
}
