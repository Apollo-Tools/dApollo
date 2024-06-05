package at.uibk.dps.sc.core.capacity;

import at.uibk.dps.ee.model.properties.PropertyServiceFunction;
import at.uibk.dps.ee.model.properties.PropertyServiceResource;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * Parent of all {@link CapacityCalculator}s
 * 
 * @author Fedor Smirnov
 *
 */
public abstract class CapacityCalculatorAbstract implements CapacityCalculator {

  @Override
  public final double getCapacityFraction(final Task task, final Resource resource) {
    if (!PropertyServiceResource.hasLimitedCapacity(resource)
        || PropertyServiceFunction.hasNegligibleWorkload(task)) {
      return 0;
    } else {
      return getNonTrivialCapacityFraction(task, resource);
    }
  }

  /**
   * Methods which should be defined in subclasses to specify how to deal with
   * cases of limited capacity.
   * 
   * @param task the task with non-negligible workload
   * @param res the resource with limited capacity
   * @return the capacity fraction that the given task requires on the given
   *         resource
   */
  protected abstract double getNonTrivialCapacityFraction(Task task, Resource res);

}
