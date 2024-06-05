package at.uibk.dps.sc.core.capacity;

import com.google.inject.Singleton;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * The {@link CapacityCalculatorNone} is the implementation of the
 * {@link CapacityCalculator} which is used in the cases where resource capacity
 * is ignored (default case).
 * 
 * @author Fedor Smirnov
 */
@Singleton
public class CapacityCalculatorNone extends CapacityCalculatorAbstract {

  @Override
  protected double getNonTrivialCapacityFraction(final Task task, final Resource res) {
    return 0;
  }
}
