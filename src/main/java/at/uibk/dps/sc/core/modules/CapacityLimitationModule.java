package at.uibk.dps.sc.core.modules;

import org.opt4j.core.config.annotations.Info;
import org.opt4j.core.config.annotations.Order;
import org.opt4j.core.start.Constant;
import at.uibk.dps.ee.guice.modules.EeModule;
import at.uibk.dps.sc.core.capacity.CapacityCalculator;
import at.uibk.dps.sc.core.capacity.CapacityCalculatorTaskNumber;

/**
 * Simple module to define the maximum number of tasks which is to be placed
 * onto capacity-restricted resources.
 * 
 * @author Fedor Smirnov
 */
public class CapacityLimitationModule extends EeModule {

  @Order(1)
  @Info("The maximal number of tasks which can be placed on resources with limited capacity.")
  @Constant(value = "maxTaskNumber", namespace = CapacityCalculatorTaskNumber.class)
  public int maximalTaskNumber = 4;

  @Override
  protected void config() {
    bind(CapacityCalculator.class).to(CapacityCalculatorTaskNumber.class);
  }

  public int getMaximalTaskNumber() {
    return maximalTaskNumber;
  }

  public void setMaximalTaskNumber(final int maximalTaskNumber) {
    this.maximalTaskNumber = maximalTaskNumber;
  }
}
