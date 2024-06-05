package at.uibk.dps.sc.core.capacity;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import at.uibk.dps.ee.model.properties.PropertyServiceFunction;
import at.uibk.dps.ee.model.properties.PropertyServiceResource;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

class CapacityCalculatorAbstractTest {

  protected class CapacityCalculatorMock extends CapacityCalculatorAbstract {
    @Override
    protected double getNonTrivialCapacityFraction(Task task, Resource res) {
      return 1.0;
    }
  }

  @Test
  void test() {
    Task negligible = new Task("negligible");
    Task notNegligible = new Task("notNegligible");
    PropertyServiceFunction.annotateNonNegligibleWorkload(notNegligible);
    Resource unlimited = new Resource("unlimited");
    Resource limited = new Resource("limited");
    PropertyServiceResource.annotateUnlimitedCapacity(unlimited);
    CapacityCalculatorMock tested = new CapacityCalculatorMock();
    assertEquals(0.0, tested.getCapacityFraction(negligible, unlimited), 0.0);
    assertEquals(0.0, tested.getCapacityFraction(notNegligible, unlimited), 0.0);
    assertEquals(0.0, tested.getCapacityFraction(negligible, limited), 0.0);
    assertEquals(1.0, tested.getCapacityFraction(notNegligible, limited), 0.0);
  }
}
