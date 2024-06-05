package at.uibk.dps.sc.core.capacity;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

class CapacityCalculatorNoneTest {

  @Test
  void test() {
    Task task = new Task("task");
    Resource res = new Resource("res");
    CapacityCalculatorNone tested = new CapacityCalculatorNone();
    assertEquals(0.0, tested.getNonTrivialCapacityFraction(task, res), 0.0);
  }
}
