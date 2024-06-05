package at.uibk.dps.sc.core.capacity;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import net.sf.opendse.model.Task;

class CapacityLimitExceptionTest {

  @Test
  void test() {
    Task task = new Task("task");
    CapacityLimitException tested = new CapacityLimitException(task);
    assertEquals(task, tested.getUnscheduledTask());
  }
}
