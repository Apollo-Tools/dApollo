package at.uibk.dps.sc.core.arbitration;

import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

class ResourceArbiterFCFSTest {

  @Test
  void test() {
    Task t1 = new Task("t1");
    Task t2 = new Task("t2");
    Task t3 = new Task("t3");
    Resource res = new Resource("res");

    List<Task> input = new ArrayList<>();
    input.add(t1);
    input.add(t2);
    input.add(t3);

    ResourceArbiterFCFS tested = new ResourceArbiterFCFS();
    List<Task> result = tested.prioritizeTasks(input, res);
    assertEquals(input.get(0), result.get(0));
    assertEquals(input.get(1), result.get(1));
    assertEquals(input.get(2), result.get(2));
  }
}
