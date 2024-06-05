package at.uibk.dps.sc.core;

import static org.junit.jupiter.api.Assertions.*;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

public class ScheduleModelTest {

  @Test
  public void testBasic() {
    ScheduleModel tested = new ScheduleModel();
    Task task = new Task("task");
    Resource res1 = new Resource("res1");
    Resource res2 = new Resource("res2");
    Mapping<Task, Resource> mapping1 = new Mapping<Task, Resource>("m1", task, res1);
    Set<Mapping<Task, Resource>> schedule = new HashSet<>();
    Mapping<Task, Resource> mapping2 = new Mapping<Task, Resource>("m2", task, res2);
    schedule.add(mapping1);
    schedule.add(mapping2);
    assertFalse(tested.isScheduled(task));
    tested.setTaskSchedule(task, schedule);
    assertTrue(tested.isScheduled(task));
    assertEquals(schedule, tested.getTaskSchedule(task));
    tested.resetTaskSchedule(task);
    assertFalse(tested.isScheduled(task));
  }

  @Test
  public void testeUnscheduled() {
    assertThrows(IllegalArgumentException.class, () -> {
      Task task = new Task("task");
      ScheduleModel tested = new ScheduleModel();
      tested.getTaskSchedule(task);
    });
  }
}
