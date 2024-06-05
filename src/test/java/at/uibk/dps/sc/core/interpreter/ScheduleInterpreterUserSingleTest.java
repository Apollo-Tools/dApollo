package at.uibk.dps.sc.core.interpreter;

import static org.junit.jupiter.api.Assertions.*;
import at.uibk.dps.ee.enactables.FactoryInputUser;
import at.uibk.dps.ee.enactables.FunctionFactoryUser;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class ScheduleInterpreterUserSingleTest {


  @Test
  public void testCheckSchedule() {
    assertThrows(IllegalArgumentException.class, () -> {
      FunctionFactoryUser factoryMock = mock(FunctionFactoryUser.class);
      Task task = new Task("task");
      Resource res1 = new Resource("res1");
      Resource res2 = new Resource("res2");
      Mapping<Task, Resource> m1 = new Mapping<>("m1", task, res1);
      Mapping<Task, Resource> m2 = new Mapping<>("m2", task, res2);
      Set<Mapping<Task, Resource>> scheduleModel = new HashSet<>();
      scheduleModel.add(m1);
      scheduleModel.add(m2);
      when(factoryMock.isApplicable(new FactoryInputUser(task, m1))).thenReturn(true);
      when(factoryMock.isApplicable(new FactoryInputUser(task, m2))).thenReturn(true);
      Set<FunctionFactoryUser> factories = new HashSet<>();
      factories.add(factoryMock);
      ScheduleInterpreterUserSingle tested = new ScheduleInterpreterUserSingle(factories);
      tested.checkSchedule(task, scheduleModel);
    });
  }
}
