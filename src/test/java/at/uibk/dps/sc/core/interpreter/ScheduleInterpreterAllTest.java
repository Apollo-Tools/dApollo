package at.uibk.dps.sc.core.interpreter;

import static org.junit.jupiter.api.Assertions.*;
import at.uibk.dps.ee.core.function.EnactmentFunction;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUser;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUtilityCollections;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUtilityCollections.CollectionOperation;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class ScheduleInterpreterAllTest {

  @Test
  public void test() {
    ScheduleInterpreterUser mockInterpreterUser = mock(ScheduleInterpreterUser.class);
    Task userTask = PropertyServiceFunctionUser.createUserTask("usertask", "addition");
    Resource res = new Resource("res");
    Mapping<Task, Resource> mapping = new Mapping<Task, Resource>("mapping", userTask, res);
    Set<Mapping<Task, Resource>> userSchedule = new HashSet<>();
    userSchedule.add(mapping);
    EnactmentFunction userFunction = mock(EnactmentFunction.class);
    when(mockInterpreterUser.interpretSchedule(userTask, userSchedule)).thenReturn(userFunction);

    ScheduleInterpreterEE mockInterpreterEE = mock(ScheduleInterpreterEE.class);
    Task eeTask = PropertyServiceFunctionUtilityCollections.createCollectionOperation("bla",
        "blabla", CollectionOperation.Block);
    Set<Mapping<Task, Resource>> eeSchedule = new HashSet<>();
    EnactmentFunction eeFunction = mock(EnactmentFunction.class);
    when(mockInterpreterEE.interpretSchedule(eeTask, eeSchedule)).thenReturn(eeFunction);

    ScheduleInterpreterAll tested =
        new ScheduleInterpreterAll(mockInterpreterEE, mockInterpreterUser);
    assertEquals(eeFunction, tested.interpretSchedule(eeTask, eeSchedule));
    assertEquals(userFunction, tested.interpretSchedule(userTask, userSchedule));
  }
}
