package at.uibk.dps.sc.core.interpreter;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import at.uibk.dps.ee.core.function.EnactmentFunction;
import at.uibk.dps.ee.enactables.FactoryInputUser;
import at.uibk.dps.ee.enactables.FunctionFactoryUser;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.*;

public class ScheduleInterpreterUserTest {

  protected static class InterpreterMock extends ScheduleInterpreterUser {

    public InterpreterMock(Set<FunctionFactoryUser> userFactories) {
      super(userFactories);
    }

    @Override
    protected EnactmentFunction interpretScheduleUser(Task task,
        Set<Mapping<Task, Resource>> scheduleModel) {
      return null;
    }
  }

  FactoryInputUser factoryInput;
  FunctionFactoryUser factoryApplicable;
  FunctionFactoryUser factoryUnapplicable;
  Set<FunctionFactoryUser> userFactories;
  EnactmentFunction function;

  @BeforeEach
  void setup() {
    Task task = new Task("t");
    Resource res = new Resource("res");
    Mapping<Task, Resource> mapping = new Mapping<Task, Resource>("mapping", task, res);
    factoryInput = new FactoryInputUser(task, mapping);
    factoryApplicable = mock(FunctionFactoryUser.class);
    when(factoryApplicable.isApplicable(factoryInput)).thenReturn(true);
    factoryUnapplicable = mock(FunctionFactoryUser.class);
    when(factoryUnapplicable.isApplicable(factoryInput)).thenReturn(false);
    function = mock(EnactmentFunction.class);
    when(factoryApplicable.makeFunction(factoryInput)).thenReturn(function);
    userFactories = new HashSet<>();
    userFactories.add(factoryApplicable);
    userFactories.add(factoryUnapplicable);
  }

  @Test
  public void testGetScheduleForMapping() {
    InterpreterMock tested = new InterpreterMock(userFactories);
    EnactmentFunction result =
        tested.getFunctionForMapping(factoryInput.getTask(), factoryInput.getMapping());
    assertEquals(function, result);
  }

  @Test
  public void testGetSchedulerForMappingNoFactory() {
    assertThrows(IllegalStateException.class, () -> {
      userFactories.remove(factoryApplicable);
      InterpreterMock tested = new InterpreterMock(userFactories);
      tested.getFunctionForMapping(factoryInput.getTask(), factoryInput.getMapping());
    });
  }

  @Test
  public void testEmptyMapping() {
    assertThrows(IllegalArgumentException.class, () -> {
      Task task = new Task("task");
      Set<Mapping<Task, Resource>> schedule = new HashSet<>();
      InterpreterMock tested = new InterpreterMock(userFactories);
      tested.interpretSchedule(task, schedule);
    });
  }
}
