package at.uibk.dps.sc.core.interpreter;

import static org.junit.jupiter.api.Assertions.*;
import at.uibk.dps.ee.core.function.EnactmentFunction;
import at.uibk.dps.ee.enactables.local.dataflow.FunctionFactoryDataFlow;
import at.uibk.dps.ee.enactables.local.utility.FunctionFactoryUtility;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionDataFlow;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionDataFlow.DataFlowType;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUtilityCollections.CollectionOperation;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUtilityCollections;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import static org.mockito.Mockito.when;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;

public class ScheduleInterpreterEETest {

  @Test
  public void testInterpretSchedule() {

    FunctionFactoryUtility utilityMock = mock(FunctionFactoryUtility.class);
    FunctionFactoryDataFlow dataFlowMock = mock(FunctionFactoryDataFlow.class);
    Task dfTask = PropertyServiceFunctionDataFlow.createDataFlowFunction("taskdf",
        DataFlowType.EarliestInput);
    Task utilTask = PropertyServiceFunctionUtilityCollections.createCollectionOperation("bla",
        "blabla", CollectionOperation.Block);
    EnactmentFunction dataFunc = mock(EnactmentFunction.class);
    EnactmentFunction utilFunc = mock(EnactmentFunction.class);
    when(utilityMock.makeFunction(utilTask)).thenReturn(utilFunc);
    when(dataFlowMock.makeFunction(dfTask)).thenReturn(dataFunc);

    ScheduleInterpreterEE tested = new ScheduleInterpreterEE(dataFlowMock, utilityMock);

    assertEquals(dataFunc, tested.interpretSchedule(dfTask, new HashSet<>()));
    assertEquals(utilFunc, tested.interpretSchedule(utilTask, new HashSet<>()));

    Mapping<Task, Resource> mapping = new Mapping<>("mapping", utilTask, new Resource("res"));
    Set<Mapping<Task, Resource>> mappings = new HashSet<>();
    mappings.add(mapping);
    try {
      tested.interpretSchedule(utilTask, mappings);
      fail();
    } catch (IllegalArgumentException exc) {
    }
  }
}
