package at.uibk.dps.sc.core.scheduler;

import static org.junit.jupiter.api.Assertions.*;
import com.google.gson.JsonObject;
import at.uibk.dps.ee.guice.starter.VertxProvider;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.graph.MappingsConcurrent;
import at.uibk.dps.ee.model.graph.SpecificationProvider;
import at.uibk.dps.ee.model.properties.PropertyServiceFunction;
import at.uibk.dps.ee.model.properties.PropertyServiceMapping;
import at.uibk.dps.ee.model.properties.PropertyServiceMapping.EnactmentMode;
import at.uibk.dps.sc.core.capacity.CapacityCalculatorNone;
import io.vertx.core.Vertx;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import static org.mockito.Mockito.mock;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.when;

public class SchedulerDataSizeTest {

  protected static VertxProvider vProv;

  @BeforeAll
  public static void setup() {
    Vertx vertx = Vertx.vertx();
    vProv = new VertxProvider(vertx);
  }

  @Test
  public void testUnderThresh() {
    SpecificationProvider provMock = mock(SpecificationProvider.class);
    EnactmentSpecification mockSpec = mock(EnactmentSpecification.class);
    MappingsConcurrent mappings = new MappingsConcurrent();
    when(mockSpec.getMappings()).thenReturn(mappings);
    when(provMock.getSpecification()).thenReturn(mockSpec);
    SchedulerDataSize tested =
        new SchedulerDataSize(provMock, new Random(), 1, 1, new CapacityCalculatorNone(), vProv);
    Task task = new Task("task");
    JsonObject empty = new JsonObject();
    PropertyServiceFunction.setInput(task, empty);
    Resource res = new Resource("res");
    Mapping<Task, Resource> serverlessMapping =
        PropertyServiceMapping.createMapping(task, res, EnactmentMode.Serverless, "serv");
    Mapping<Task, Resource> localMapping =
        PropertyServiceMapping.createMapping(task, res, EnactmentMode.Local, "loc");
    assertTrue(tested.excludeMapping(localMapping, task));
    assertFalse(tested.excludeMapping(serverlessMapping, task));

    Set<Mapping<Task, Resource>> maps = new HashSet<>();
    maps.add(serverlessMapping);
    maps.add(localMapping);

    Set<Mapping<Task, Resource>> result = tested.chooseMappingSubset(task, maps);
    assertEquals(1, result.size());
    assertEquals(serverlessMapping, result.iterator().next());
  }

  @Test
  public void testOverThresh() {
    SpecificationProvider provMock = mock(SpecificationProvider.class);
    EnactmentSpecification mockSpec = mock(EnactmentSpecification.class);
    MappingsConcurrent mappings = new MappingsConcurrent();
    when(mockSpec.getMappings()).thenReturn(mappings);
    when(provMock.getSpecification()).thenReturn(mockSpec);
    SchedulerDataSize tested =
        new SchedulerDataSize(provMock, new Random(), 1, 0, new CapacityCalculatorNone(), vProv);
    Task task = new Task("task");
    JsonObject empty = new JsonObject();
    PropertyServiceFunction.setInput(task, empty);
    Resource res = new Resource("res");
    Mapping<Task, Resource> serverlessMapping =
        PropertyServiceMapping.createMapping(task, res, EnactmentMode.Serverless, "serv");
    Mapping<Task, Resource> localMapping =
        PropertyServiceMapping.createMapping(task, res, EnactmentMode.Local, "loc");
    assertFalse(tested.excludeMapping(localMapping, task));
    assertTrue(tested.excludeMapping(serverlessMapping, task));
  }
}
