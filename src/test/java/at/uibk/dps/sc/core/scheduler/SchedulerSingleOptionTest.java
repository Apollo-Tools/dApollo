package at.uibk.dps.sc.core.scheduler;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import at.uibk.dps.ee.guice.starter.VertxProvider;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.graph.MappingsConcurrent;
import at.uibk.dps.ee.model.graph.ResourceGraph;
import at.uibk.dps.ee.model.graph.SpecificationProvider;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUser;
import at.uibk.dps.sc.core.capacity.CapacityCalculatorNone;
import io.vertx.core.Vertx;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

public class SchedulerSingleOptionTest {

  protected static VertxProvider vProv;

  @BeforeAll
  public static void setup() {
    Vertx vertx = Vertx.vertx();
    vProv = new VertxProvider(vertx);
  }

  @Test
  public void test() throws InterruptedException {
    Task task = PropertyServiceFunctionUser.createUserTask("bla", "addition");
    Resource res = new Resource("res");
    Mapping<Task, Resource> mapping = new Mapping<Task, Resource>("m", task, res);
    Set<Mapping<Task, Resource>> expected = new HashSet<>();
    expected.add(mapping);
    MappingsConcurrent mappings = new MappingsConcurrent();
    mappings.addMapping(mapping);
    EnactmentGraph eGraph = new EnactmentGraph();
    ResourceGraph rGraph = new ResourceGraph();
    EnactmentSpecification spec = new EnactmentSpecification(eGraph, rGraph, mappings, "");
    SpecificationProvider providerMock = mock(SpecificationProvider.class);
    when(providerMock.getMappings()).thenReturn(mappings);
    when(providerMock.getSpecification()).thenReturn(spec);
    SchedulerSingleOption tested =
        new SchedulerSingleOption(providerMock, new CapacityCalculatorNone(), vProv);
    Set<Mapping<Task, Resource>> result = new HashSet<>();
    CountDownLatch waitSched = new CountDownLatch(1);
    tested.scheduleTask(task).onComplete(asynRes -> {
      if (asynRes.succeeded()) {
        result.addAll(asynRes.result());
        waitSched.countDown();
      } else {
        fail("Async scheduling failed");
      }
    });
    waitSched.await();
    assertEquals(expected, result);
  }

  @Test
  public void testMoreThanOne() {
    assertThrows(IllegalArgumentException.class, () -> {
      Task task = PropertyServiceFunctionUser.createUserTask("bla", "addition");
      Resource res = new Resource("res");
      Resource res2 = new Resource("res2");
      Mapping<Task, Resource> mapping = new Mapping<Task, Resource>("m", task, res);
      Mapping<Task, Resource> mapping2 = new Mapping<Task, Resource>("m2", task, res2);
      MappingsConcurrent mappings = new MappingsConcurrent();
      mappings.addMapping(mapping);
      mappings.addMapping(mapping2);
      EnactmentGraph eGraph = new EnactmentGraph();
      ResourceGraph rGraph = new ResourceGraph();
      EnactmentSpecification spec = new EnactmentSpecification(eGraph, rGraph, mappings, "");
      SpecificationProvider providerMock = mock(SpecificationProvider.class);
      when(providerMock.getMappings()).thenReturn(mappings);
      when(providerMock.getSpecification()).thenReturn(spec);
      Set<Mapping<Task, Resource>> options = new HashSet<>();
      options.add(mapping);
      options.add(mapping2);
      SchedulerSingleOption tested =
          new SchedulerSingleOption(providerMock, new CapacityCalculatorNone(), vProv);
      tested.chooseMappingSubset(task, options);
    });
  }
}
