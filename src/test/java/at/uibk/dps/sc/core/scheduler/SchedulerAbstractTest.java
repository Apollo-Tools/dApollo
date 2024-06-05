package at.uibk.dps.sc.core.scheduler;

import static org.junit.jupiter.api.Assertions.*;
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
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUtilityCollections;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUtilityCollections.CollectionOperation;
import at.uibk.dps.ee.model.properties.PropertyServiceResource;
import at.uibk.dps.sc.core.capacity.CapacityCalculator;
import at.uibk.dps.sc.core.capacity.CapacityCalculatorNone;
import io.vertx.core.Vertx;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;;

public class SchedulerAbstractTest {

  protected static class SchedulerMock extends SchedulerAbstract {

    public SchedulerMock(SpecificationProvider specProvider, VertxProvider vProv) {
      super(specProvider, new CapacityCalculatorNone(), vProv);
    }

    @Override
    protected Set<Mapping<Task, Resource>> chooseMappingSubset(Task task,
        Set<Mapping<Task, Resource>> mappingOptions) {
      Set<Mapping<Task, Resource>> result = new HashSet<>();
      result.add(mappingOptions.iterator().next());
      return result;
    }
  }

  protected static class SchedulerCapMock extends SchedulerAbstract {

    public SchedulerCapMock(SpecificationProvider specProvider, CapacityCalculator capCal,
        VertxProvider vProv) {
      super(specProvider, capCal, vProv);
    }

    @Override
    protected Set<Mapping<Task, Resource>> chooseMappingSubset(Task task,
        Set<Mapping<Task, Resource>> mappingOptions) {
      // not used
      return null;
    }
  }

  protected static VertxProvider vProv;

  @BeforeAll
  public static void setup() {
    Vertx vertx = Vertx.vertx();
    vProv = new VertxProvider(vertx);
  }

  @Test
  public void testIsMappingValid() {
    Task task50 = new Task("task");
    Task task30 = new Task("task2");
    Resource targetRes = new Resource("res");
    CapacityCalculator capCalc = mock(CapacityCalculator.class);
    when(capCalc.getCapacityFraction(task30, targetRes)).thenReturn(.3);
    when(capCalc.getCapacityFraction(task50, targetRes)).thenReturn(.5);
    Task scheduledTask = new Task("t");
    when(capCalc.getCapacityFraction(scheduledTask, targetRes)).thenReturn(.25);
    SpecificationProvider mockProv = mock(SpecificationProvider.class);
    EnactmentGraph graph = new EnactmentGraph();
    graph.addVertex(scheduledTask);
    graph.addVertex(task30);
    graph.addVertex(task50);
    EnactmentSpecification mockSpec = mock(EnactmentSpecification.class);
    when(mockSpec.getEnactmentGraph()).thenReturn(graph);
    when(mockProv.getSpecification()).thenReturn(mockSpec);
    SchedulerCapMock tested = new SchedulerCapMock(mockProv, capCalc, vProv);
    Mapping<Task, Resource> mapping = new Mapping<>("map", scheduledTask, targetRes);
    PropertyServiceResource.addUsingTask(task50, targetRes);
    assertTrue(tested.isValidMapping(mapping));
    PropertyServiceResource.addUsingTask(task30, targetRes);
    assertFalse(tested.isValidMapping(mapping));
  }

  @Test
  public void testUser() throws InterruptedException {
    Task task = PropertyServiceFunctionUser.createUserTask("task", "addition");
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
    when(providerMock.getSpecification()).thenReturn(spec);

    SchedulerMock tested = new SchedulerMock(providerMock, vProv);
    SchedulerMock testedSpy = spy(tested);

    CountDownLatch waitForScheduling = new CountDownLatch(1);
    Set<Mapping<Task, Resource>> result = new HashSet<>();

    testedSpy.scheduleTask(task).onComplete(asynRes -> {
      if (asynRes.succeeded()) {
        result.addAll(asynRes.result());
        waitForScheduling.countDown();
      } else {
        fail("Async scheduling failed.");
      }
    });
    waitForScheduling.await();
    assertTrue(result.size() == 1);
    verify(testedSpy).chooseMappingSubset(task, expected);
  }

  @Test
  public void testUtility() throws InterruptedException {
    Task task = PropertyServiceFunctionUtilityCollections.createCollectionOperation("task", "bla",
        CollectionOperation.Block);
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

    SchedulerMock tested = new SchedulerMock(providerMock, vProv);
    SchedulerMock testedSpy = spy(tested);
    Set<Mapping<Task, Resource>> result = new HashSet<>();
    CountDownLatch schedWait = new CountDownLatch(1);

    testedSpy.scheduleTask(task).onComplete(asyncRes -> {
      if (asyncRes.succeeded()) {
        result.addAll(asyncRes.result());
        schedWait.countDown();
      } else {
        fail("Async scheduling failed.");
      }
    });
    schedWait.await();
    assertTrue(result.isEmpty());
    verify(testedSpy, never()).chooseMappingSubset(task, expected);
  }
}
