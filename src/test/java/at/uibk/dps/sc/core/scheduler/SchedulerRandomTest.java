package at.uibk.dps.sc.core.scheduler;

import static org.junit.jupiter.api.Assertions.*;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import at.uibk.dps.ee.guice.starter.VertxProvider;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.graph.MappingsConcurrent;
import at.uibk.dps.ee.model.graph.ResourceGraph;
import at.uibk.dps.ee.model.graph.SpecificationProvider;
import at.uibk.dps.sc.core.capacity.CapacityCalculatorNone;
import io.vertx.core.Vertx;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchedulerRandomTest {

  protected static VertxProvider vProv;

  @BeforeAll
  public static void setup() {
    Vertx vertx = Vertx.vertx();
    vProv = new VertxProvider(vertx);
  }

  @Test
  public void test() {
    SpecificationProvider mockSpec = mock(SpecificationProvider.class);
    EnactmentSpecification enactSpec = new EnactmentSpecification(new EnactmentGraph(),
        new ResourceGraph(), new MappingsConcurrent(), "");
    when(mockSpec.getSpecification()).thenReturn(enactSpec);
    SchedulerRandom tested =
        new SchedulerRandom(mockSpec, new Random(), 1, new CapacityCalculatorNone(), vProv);
    Task task = new Task("task");
    Resource res = new Resource("res");
    Mapping<Task, Resource> mapping = new Mapping<Task, Resource>("mapping", task, res);
    Set<Mapping<Task, Resource>> subset = new HashSet<>();
    subset.add(mapping);
    assertEquals(mapping, tested.chooseMappingSubset(task, subset).iterator().next());
  }
}
