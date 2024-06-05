package at.uibk.dps.sc.core.scheduler;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opt4j.core.common.random.Rand;
import at.uibk.dps.ee.guice.starter.VertxProvider;
import at.uibk.dps.ee.model.graph.SpecificationProvider;
import at.uibk.dps.ee.model.properties.PropertyServiceFunction;
import at.uibk.dps.ee.model.properties.PropertyServiceResource;
import at.uibk.dps.sc.core.capacity.CapacityCalculator;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.HashSet;
import java.util.Set;

class SchedulerLocalResTest {

  protected static SchedulerLocalRes tested;

  protected static Task negligible;
  protected static Task nonNegligible;
  protected static Resource limited;
  protected static Resource unlimited;

  @BeforeEach
  void setup() {
    SpecificationProvider sProvMock = mock(SpecificationProvider.class);
    CapacityCalculator capCalMock = mock(CapacityCalculator.class);
    VertxProvider vProvMock = mock(VertxProvider.class);
    Rand randMock = mock(Rand.class);
    when(randMock.nextInt(1)).thenReturn(0);
    when(randMock.nextInt(2)).thenReturn(0);

    tested = new SchedulerLocalRes(sProvMock, capCalMock, vProvMock, randMock);

    negligible = new Task("task");
    nonNegligible = new Task("task2");
    PropertyServiceFunction.annotateNonNegligibleWorkload(nonNegligible);
    limited = new Resource("res");
    unlimited = new Resource("res2");
    PropertyServiceResource.annotateUnlimitedCapacity(unlimited);
  }

  @Test
  void testChooseMappingSubset() {
    Mapping<Task, Resource> capMapping = new Mapping<Task, Resource>("cap", nonNegligible, limited);
    Mapping<Task, Resource> nonCapMapping =
        new Mapping<Task, Resource>("noCap", nonNegligible, unlimited);
    Set<Mapping<Task, Resource>> cap = new HashSet<>();
    Set<Mapping<Task, Resource>> noCap = new HashSet<>();
    cap.add(capMapping);
    cap.add(nonCapMapping);
    noCap.add(nonCapMapping);

    assertEquals(capMapping, tested.chooseMappingSubset(nonNegligible, cap).iterator().next());
    assertEquals(nonCapMapping, tested.chooseMappingSubset(nonNegligible, noCap).iterator().next());
  }

  @Test
  void testIsCapacityMapping() {
    assertFalse(
        tested.isCapacityMapping(new Mapping<Task, Resource>("bla", negligible, unlimited)));
    assertFalse(
        tested.isCapacityMapping(new Mapping<Task, Resource>("bla", nonNegligible, unlimited)));
    assertFalse(tested.isCapacityMapping(new Mapping<Task, Resource>("bla", negligible, limited)));
    assertTrue(
        tested.isCapacityMapping(new Mapping<Task, Resource>("bla", nonNegligible, limited)));
  }
}
