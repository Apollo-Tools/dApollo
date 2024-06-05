package at.uibk.dps.sc.core.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.opt4j.core.start.Constant;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import at.uibk.dps.ee.guice.starter.VertxProvider;
import at.uibk.dps.ee.model.graph.SpecificationProvider;
import at.uibk.dps.sc.core.capacity.CapacityCalculator;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * The {@link SchedulerRandom} randomly picks a (non-unique) subcollection of
 * given size from the mappings available for a user task.
 * 
 * @author Fedor Smirnov
 */
@Singleton
public class SchedulerRandom extends SchedulerAbstract {

  protected final Random random;
  protected final int mappingsToPick;

  /**
   * The injection constructor.
   * 
   * @param specProvider the specification provider
   * @param random the random number generator
   * @param mappingsToPick the number of mappings to pick
   */
  @Inject
  public SchedulerRandom(final SpecificationProvider specProvider, final Random random,
      @Constant(namespace = SchedulerRandom.class,
          value = "mappingsToPick") final int mappingsToPick,
      final CapacityCalculator capCalc, final VertxProvider vProv) {
    super(specProvider, capCalc, vProv);
    this.random = random;
    this.mappingsToPick = mappingsToPick;
  }

  @Override
  protected Set<Mapping<Task, Resource>> chooseMappingSubset(final Task task,
      final Set<Mapping<Task, Resource>> mappingOptions) {
    final List<Mapping<Task, Resource>> mappingList = new ArrayList<>(mappingOptions);
    return IntStream.generate(() -> random.nextInt(mappingOptions.size())).limit(mappingsToPick)
        .boxed().map(index -> mappingList.get(index)).collect(Collectors.toSet());
  }
}
