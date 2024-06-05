package at.uibk.dps.sc.core.scheduler;

import java.util.HashSet;
import java.util.Set;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import at.uibk.dps.ee.guice.starter.VertxProvider;
import at.uibk.dps.ee.model.graph.SpecificationProvider;
import at.uibk.dps.sc.core.capacity.CapacityCalculator;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * The {@link SchedulerSingleOption} is used in cases where exactly one mapping
 * is provided for each task (enactments which are to run with the specified
 * binding, without any scheduling decisions).
 * 
 * @author Fedor Smirnov
 */
@Singleton
public class SchedulerSingleOption extends SchedulerAbstract {

  /**
   * Injection constructor; Same as parent.
   */
  @Inject
  public SchedulerSingleOption(final SpecificationProvider specProvider,
      final CapacityCalculator capCalc, final VertxProvider vProv) {
    super(specProvider, capCalc, vProv);
  }

  @Override
  protected Set<Mapping<Task, Resource>> chooseMappingSubset(final Task task,
      final Set<Mapping<Task, Resource>> mappingOptions) {
    if (mappingOptions.size() != 1) {
      throw new IllegalArgumentException(
          "Not exactly one mapping provided for task " + task.getId());
    }
    return new HashSet<>(mappingOptions);
  }
}
