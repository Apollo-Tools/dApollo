package at.uibk.dps.sc.core.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.opt4j.core.common.random.Rand;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import at.uibk.dps.ee.guice.starter.VertxProvider;
import at.uibk.dps.ee.model.graph.SpecificationProvider;
import at.uibk.dps.ee.model.properties.PropertyServiceFunction;
import at.uibk.dps.ee.model.properties.PropertyServiceResource;
import at.uibk.dps.sc.core.capacity.CapacityCalculator;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * This scheduler places the tasks preferably on local resources (or more
 * precisely, the available resources with limited capacity); Remote resources
 * are used only after exceeding the capacity of available local resources.
 * 
 * @author Fedor Smirnov
 */
@Singleton
public class SchedulerLocalRes extends SchedulerAbstract {

  protected final Rand rand;

  /**
   * Direct usage of the parent constructor + injection of a rand
   */
  @Inject
  public SchedulerLocalRes(final SpecificationProvider specProvider,
      final CapacityCalculator capacityCalculator, final VertxProvider vertProv, final Rand rand) {
    super(specProvider, capacityCalculator, vertProv);
    this.rand = rand;
  }

  @Override
  protected Set<Mapping<Task, Resource>> chooseMappingSubset(final Task task,
      final Set<Mapping<Task, Resource>> mappingOptions) {
    final List<Mapping<Task, Resource>> actualOptions = new ArrayList<>();
    if (mappingOptions.stream().anyMatch(this::isCapacityMapping)) {
      // we have capacity mappings -> use one of those
      actualOptions.addAll(
          mappingOptions.stream().filter(this::isCapacityMapping).collect(Collectors.toSet()));
    } else {
      actualOptions.addAll(mappingOptions);
    }
    final int idx = rand.nextInt(actualOptions.size());
    final Set<Mapping<Task, Resource>> result = new HashSet<>();
    result.add(actualOptions.get(idx));
    return result;
  }

  /**
   * Returns true iff the given mapping is affected by resource capacity.
   * 
   * @param mapping the given mapping
   * @return true iff the given mapping is affected by resource capacity
   */
  protected boolean isCapacityMapping(final Mapping<Task, Resource> mapping) {
    final Task task = mapping.getSource();
    final Resource res = mapping.getTarget();
    return !PropertyServiceFunction.hasNegligibleWorkload(task)
        && PropertyServiceResource.hasLimitedCapacity(res);
  }
}
