package at.uibk.dps.sc.core.interpreter;

import java.util.Set;
import com.google.inject.Inject;
import at.uibk.dps.ee.core.function.EnactmentFunction;
import at.uibk.dps.ee.enactables.local.dataflow.FunctionFactoryDataFlow;
import at.uibk.dps.ee.enactables.local.utility.FunctionFactoryUtility;
import at.uibk.dps.ee.model.properties.PropertyServiceFunction;
import at.uibk.dps.ee.model.properties.PropertyServiceFunction.UsageType;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

/**
 * The {@link ScheduleInterpreterEE} provides the schedules for the tasks
 * representing the internal operations of the EE.
 * 
 * @author Fedor Smirnov
 *
 */
public class ScheduleInterpreterEE implements ScheduleInterpreter {

  protected final FunctionFactoryDataFlow factoryDataFlow;
  protected final FunctionFactoryUtility factoryUtility;

  /**
   * Injection constructor.
   * 
   * @param factoryDataFlow factory for the data flow functions
   * @param factoryUtility factory for the utility functions
   */
  @Inject
  public ScheduleInterpreterEE(final FunctionFactoryDataFlow factoryDataFlow,
      final FunctionFactoryUtility factoryUtility) {
    this.factoryDataFlow = factoryDataFlow;
    this.factoryUtility = factoryUtility;
  }

  @Override
  public EnactmentFunction interpretSchedule(final Task task,
      final Set<Mapping<Task, Resource>> scheduleModel) {
    if (!scheduleModel.isEmpty()) {
      throw new IllegalArgumentException("EE tasks should not be annotated with mappings.");
    }
    final UsageType usage = PropertyServiceFunction.getUsageType(task);
    if (usage.equals(UsageType.DataFlow)) {
      return factoryDataFlow.makeFunction(task);
    } else if (usage.equals(UsageType.Utility)) {
      return factoryUtility.makeFunction(task);
    } else {
      throw new IllegalArgumentException("Neither a utility nor a data flow task: " + task.getId());
    }
  }
}
