package at.uibk.dps.sc.core.scheduler.dApollo;

import java.util.HashMap;

public class SchedulerInput {

    /**
     * The cost limit for the schedule.
     */
    private double costLimit;

    /**
     * The location of the (starting) RS instance.
     */
    private String locationRS;

    /**
     * The resources of the RS instances.
     */
    private HashMap<String, HashMap<String, Double>> resourcesRSInstances;

    /**
     * Default constructor.
     */
    public SchedulerInput() {
    }

    /** Getter and Setter */

    public double getCostLimit() {
        return costLimit;
    }

    public void setCostLimit(double costLimit) {
        this.costLimit = costLimit;
    }

    public String getLocationRS() {
        return locationRS;
    }

    public void setLocationRS(String locationRS) {
        this.locationRS = locationRS;
    }

    public HashMap<String, HashMap<String, Double>> getResourcesRSInstances() {
        return resourcesRSInstances;
    }

    public void setResourcesRSInstances(HashMap<String, HashMap<String, Double>> resourcesRSInstances) {
        this.resourcesRSInstances = resourcesRSInstances;
    }
}
