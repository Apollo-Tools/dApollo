package at.uibk.dps.sc.core.scheduler.dApollo;

public class Statistics {

    /**
     * The overall workflow cost.
     */
    private double cost;

    /**
     * The overall workflow runtime.
     */
    private double runtime;

    /**
     * Default constructor.
     */
    public Statistics() { }

    /** Getter and Setter */

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public double getRuntime() {
        return runtime;
    }

    public void setRuntime(double runtime) {
        this.runtime = runtime;
    }
}
