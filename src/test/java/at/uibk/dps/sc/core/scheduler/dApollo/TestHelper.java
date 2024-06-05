package at.uibk.dps.sc.core.scheduler.dApollo;

public class TestHelper {

    double costLimit;
    double bandwidth;

    double expectedCost;
    double expectedRuntime;

    public TestHelper(double costLimit, double bandwidth, double expectedCost, double expectedRuntime) {
        this.costLimit = costLimit;
        this.bandwidth = bandwidth;
        this.expectedCost = expectedCost;
        this.expectedRuntime = expectedRuntime;
    }

    public double getCostLimit() {
        return costLimit;
    }

    public void setCostLimit(double costLimit) {
        this.costLimit = costLimit;
    }

    public double getBandwidth() {
        return bandwidth;
    }

    public void setBandwidth(double bandwidth) {
        this.bandwidth = bandwidth;
    }

    public double getExpectedCost() {
        return expectedCost;
    }

    public void setExpectedCost(double expectedCost) {
        this.expectedCost = expectedCost;
    }

    public double getExpectedRuntime() {
        return expectedRuntime;
    }

    public void setExpectedRuntime(double expectedRuntime) {
        this.expectedRuntime = expectedRuntime;
    }
}
