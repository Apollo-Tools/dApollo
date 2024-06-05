package at.uibk.dps.sc.core.scheduler.dApollo;

import net.sf.opendse.model.Task;

import java.util.ArrayList;
import java.util.List;

public class Proposal {

    /**
     * List of mappings in the proposal.
     */
    private List<Mapping_> mappings;

    /**
     * Runtime savings if the proposal is applied.
     */
    private double ts;

    /**
     * Additional cost if the proposal is applied.
     */
    private double ac;

    /**
     * Runtime savings of only the proposal (excluding proposals running in parallel).
     */
    private double tsPlain;

    /**
     * Additional cost of only the proposal (excluding proposals running in parallel).
     */
    private double acPlain;

    /**
     * The trade-off between ts and ac.
     */
    private double tradeoff;

    /**
     * List of proposals that are included to achieve the desired ts.
     */
    private List<Proposal> includes;

    /**
     * List of proposals that are included to achieve the desired ts.
     */
    private List<Proposal> includesAll;

    /**
     * List of Tasks that are included in the proposal.
     */
    private List<Task> taskIncludes;

    /**
     * Default constructor.
     *
     * @param mappings list of mappings in the proposal.
     * @param ts runtime savings if the proposal is applied.
     * @param ac additional cost if the proposal is applied.
     */
    public Proposal(List<Mapping_> mappings, double ts, double ac) {
        this.mappings = mappings;
        this.ts = ts;
        this.ac = ac;
        this.tsPlain = ts;
        this.acPlain = ac;
        this.includes = new ArrayList<>();
        this.includesAll = new ArrayList<>();
        this.taskIncludes = new ArrayList<>();
        this.calculateTradeOff();
    }

    public void calculateTradeOff() {
        if(this.ts <= 0.0) {
            this.tradeoff = Double.MIN_VALUE;
        } else if (this.ac <= 0.0) {
            this.tradeoff = Double.MAX_VALUE;
        } else {
            this.tradeoff = this.ts / this.ac;
        }
    }

    /** Getter and Setter */

    public List<Mapping_> getMappings() {
        return mappings;
    }

    public void setMappings(List<Mapping_> mappings) {
        this.mappings = mappings;
    }

    public double getTs() {
        return ts;
    }

    public void setTs(double ts) {
        this.ts = ts;
    }

    public double getAc() {
        return ac;
    }

    public void setAc(double ac) {
        this.ac = ac;
    }

    public double getTsPlain() {
        return tsPlain;
    }

    public void setTsPlain(double tsPlain) {
        this.tsPlain = tsPlain;
    }

    public double getAcPlain() {
        return acPlain;
    }

    public void setAcPlain(double acPlain) {
        this.acPlain = acPlain;
    }

    public List<Proposal> getIncludes() {
        return includes;
    }

    public void setIncludes(List<Proposal> includes) {
        this.includes = includes;
    }

    public double getTradeoff() {
        return tradeoff;
    }

    public void setTradeoff(double tradeoff) {
        this.tradeoff = tradeoff;
    }

    public List<Proposal> getIncludesAll() {
        return includesAll;
    }

    public void setIncludesAll(List<Proposal> includesAll) {
        this.includesAll = includesAll;
    }

    public List<Task> getTaskIncludes() {
        return taskIncludes;
    }

    public void setTaskIncludes(List<Task> taskIncludes) {
        this.taskIncludes = taskIncludes;
    }
}
