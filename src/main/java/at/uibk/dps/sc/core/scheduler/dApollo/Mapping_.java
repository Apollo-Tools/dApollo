package at.uibk.dps.sc.core.scheduler.dApollo;

import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

public class Mapping_ {

    /**
     * Task of the mapping.
     */
    private Task task;

    /**
     * Resource of the task.
     */
    private Resource resource;

    /**
     * Resource of the RS instance.
     */
    private String RSInstanceResource;

    /**
     * If the mapping is finalized.
     */
    private boolean finalized;

    /**
     * If the mapping is set by another proposal.
     */
    private boolean setByOtherProposal;

    /**
     * Default constructor.
     *
     * @param task of the mapping.
     * @param resource  of the task.
     * @param RSInstanceResource of the RS instance.
     */
    public Mapping_(Task task, Resource resource, String RSInstanceResource) {
        this.task = task;
        this.resource = resource;
        this.RSInstanceResource = RSInstanceResource;
        this.finalized = false;
        this.setByOtherProposal = false;
    }

    /** Getter and Setter */

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public String getRSInstanceResource() {
        return RSInstanceResource;
    }

    public void setRSInstanceResource(String RSInstanceResource) {
        this.RSInstanceResource = RSInstanceResource;
    }

    public boolean isFinalized() {
        return finalized;
    }

    public void setFinalized(boolean finalized) {
        this.finalized = finalized;
    }

    public boolean isSetByOtherProposal() {
        return setByOtherProposal;
    }

    public void setSetByOtherProposal(boolean setByOtherProposal) {
        this.setByOtherProposal = setByOtherProposal;
    }
}
