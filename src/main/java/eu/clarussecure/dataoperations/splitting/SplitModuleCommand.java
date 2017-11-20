package eu.clarussecure.dataoperations.splitting;

import eu.clarussecure.dataoperations.Criteria;
import eu.clarussecure.dataoperations.DataOperationCommand;

import java.util.Map;
import java.util.Random;

/**
 * Created by Alberto Blanco on 25/01/2017.
 */
public class SplitModuleCommand extends DataOperationCommand {

    private Map<String, SplitPoint> splitPoints = null;
    private Criteria[] originalCriteria;

    public SplitModuleCommand(String[] attributeNames, String[] protectedAttributeNames, Map<String, String> mapping,
            Map<String, SplitPoint> splitPoints, String[][] contents, String[][] protectedContents) {

        if (Util.isNullOrContainsNullString(attributeNames)) {
            throw new RuntimeException("Trying to create split module command with null value: attributeNames");
        }
        if (Util.isNullOrContainsNullString(protectedAttributeNames)) {
            throw new RuntimeException(
                    "Trying to create split module command with null value: protectedAttributeNames");
        }
        if (Util.isNullOrContainsNullString(mapping)) {
            throw new RuntimeException("Trying to create split module command with null value: mapping");
        }

        // This is ok to be null obviously
        //        if (Util.isNullOrContainsNullString(contents)) {
        //            throw new RuntimeException("Trying to create split module command with null value: contents");
        //        }
        //        if (Util.isNullOrContainsNullString(protectedContents)) {
        //            throw new RuntimeException("Trying to create split module command with null value: protectedContents");
        //        }
        super.id = new Random().nextInt();
        super.attributeNames = attributeNames;
        super.protectedAttributeNames = protectedAttributeNames;
        super.extraProtectedAttributeNames = null;
        super.extraBinaryContent = null;
        super.mapping = mapping;
        super.protectedContents = protectedContents;
        super.criteria = null;
        this.splitPoints = splitPoints;
    }

    public Map<String, SplitPoint> getSplitPoints() {
        return splitPoints;
    }

    @Override
    public void setCriteria(Criteria criteria[]) {
        super.criteria = criteria;
    }

    public void setOriginalCriteria(Criteria[] criteria) {
        this.originalCriteria = criteria;
    }

    public Criteria[] getOriginalCriteria() {
        return this.originalCriteria;
    }
}
