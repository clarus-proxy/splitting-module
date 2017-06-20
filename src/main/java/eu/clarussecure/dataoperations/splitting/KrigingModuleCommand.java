package eu.clarussecure.dataoperations.splitting;

import eu.clarussecure.dataoperations.DataOperationCommand;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Random;

public class KrigingModuleCommand extends DataOperationCommand {

    private Map<String, SplitPoint> splitPoints = null;
    private String measure;
    private String geomAttribute;
    private String point;
    private int step;
    private String[] calculatedOnCloud;
    private String[] measureContents;
    private String[] xCoordinate;
    private String[] yCoordinate;

    public KrigingModuleCommand(String[] attributeNames, String[] protectedAttributeNames, Map<String, String> mapping,
            Map<String, SplitPoint> splitPoints, String measure, String geomAttribute, String point) {
        super.id = new Random().nextInt();
        super.attributeNames = attributeNames;
        super.protectedAttributeNames = protectedAttributeNames;
        this.splitPoints = splitPoints;

        // Declare custom call to the cloud
        super.extraBinaryContent = null;
        super.mapping = mapping;
        this.step = 1;
        this.measure = measure;
        this.geomAttribute = geomAttribute;
        this.point = point;
    }

    public String[][] calculateKriging() {
        KrigingResult k = KrigingCalculator.calculate(xCoordinate, yCoordinate, measureContents, calculatedOnCloud,
                point);
        return new String[][] { { String.valueOf(k.ZEstimation), String.valueOf(k.kriegageVvariance) } };
    }

    public int getStep() {
        return step;
    }

    public void nextStep() {
        this.step++;
    }

    public Map<String, SplitPoint> getSplitPoints() {
        return splitPoints;
    }

    public String getMeasure() {
        return measure;
    }

    public String getGeomAttribute() {
        return geomAttribute;
    }

    public void setCustomCall(String customCall) {
        // Declare custom call to the cloud
        InputStream is = null;
        try {
            is = new ByteArrayInputStream(customCall.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        super.extraBinaryContent = new InputStream[1];
        super.extraBinaryContent[0] = is;
    }

    public void addAttributeName(String attributeName) {
        String[] newAttributes = new String[attributeNames.length + 1];
        System.arraycopy(attributeNames, 0, newAttributes, 0, attributeNames.length);
        newAttributes[newAttributes.length - 1] = attributeName;
        attributeNames = newAttributes;
    }

    public void addProtectedAttributeName(String protectedAttributeName) {
        String[] newAttributes = new String[protectedAttributeNames.length + 1];
        System.arraycopy(protectedAttributeNames, 0, newAttributes, 0, protectedAttributeNames.length);
        newAttributes[newAttributes.length - 1] = protectedAttributeName;
        protectedAttributeNames = newAttributes;
    }

    public void setCalculatedOnCloud(String[] calculatedOnCloud) {
        this.calculatedOnCloud = calculatedOnCloud;
    }

    public void setMeasureContents(String[] measureContents) {
        this.measureContents = measureContents;
    }

    public void setxCoordinate(String[] xCoordinate) {
        this.xCoordinate = xCoordinate;
    }

    public void setyCoordinate(String[] yCoordinate) {
        this.yCoordinate = yCoordinate;
    }
}
