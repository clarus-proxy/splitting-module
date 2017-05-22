package eu.clarussecure.dataoperations.kriging;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import eu.clarussecure.dataoperations.Criteria;
import eu.clarussecure.dataoperations.DataOperationCommand;
import eu.clarussecure.dataoperations.splitting.SplitPoint;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Created by sergi on 05/05/2017.
 */
public class KrigingModuleCommand extends DataOperationCommand {

    private Map<String, SplitPoint> splitPoints = null;
    private Criteria[] originalCriteria;
    private String measure;
    private String geomAttribute;
    private String point;
    private int step;
    private String[] calculateX;
    private String[] calculatedOnCloud;
    private String[] measureContents;
    private String[] xCoordinate;
    private String[] yCoordinate;

    public KrigingModuleCommand(String[] attributeNames, String[] protectedAttributeNames,
                                Map<String, String> mapping, Map<String, SplitPoint> splitPoints,
                                String measure, String geomAttribute, String point) {
        super.id = new Random().nextInt();
        super.attributeNames = attributeNames;
        super.protectedAttributeNames = protectedAttributeNames;
        super.extraProtectedAttributeNames = extraProtectedAttributeNames;
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
        KrigingResult k = KrigingCalculator.calculate(xCoordinate, yCoordinate, measureContents, calculatedOnCloud, point);
        return new String[][] {{String.valueOf(k.ZEstimation), String.valueOf(k.kriegageVvariance)}};
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

    public String[] getCalculatedOnCloud() {
        return calculatedOnCloud;
    }

    public String getMeasure() {
        return measure;
    }

    public String getGeomAttribute() {
        return geomAttribute;
    }

    public String getPoint() {
        return point;
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

    public void removeCustomCall() {
        super.extraBinaryContent = null;
    }

    public void addAttributeName(String attributeName) {
        String[] newAttributes = new String[attributeNames.length + 1];
        System.arraycopy(attributeNames, 0, newAttributes, 0, attributeNames.length);
        newAttributes[newAttributes.length -1] = attributeName;
        attributeNames = newAttributes;
    }

    public void addProtectedAttributeName(String protectedAttributeName) {
        String[] newAttributes = new String[protectedAttributeNames.length + 1];
        System.arraycopy(protectedAttributeNames, 0, newAttributes, 0, protectedAttributeNames.length);
        newAttributes[newAttributes.length -1] = protectedAttributeName;
        protectedAttributeNames = newAttributes;
    }

    public void setCalculateX(String[] calculateX) {
        this.calculateX = calculateX;
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

    public String[] getCalculateX() {
        return calculateX;
    }

    public String[] getMeasureContents() {
        return measureContents;
    }

    public String[] getxCoordinate() {
        return xCoordinate;
    }

    public String[] getyCoordinate() {
        return yCoordinate;
    }


    private String[] joinGeomColumn(String[] x, String[] y) {
        return IntStream.range(0, x.length).mapToObj(i -> joinCoords(x[i], y[i])).toArray(size -> new String[size]);
    }

    //Joins two String coordinates
    private String joinCoords(String wkbX, String wkbY) {
        Geometry geom = null;
        WKBReader reader = new WKBReader();
        WKBWriter writer = new WKBWriter(2, 2, true);
        Coordinate coordX, coordY, newCoord;
        PrecisionModel pmodel = new PrecisionModel();
        GeometryFactory builder = new GeometryFactory(pmodel, 4326);


        try {
            geom = reader.read(WKBReader.hexToBytes(wkbX));
            coordX = geom.getCoordinate();
            geom = reader.read(WKBReader.hexToBytes(wkbY));
            coordY = geom.getCoordinate();
            newCoord = new Coordinate(coordX.x, coordY.y);
            geom = builder.createPoint(newCoord);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return WKBWriter.toHex(writer.write(geom));
    }

}
