package eu.clarussecure.dataoperations.testing;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import eu.clarussecure.dataoperations.Criteria;
import eu.clarussecure.dataoperations.splitting.Constants;
import eu.clarussecure.dataoperations.splitting.Util;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.log4j.Logger;

public class Cloud {

    private final static Logger LOG = Logger.getLogger(Cloud.class);

    private String[][] data;
    private final String[] attributes;

    public Cloud(String[] attrs, String[][] data) {
        this.data = data;
        this.attributes = attrs;
    }

    @Override
    public String toString() {
        String result = String.join(", ", attributes);
        for (String[] row : data) {
            result += String.join(", ", row);
        }
        return result;
    }

    public String[][] get(String[] protectedAttributeNames, Criteria[] criteria) {
        LOG.info(Arrays.toString(attributes));
        LOG.info(Arrays.toString(protectedAttributeNames));
        String[][] loadedData = data.clone();
        if (criteria != null && criteria.length > 0) {
            for (Criteria c : criteria) {
                int pos = 0;
                if ((pos = haveAttribute(c.getAttributeName())) != -1) {
                    final int P = pos;
                    loadedData = Arrays.stream(loadedData).filter(getPredicate(c, P)).toArray(String[][]::new);
                }
            }
        }
        if (loadedData.length == 0) {
            // Nothing to return
            return null;
        }
        Map<String, String[]> table = datasetByColumns(attributes, loadedData);

        for (String a : protectedAttributeNames) {
            if (a.contains(Constants.krigingCalculateX)) {
                String geomAttr = a.split("(\\(|\\))")[1];
                String[] calculateX = calculateX(geomAttr);
                if (calculateX == null) {
                    throw new RuntimeException("Calculate X is null");
                }
                LOG.info("Inserting into table: " + a + " -> " + calculateX);
                table.put(a, calculateX);
            } else if (a.contains(Constants.krigingCalculateY)) {
                String[] parameters = a.split("(\\(|\\))")[1].split(",\\s*\\{");
                String geomAttr = parameters[0];
                String[] calculateX = parameters[1].substring(0, parameters[1].length() - 1).split(",");
                String[] calculateY = calculateY(geomAttr, calculateX);
                if (calculateY == null) {
                    throw new RuntimeException("Calculate X is null");
                }
                LOG.info("Inserting into table: " + a + " -> " + calculateY);
                table.put(a, calculateY);
            }
        }

//        if (extraContent != null && extraContent.length > 0) {
//            BufferedReader in = new BufferedReader(new InputStreamReader(extraContent[0]));
//            String instruction = null;
//            try {
//                instruction = in.readLine();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            switch (instruction) {
//                case Constants.krigingCalculateX:
//                    String attribute = null;
//                    try {
//                        attribute
//                                = in.readLine();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    String[] calculateX = this.calculateX(attribute);
//                    table.put(protectedAttributeNames[0], calculateX);
//                    break;
//                case Constants.krigingCalculateY:
//                    attribute = null;
//                    calculateX = null;
//                    try {
//                        attribute = in.readLine();
//                        calculateX = in.readLine().split(":");
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    String[] calculateY
//                            = this.calculateY(attribute, calculateX);
//                    table.put(protectedAttributeNames[0], calculateY);
//                    break;
//                default:
//                    break;
//            }
//        }
        LOG.info("Protected attribute names:");
        for (String s : protectedAttributeNames) {
            LOG.info(s);
        }

        LOG.info("Values in original table:");
        for (String s : table.keySet()) {
            LOG.info(s);
        }

        Map<String, String[]> lTable = new HashMap<>();
        for (String attr : protectedAttributeNames) {

            attr = Demo.removeMeusePrefix(attr);

            if (!table.containsKey(attr)) {
                throw new RuntimeException("Table has no attribute named " + attr);
            }
            String[] value = table.get(attr);
            if (value == null) {
                throw new RuntimeException("Trying to insert null value into map");
            }
            lTable.put(attr, value);
        }
        return datasetByRows(protectedAttributeNames, lTable);
    }

    public void delete(Criteria[] criteria) {
        String[][] loadedData = data.clone();
        if (criteria != null && criteria.length > 0) {
            for (Criteria c : criteria) {
                int pos = 0;
                if ((pos = haveAttribute(c.getAttributeName())) != -1) {
                    final int P = pos;
                    loadedData = Arrays.stream(loadedData).filter(getNegatedPredicate(c, P))
                            .toArray(size -> new String[size][]);
                }
            }
        }
        data = loadedData;
    }

    public void update(Criteria[] criteria, String[] protectedAttributeNames, String[][] protectedContents) {
        Map<String, String[]> table = datasetByColumns(protectedAttributeNames, protectedContents);
        protectedContents = datasetByRows(attributes, table);

        int[] indexes = null;
        if (criteria != null && criteria.length > 0) {
            for (Criteria c : criteria) {
                int pos = 0;
                if ((pos = haveAttribute(c.getAttributeName())) != -1) {
                    final int P = pos;
                    indexes = IntStream.range(0, data.length).filter(getPredicateByInt(c, P)).toArray();
                }
            }
        }
        for (int i = 0; i < indexes.length; i++) {
            data[indexes[i]] = protectedContents[i];
        }
    }

    public void post(String[] protectedAttributeNames, String[][] protectedContents) {
        String[][] newData = new String[data.length + protectedContents.length][];
        System.arraycopy(data, 0, newData, 0, data.length);
        System.arraycopy(protectedContents, 0, newData, data.length, protectedContents.length);
        data = newData;
    }

    private Predicate<String[]> getPredicate(Criteria c, final int pos) {
        switch (c.getOperator()) {
            case "=":
                return p -> p[pos].equals(c.getValue()) || Double.parseDouble(p[pos]) == Double.parseDouble(c.getValue());
            case ">":
                return p -> Double.parseDouble(p[pos]) > Double.parseDouble(c.getValue());
            case ">=":
                return p -> Double.parseDouble(p[pos]) >= Double.parseDouble(c.getValue());
            case "<":
                return p -> Double.parseDouble(p[pos]) < Double.parseDouble(c.getValue());
            case "<=":
                return p -> Double.parseDouble(p[pos]) <= Double.parseDouble(c.getValue());
            case Constants.area:
                return p -> {
                    double[] boundaries = Arrays.stream(c.getValue().split(",")).mapToDouble(Double::parseDouble).toArray();
                    return inArea(p[pos], boundaries);
                };
            case Constants.in:
                return p -> {
                    String[] listOfValues = c.getValue().split(",");
                    List<String> listOfStrings = Arrays.asList(listOfValues);
                    List<Double> listOfDoubles = Arrays.stream(listOfValues).map(Double::parseDouble)
                            .collect(Collectors.toList());
                    return listOfStrings.contains(p[pos]) || listOfDoubles.contains(Double.parseDouble(p[pos]));
                };
            default:
                return p -> true;
        }
    }

    private IntPredicate getPredicateByInt(Criteria c, final int pos) {
        switch (c.getOperator()) {
            case "=":
                return p -> data[p][pos].equals(c.getValue())
                        || Double.parseDouble(data[p][pos]) == Double.parseDouble(c.getValue());
            case ">":
                return p -> Double.parseDouble(data[p][pos]) > Double.parseDouble(c.getValue());
            case ">=":
                return p -> Double.parseDouble(data[p][pos]) >= Double.parseDouble(c.getValue());
            case "<":
                return p -> Double.parseDouble(data[p][pos]) < Double.parseDouble(c.getValue());
            case "<=":
                return p -> Double.parseDouble(data[p][pos]) <= Double.parseDouble(c.getValue());
            case Constants.area:
                return p -> {
                    double[] boundaries = Arrays.stream(c.getValue().split(",")).mapToDouble(Double::parseDouble).toArray();
                    return inArea(data[p][pos], boundaries);
                };
            case Constants.in:
                return p -> {
                    String[] listOfValues = c.getValue().split(",");
                    List<String> listOfStrings = Arrays.asList(listOfValues);
                    List<Double> listOfDoubles = Arrays.stream(listOfValues).map(Double::parseDouble)
                            .collect(Collectors.toList());
                    return listOfStrings.contains(data[p][pos]) || listOfDoubles.contains(Double.parseDouble(data[p][pos]));
                };
            default:
                return p -> true;
        }

    }

    private Predicate<String[]> getNegatedPredicate(Criteria c, final int pos) {
        switch (c.getOperator()) {
            case "=":
                return p -> !(p[pos].equals(c.getValue())
                        || Double.parseDouble(p[pos]) == Double.parseDouble(c.getValue()));
            case ">":
                return p -> !(Double.parseDouble(p[pos]) > Double.parseDouble(c.getValue()));
            case ">=":
                return p -> !(Double.parseDouble(p[pos]) >= Double.parseDouble(c.getValue()));
            case "<":
                return p -> !(Double.parseDouble(p[pos]) < Double.parseDouble(c.getValue()));
            case "<=":
                return p -> !(Double.parseDouble(p[pos]) <= Double.parseDouble(c.getValue()));
            case Constants.area:
                return p -> {
                    double[] boundaries = Arrays.stream(c.getValue().split(",")).mapToDouble(Double::parseDouble).toArray();
                    return !inArea(p[pos], boundaries);
                };
            case Constants.in:
                return p -> {
                    String[] listOfValues = c.getValue().split(",");
                    List<String> listOfStrings = Arrays.asList(listOfValues);
                    List<Double> listOfDoubles = Arrays.stream(listOfValues).map(Double::parseDouble)
                            .collect(Collectors.toList());
                    return !(listOfStrings.contains(p[pos]) || listOfDoubles.contains(Double.parseDouble(p[pos])));
                };
            default:
                return p -> false;
        }
    }

    public String[] calculateX(String geoAttributeName) {
        List<BigDecimal> resultList = new ArrayList<BigDecimal>();
        int gpos = Arrays.asList(attributes).indexOf(geoAttributeName);
        String[] result;

        for (int i = 0; i < data.length; i++) {
            for (int j = i; j < data.length; j++) {

                BigDecimal dist = null, xi = null, xj = null;

                try {
                    xi = extractX(data[i][gpos]);
                    xj = extractX(data[j][gpos]);
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                dist = (xj.subtract(xi)).pow(2);
                resultList.add(dist);
            }
        }

        result = new String[resultList.size()];
        for (int i = 0; i < resultList.size(); i++) {
            result[i] = resultList.get(i).toString();
        }
        return result;
    }

    public String[] calculateY(String geoAttributeName, String[] xPoints) {
        List<BigDecimal> xList = new ArrayList<BigDecimal>();
        List<BigDecimal> yList = new ArrayList<BigDecimal>();
        List<BigDecimal> resultList;
        String[] result;
        int gpos = Arrays.asList(attributes).indexOf(geoAttributeName);

        for (int i = 0; i < xPoints.length; i++) {
            xList.add(new BigDecimal(xPoints[i]));
        }

        for (int i = 0; i < data.length; i++) {
            for (int j = i; j < data.length; j++) {

                BigDecimal dist = null, yi = null, yj = null;

                try {
                    yi = extractY(data[i][gpos]);
                    yj = extractY(data[j][gpos]);
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                dist = (yj.subtract(yi)).pow(2);
                yList.add(dist);
            }
        }

        resultList = calculateDistance(xList, yList);

        result = new String[resultList.size()];
        for (int i = 0; i < resultList.size(); i++) {
            result[i] = resultList.get(i).toString();
        }
        return result;
    }

    public List<BigDecimal> calculateDistance(List<BigDecimal> xPoints, List<BigDecimal> yPoints) {
        List<BigDecimal> result = new ArrayList<BigDecimal>();
        for (int i = 0; i < xPoints.size(); i++) {
            result.add(sqrt(xPoints.get(i).add(yPoints.get(i))));
        }

        return result;
    }

    private BigDecimal sqrt(BigDecimal x) {
        return BigDecimal.valueOf(StrictMath.sqrt(x.doubleValue()));
    }

    private BigDecimal extractX(String wkb) throws ParseException {
        BigDecimal result = null;
        Geometry geom = null;
        WKBReader reader = new WKBReader();

        try {
            geom = reader.read(WKBReader.hexToBytes(wkb));
        } catch (com.vividsolutions.jts.io.ParseException e) {
            e.printStackTrace();
        }
        result = new BigDecimal(geom.getCoordinate().x);

        return result;
    }

    private BigDecimal extractY(String wkb) throws ParseException {
        BigDecimal result = null;
        Geometry geom = null;
        WKBReader reader = new WKBReader();

        try {
            geom = reader.read(WKBReader.hexToBytes(wkb));
        } catch (com.vividsolutions.jts.io.ParseException e) {
            e.printStackTrace();
        }
        result = new BigDecimal(geom.getCoordinate().y);

        return result;
    }

    public static Map<String, String[]> datasetByColumns(String[] attributeNames, String[][] content) {
        // Have content in matrix[rows][columns] need content in
        // matrix[columns][rows]
        String[][] transposedContent = transpose(content);
        Map<String, String[]> table = new HashMap<>();
        for (int i = 0; i < attributeNames.length; i++) {
            String[] value = transposedContent[i];
            if (value == null) {
                throw new RuntimeException("Trying to insert null value into map");
            }
            table.put(attributeNames[i], value);
        }
        return table;
    }

    public static String[][] datasetByRows(String[] attributeNames, Map<String, String[]> dataset) {
        attributeNames[0] = Demo.removeMeusePrefix(attributeNames[0]);
        if (!dataset.containsKey(attributeNames[0]) || dataset.get(attributeNames[0]) == null) {
            throw new RuntimeException("Dataset does not contain key " + attributeNames[0]);
        }
        int rows = dataset.get(attributeNames[0]).length;
        int columns = attributeNames.length;
        String[][] table = new String[columns][rows];

        for (int i = 0; i < attributeNames.length; i++) {
            table[i] = dataset.get(attributeNames[i]);
        }

        return transpose(table);
    }

    public static String[][] transpose(String[][] content) {
        LOG.info("Entering transpose with content " + content.length + " / " + content[0].length);
        if (Util.isNullOrContainsNullString(content)) {
            LOG.info(Util.stringArrayToString(content));
            throw new RuntimeException("Contains null");
        }
        String[][] transposedContent = new String[content[0].length][content.length];
        for (int i = 0; i < content.length; i++) {
            for (int j = 0; j < content[0].length; j++) {
                transposedContent[j][i] = content[i][j];
            }
        }
        return transposedContent;
    }

    public int haveAttribute(String attributeName) {
        int pos = -1;
        for (int i = 0; i < attributes.length; i++) {
            if (attributes[i].equals(attributeName)) {
                pos = i;
                break;
            }
        }
        return pos;
    }

    private boolean inArea(String wkbHex, double[] boundary) {
        Geometry geom = null;
        WKBReader reader = new WKBReader();
        WKBWriter writer = new WKBWriter(2, 2, true);
        double x = 0;
        double y = 0;
        try {
            geom = reader.read(WKBReader.hexToBytes(wkbHex));
            x = geom.getCoordinate().x;
            y = geom.getCoordinate().y;
        } catch (Exception e) {
            e.printStackTrace();
        }
        boolean inX = x >= boundary[0] && x <= boundary[2];
        boolean inY = y >= boundary[1] && y <= boundary[3];
        return inX && inY;
    }
}
