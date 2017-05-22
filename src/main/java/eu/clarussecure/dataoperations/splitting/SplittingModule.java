package eu.clarussecure.dataoperations.splitting;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import eu.clarussecure.dataoperations.*;
import eu.clarussecure.dataoperations.kriging.DBRecord;
import eu.clarussecure.dataoperations.kriging.KrigingModuleCommand;
import eu.clarussecure.dataoperations.kriging.KrigingModuleResponse;
import org.w3c.dom.Document;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SplittingModule implements DataOperation {
    //todo Testing data base
    List<Map<String, String>> dataBase;
    Map<String, SplitPoint> splitPoints;


    public SplittingModule(Document document) {
        Functions.readProperties(document);
    }

    @Override
    public List<DataOperationCommand> get(String[] attributeNames, Criteria[] criterias) {
        //todo Testing load data base
        List<Map<String, String>> loadedDataBase = dataBase;
        Map<String, SplitPoint> points = splitPoints;

        if (criterias != null && criterias.length > 0) {
            if (criterias[0].getOperator().equals(Constants.kriging)) {
                String a = criterias[0].getAttributeName();
                String g = criterias[0].getValue().split(":")[0];
                String p = criterias[0].getValue().split(":")[1];
                return krigingFirst(a, g, p);
            }
        }

        // List of Commands to return
        List<DataOperationCommand> commands = new ArrayList<>();

        // Todo: get the number of clouds in the policy, for now the number of mappings will do.
        int clouds = loadedDataBase.size();

        // Initialize list of attribute names of each cloud
        List<List<String>> attr = new ArrayList<>();
        List<List<String>> protAttr = new ArrayList<>();
        for (int i = 0; i < clouds; i++) {
            attr.add(new ArrayList<>());
            protAttr.add(new ArrayList<String>());
        }

        // Split call attributes among clouds
        for (String a: attributeNames) {
            // TODO: special treatment of geometric attributes
            if (a.equals("meuseDB/meuse/geom")) { // IF ATTRIBUTE IS GEOM
                int x = points.get(a).getX();
                int y = points.get(a).getY();
                attr.get(x).add(a);
                protAttr.get(x).add(loadedDataBase.get(x).get(a));
                attr.get(y).add(a);
                protAttr.get(y).add(loadedDataBase.get(y).get(a));
            } else {
                for (int i = 0; i < clouds; i++) {
                    if (loadedDataBase.get(i).containsKey(a)) {
                        attr.get(i).add(a);
                        protAttr.get(i).add(loadedDataBase.get(i).get(a));
                    }
                }
            }
        }

        // Split criteria among clouds
        if (criterias != null && criterias.length > 0) {
            List<Criteria[]> splitCriteria = splitCriteria(criterias, loadedDataBase, points);
            commands = genericOutboundGET(attributeNames, attr, protAttr, loadedDataBase, points, criterias, splitCriteria);
        } else {
            // Base case
            commands = genericOutboundGET(attributeNames, attr, protAttr, loadedDataBase, points, null,  null);
        }

        return commands;
    }

    @Override
    public List<DataOperationResult> get(List<DataOperationCommand> promise, List<String[][]> contents) {
        //todo Testing load data base
        List<Map<String, String>> loadedDataBase = dataBase;
        Map<String, SplitPoint> points = splitPoints;

        if (promise.stream().anyMatch(p -> p instanceof KrigingModuleCommand)) {
            return kriging(promise, contents);
        }

        List<DataOperationResult> result = new ArrayList<>();
        Map<String, String[]> table = new HashMap<>();

        // Base case
        // Look for the primary key
        String[] attributeNames = promise.get(0).getAttributeNames();
        String keyAttribute = "";
        for (String a: attributeNames) {
            if (a.equals("meuseDB/meuse/gid")) { // TODO: if a is primary key
                keyAttribute = a;
            }
        }

        // Remove records which are not in results from all clouds
        List<String[][]> contentsIntersection = filterContentsByPrimaryKey(keyAttribute, promise, contents);


        // Transform table of rows to table of named columns
        List<Map<String, String[]>> tables = new ArrayList<>();
        for (int i = 0; i < promise.size(); i++) {
            tables.add(datasetByColumns(promise.get(i).getProtectedAttributeNames(), contentsIntersection.get(i)));
        }

        // Reconstruct original table
        for (String a: attributeNames) {
            //TODO: for geometric attributes
            if (a.equals("meuseDB/meuse/geom")) { //IF ATTRIBUTE IS GEOM
                int x = ((SplitModuleCommand) promise.get(0)).getSplitPoints().get(a).getX();
                int y = ((SplitModuleCommand) promise.get(0)).getSplitPoints().get(a).getY();
                String columnXName = promise.get(x).getMapping().get(a);
                String columnYName = promise.get(y).getMapping().get(a);
                String[] columnX = tables.get(x).get(columnXName);
                String[] columnY = tables.get(y).get(columnYName);
                table.put(a, joinGeomColumn(columnX, columnY));
            } else {
                for (int i = 0; i < promise.size(); i++) {
                    if (promise.get(i).getMapping().containsKey(a)) {
                        table.put(a, tables.get(i).get(promise.get(i).getMapping().get(a)));
                    }
                }
            }
        }

        //TODO: decrypt needed columns

        // Transform table of named columns to table of rows
        String[][] datasetByRows = datasetByRows(attributeNames, table);

        datasetByRows = filter(attributeNames, datasetByRows, ((SplitModuleCommand) promise.get(0)).getOriginalCriteria());
        result.add(new SplitModuleResponse(attributeNames, datasetByRows));

        return result;
    }

    @Override
    public List<DataOperationCommand> post(String[] attributeNames, String[][] content) {
        //TODO: Load data base
        List<Map<String, String>> loadedDataBase = dataBase;
        Map<String, SplitPoint> loadedSplitPoints = splitPoints;

        // TODO: Number of clouds in policy and, where should an attribute go?
        int clouds = 2;

        // Transform table of rows to table of named columns
        Map<String, String[]> dataset = datasetByColumns(attributeNames, content);

        // List of commands to return
        List<DataOperationCommand> commands = new ArrayList<>();
        // Split dataset
        List<Map<String, String[]>> splitDataset;

        // Dataset does not exist, initialize database and split data
        if (loadedDataBase == null) {
            // TODO: Encrypt needed columns

            // Create mapping database
            List<Map<String, String>> mapping = new ArrayList<>();
            for (int i = 0; i < clouds; i++) {
                mapping.add(new HashMap<String, String>());
            }

            // Initialize split dataset
            splitDataset = new ArrayList<>();
            for (int i = 0; i < clouds; i++) {
                splitDataset.add(new HashMap<String, String[]>());
            }

            // Fill in split dataset
            // For now, assignment is random, should the policy specify WHERE each attribute goes?
            Random rng = new Random();
            Map<String, SplitPoint> points = new HashMap<>();
            for (String a : attributeNames) {
                // ALL GEOMETRIC OBJECTS ARE SPLIT
                if (a.equals("meuseDB/meuse/geom")) { //TODO: IF ATTRIBUTE IS GEOM
                    int xCoordLocation = rng.nextInt(clouds);
                    int yCoordLocation = 0;
                    do {
                        yCoordLocation = rng.nextInt(clouds);
                    } while (yCoordLocation == xCoordLocation);

                    points.put(a, new SplitPoint(xCoordLocation, yCoordLocation));
                    mapping.get(xCoordLocation).put(a, buildProtectedAttributeName(a, xCoordLocation));
                    splitDataset.get(xCoordLocation).put(buildProtectedAttributeName(a, xCoordLocation), splitGeomColumn(dataset.get(a), "x"));
                    mapping.get(yCoordLocation).put(a, buildProtectedAttributeName(a, yCoordLocation));
                    splitDataset.get(yCoordLocation).put(buildProtectedAttributeName(a, yCoordLocation), splitGeomColumn(dataset.get(a), "y"));
                }
                // PRIMARY KEYS ARE REPLICATED IN ALL CLOUDS
                else if (a.equals("meuseDB/meuse/gid")) { // TODO: IF ATTRIBUTE IS PRIMARY KEY
                    for (int i = 0; i < clouds; i++) {
                        mapping.get(i).put(a, buildProtectedAttributeName(a, i));
                        splitDataset.get(i).put(buildProtectedAttributeName(a, i), dataset.get(a));
                    }
                }
                // THE REST ARE ASSIGNED TO RANDOM CLOUDS
                else {
                    int cloud = rng.nextInt(clouds);
                    mapping.get(cloud).put(a, buildProtectedAttributeName(a, cloud));
                    splitDataset.get(cloud).put(buildProtectedAttributeName(a, cloud), dataset.get(a));
                }
            }

            // TODO: Save DB
            this.dataBase = mapping;
            loadedDataBase = mapping;
            this.splitPoints = points;
            loadedSplitPoints = points;

        } else {
            // New data on existing database

            // Initialize split dataset
            splitDataset = new ArrayList<>();
            for (int i = 0; i < clouds; i++) {
                splitDataset.add(new HashMap<String, String[]>());
            }

            // Fill in split dataset
            for (String a: attributeNames) {
                // PRIMARY KEYS ARE REPLICATED IN ALL CLOUDS
                if (a.equals("meuseDB/meuse/gid")) { // TODO: IF ATTRIBUTE IS PRIMARY KEY
                    for (int i = 0; i < clouds; i++) {
                        splitDataset.get(i).put(loadedDataBase.get(i).get(a), dataset.get(a));
                    }
                }
                // GEOMETRIC OBJECTS ARE SPLIT ACCORDING TO MAPPING & SPLITPOINTS
                else if (a.equals("meuseDB/meuse/geom")) { // TODO: IF ATTRIBUTE IS GEOM
                    int x = loadedSplitPoints.get(a).getX();
                    int y = loadedSplitPoints.get(a).getY();
                    splitDataset.get(x).put(loadedDataBase.get(x).get(a), splitGeomColumn(dataset.get(a), "x"));
                    splitDataset.get(y).put(loadedDataBase.get(y).get(a), splitGeomColumn(dataset.get(a), "y"));
                }
                // THE REST ARE SPLIT ACCORDING TO EXISTING MAPPING
                else {
                    for (int i = 0; i < clouds; i++) {
                        if (loadedDataBase.get(i).containsKey(a)) {
                            splitDataset.get(i).put(loadedDataBase.get(i).get(a), dataset.get(a));
                        }
                    }
                }
            }
        }

        // Build post Commands
        for (int i = 0; i < clouds; i++) {
            String[] attrNames = new String[loadedDataBase.get(i).keySet().size()];
            attrNames = loadedDataBase.get(i).keySet().toArray(attrNames);
            String[] protectedAttributeNames = new String[attrNames.length];
            for (int j = 0; j < attrNames.length; j++) {
                protectedAttributeNames[j] = loadedDataBase.get(i).get(attrNames[j]);
            }
            String[][] protectedContent = datasetByRows(protectedAttributeNames, splitDataset.get(i));
            //TODO add to existing clouds
            commands.add(new SplitModuleCommand(attributeNames, protectedAttributeNames, loadedDataBase.get(i), loadedSplitPoints, content, protectedContent));
        }

        return commands;
    }

    @Override
    public List<DataOperationCommand> put(String[] attributeNames, Criteria[] criterias, String[][] content) {
        // TODO: IMPORTANT -> ONLY WORKS FINE FOR UPDATES REFERENCING DATA BY PRIMARY KEY
        // TODO: FOR NOW PLEASE DO A GET BEFORE PUT TO KNOW THE PRIMARY KEYS TO UPDATE,
        // TODO: OTHERWISE, ONLY THE CLOUD THAT CONTAINS THE SPECIFIC COLUMN WILL BE UPDATED
        // TODO: TO SOLVE THIS, PUT SHOULD ALSO BE ORCHESTRATED

        //todo Testing load data base
        List<Map<String, String>> loadedDataBase = dataBase;
        Map<String, SplitPoint> points = splitPoints;

        List<DataOperationCommand> commands = new ArrayList<>();
        int clouds = loadedDataBase.size();

        // Parse dataset into column form
        Map<String, String[]> dataset = datasetByColumns(attributeNames, content);

        // Initialize split dataset
        List<Map<String, String[]>> splitDataset = new ArrayList<>();
        for (int i = 0; i < clouds; i++) {
            splitDataset.add(new HashMap<String, String[]>());
        }

        List<Criteria[]> splitCriteria = splitCriteria(criterias, loadedDataBase, points);

        for (String a: attributeNames) {
            if (a.equals("meuseDB/meuse/gid")) {
                for (int i = 0; i < clouds; i++) {
                    splitDataset.get(i).put(loadedDataBase.get(i).get(a), dataset.get(a));
                }
            } else if (a.equals("meuseDB/meuse/geom")) {
                int x = points.get(a).getX();
                int y = points.get(a).getY();
                splitDataset.get(x).put(loadedDataBase.get(x).get(a), splitGeomColumn(dataset.get(a), "x"));
                splitDataset.get(y).put(loadedDataBase.get(y).get(a), splitGeomColumn(dataset.get(a), "y"));
            } else {
                for (int i = 0; i < clouds; i++) {
                    if (loadedDataBase.get(i).containsKey(a)) {
                        splitDataset.get(i).put(loadedDataBase.get(i).get(a), dataset.get(a));
                    }
                }
            }
        }

        for (int i = 0; i < clouds; i++) {
            String[] attrNames = new String[loadedDataBase.get(i).keySet().size()];
            attrNames = loadedDataBase.get(i).keySet().toArray(attrNames);
            String[] protectedAttributeNames = new String[attrNames.length];
            for (int j = 0; j < attrNames.length; j++) {
                protectedAttributeNames[j] = loadedDataBase.get(i).get(attrNames[j]);
            }
            String[][] protectedContent = datasetByRows(protectedAttributeNames, splitDataset.get(i));
            SplitModuleCommand command = new SplitModuleCommand(attributeNames, protectedAttributeNames, loadedDataBase.get(i), splitPoints, content, protectedContent);
            command.setCriteria(splitCriteria.get(i));
            command.setOriginalCriteria(criterias);
            commands.add(command);
        }

        return commands;
    }

    @Override
    public List<DataOperationCommand> delete(String[] attributeNames, Criteria[] criterias) {
        // TODO: IMPORTANT -> ONLY WORKS FINE FOR DELETES REFERENCING DATA BY PRIMARY KEY
        // TODO: FOR NOW PLEASE DO A GET BEFORE DELETE TO KNOW THE PRIMARY KEYS TO DELETE,
        // TODO: OTHERWISE, ONLY THE CLOUD THAT CONTAINS THE SPECIFIC COLUMN WILL BE DELETED
        // TODO: TO SOLVE THIS, DELETE SHOULD ALSO BE ORCHESTRATED

        //todo Testing load data base
        List<Map<String, String>> loadedDataBase = dataBase;
        Map<String, SplitPoint> points = splitPoints;

        List<DataOperationCommand> commands = new ArrayList<>();
        int clouds = loadedDataBase.size();

        List<Criteria[]> splitCriteria = splitCriteria(criterias, loadedDataBase, points);

        for (int i = 0; i < clouds; i++) {
            String[] attrNames = new String[loadedDataBase.get(i).keySet().size()];
            attrNames = loadedDataBase.get(i).keySet().toArray(attrNames);
            String[] protectedAttributeNames = new String[attrNames.length];
            for (int j = 0; j < attrNames.length; j++) {
                protectedAttributeNames[j] = loadedDataBase.get(i).get(attrNames[j]);
            }
            SplitModuleCommand command = new SplitModuleCommand(attributeNames, protectedAttributeNames, loadedDataBase.get(i), splitPoints, null, null);
            command.setCriteria(splitCriteria.get(i));
            command.setOriginalCriteria(criterias);
            commands.add(command);
        }

        return commands;
    }

    @Override
    public List<Map<String, String>> head(String[] strings) {
        // TODO: load from database
        return dataBase;
    }


    //TODO: Temp method for kriging ------------------------------------------------------------------------------
    public List<DataOperationCommand> krigingFirst(String attributeName, String geoAttributeName, String point) {
        //todo Testing load data base
        List<Map<String, String>> loadedDataBase = dataBase;
        Map<String, SplitPoint> points = splitPoints;

        int clouds = loadedDataBase.size();
        List<DataOperationCommand> commands = new ArrayList<>();
        for (int i = 0; i < clouds; i++) {
            commands.add(null);
        }

        // Bring in the X coordinate
        // where is X coordinate?
        int x = points.get(geoAttributeName).getX();
        // what's its name?
        String xAttributeName = loadedDataBase.get(x).get(geoAttributeName);

        KrigingModuleCommand kmc = new KrigingModuleCommand(
                new String[] {geoAttributeName},
                new String[] {xAttributeName},
                loadedDataBase.get(x),
                points,
                attributeName,
                geoAttributeName,
                point);

        commands.set(x, kmc);

        // Bring in the Y coordinate
        // where is Y coordinate?
        int y = points.get(geoAttributeName).getY();
        // what's its name
        String yAttributeName = loadedDataBase.get(y).get(geoAttributeName);
        kmc = new KrigingModuleCommand(
                new String[] {geoAttributeName},
                new String[] {yAttributeName},
                loadedDataBase.get(y),
                points,
                attributeName,
                geoAttributeName,
                point);

        commands.set(y, kmc);


        // Bring in the MEASURE
        // What cloud holds the measure
        int measureCloud = 0;
        for (int i = 0; i < clouds; i++) {
            if (loadedDataBase.get(i).containsKey(attributeName)) {
                measureCloud = i;
                break;
            }
        }
        // What's the MEASURE name in that cloud?
        String measureAttributeName = loadedDataBase.get(measureCloud).get(attributeName);

        if (x == measureCloud) {
            ((KrigingModuleCommand)commands.get(x)).addAttributeName(attributeName);
            ((KrigingModuleCommand)commands.get(x)).addProtectedAttributeName(measureAttributeName);
        } else if (y == measureCloud) {
            ((KrigingModuleCommand)commands.get(y)).addAttributeName(attributeName);
            ((KrigingModuleCommand)commands.get(y)).addProtectedAttributeName(measureAttributeName);
        } else {
           commands.set(measureCloud,
                   new SplitModuleCommand(
                           new String[] {attributeName},
                           new String[] {measureAttributeName},
                           loadedDataBase.get(measureCloud),
                           points,
                           null,
                           null
                   )
           );
        }

        return commands;
    }

    public List<DataOperationResult> kriging(List<DataOperationCommand> commands, List<String[][]> contents) {
        //todo Testing load data base
        List<Map<String, String>> loadedDataBase = dataBase;
        Map<String, SplitPoint> points = splitPoints;

        int clouds = loadedDataBase.size();
        List<DataOperationResult> results = new ArrayList<>();
        for (int i = 0; i < clouds; i++) {
            results.add(null);
        }

        // One of the KMC will be used for next calls
        KrigingModuleCommand kmc = null;
        for (DataOperationCommand c: commands) {
            if (c != null && c instanceof KrigingModuleCommand) {
                kmc = (KrigingModuleCommand) c;
                break;
            }
        }

        int x = kmc.getSplitPoints().get(kmc.getGeomAttribute()).getX();
        int y = kmc.getSplitPoints().get(kmc.getGeomAttribute()).getY();

        switch (kmc.getStep()) {
            case 1:
                kmc.nextStep();
                kmc.setxCoordinate(transpose(contents.get(x))[0]);
                kmc.setyCoordinate(transpose(contents.get(y))[0]);
                for (int i = 0; i < commands.size(); i++) {
                    if (commands.get(i) != null && commands.get(i).getMapping().containsKey(kmc.getMeasure())) {
                        String[][] content = transpose(contents.get(i));
                        kmc.setMeasureContents(content[content.length - 1]);
                    }
                }
                kmc.setProtectedAttributeNames(new String[] {Constants.krigingCalculateX});
                kmc.setCustomCall(Constants.krigingCalculateX + "\n"
                        + loadedDataBase.get(x).get(kmc.getGeomAttribute()));
                results.set(x, kmc);
                break;
            case 2:
                kmc.nextStep();
                String[] calculateX = transpose(contents.get(x))[0];
                kmc.setProtectedAttributeNames(new String[] {Constants.krigingCalculateY});
                kmc.setCustomCall(Constants.krigingCalculateY + "\n"
                        + loadedDataBase.get(y).get(kmc.getGeomAttribute()) + "\n"
                        + String.join(":", calculateX));
                results.set(y, kmc);
                break;
            case 3:
                String[] calculatedOnCloud = transpose(contents.get(y))[0];
                kmc.setCalculatedOnCloud(calculatedOnCloud);
                String[][] k = kmc.calculateKriging();
                results = new ArrayList<>();
                results.add(new KrigingModuleResponse(
                        new String[] {"value", "variance"},
                        k
                ));
                break;
        }

        return results;
    }
    //TODO: End temp method for kriging --------------------------------------------------------------------------

    private List<String[][]> filterContentsByPrimaryKey(String keyAttribute, List<DataOperationCommand> promise, List<String[][]> contents) {

        // Index table of results by primary key
        final List<Map<String, String[]>> indexedTables = new ArrayList<>();
        for (int i = 0; i < promise.size(); i++) {
            String cloudKeyAttribute = promise.get(i).getMapping().get(keyAttribute);
            int column = 0;
            for (column = 0; column < promise.get(i).getProtectedAttributeNames().length; column++) {
                if (cloudKeyAttribute.equals(promise.get(i).getProtectedAttributeNames()[column])) {
                    break;
                }
            }
            final int C = column;
            indexedTables.add(Arrays.stream(contents.get(i)).collect(Collectors.toMap(p -> p[C], p -> p)));
        }

        // Intersection of results, remove records whose
        // primary keys are not in results from all clouds
        Set<String> ids = indexedTables.stream().map(e -> e.keySet()).flatMap(l -> l.stream()).collect(Collectors.toSet());
        Map<String, Boolean> inAll = ids.stream()
                .collect(Collectors.toMap(s -> s, s -> indexedTables.stream().map(m -> m.containsKey(s)).reduce(true, (a, b) -> a&& b).booleanValue()));
        for (Map<String, String[]> m : indexedTables) {
            for (String id : inAll.keySet()) {
                if (!inAll.get(id)) m.remove(id);
            }
        }

        // Reconstruct filtered results
        List<String[][]> contentsIntersection = new ArrayList<>();
        for (Map<String, String[]> m : indexedTables) {
            String[][] t = m.entrySet().stream()
                    .sorted((e1, e2) -> Integer.valueOf(e1.getKey()).compareTo(Integer.valueOf(e2.getKey())))
                    .map(Map.Entry::getValue)
                    .toArray(size -> new String[size][]);
            contentsIntersection.add(t);
        }
        return contentsIntersection;
    }

    private String[][] filter(String[] attributeNames, String[][] contents, Criteria[] criteria) {
        if (criteria == null) {
            return contents;
        } else {
            for (Criteria c : criteria) {
                int pos = 0;
                if ((pos = haveAttribute(attributeNames, c.getAttributeName())) != -1) {
                    final int P = pos;
                    contents = Arrays.stream(contents)
                            .filter(getPredicate(c, P))
                            .toArray(size -> new String[size][]);
                }
            }

        }
        return contents;
    }

    private int haveAttribute (String[] attributes, String attributeName) {
        int pos = -1;
        for (int i = 0; i < attributes.length; i++) {
            if (attributes[i].equals(attributeName)) {
                pos = i;
                break;
            }
        }
        return pos;
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
                    double[] boundaries = Arrays.stream(c.getValue().split(","))
                            .mapToDouble(Double::parseDouble)
                            .toArray();
                    return inArea(p[pos], boundaries);
                };
            case Constants.in:
                return p -> {
                    String[] listOfValues = c.getValue().split(",");
                    List<String> listOfStrings = Arrays.asList(listOfValues);
                    List<Double> listOfDoubles = Arrays.stream(listOfValues).map(Double::parseDouble).collect(Collectors.toList());
                    return listOfStrings.contains(p[pos]) || listOfDoubles.contains(Double.parseDouble(p[pos]));
                };
            default:
                return p -> true;
        }
    }

    private List<DataOperationCommand> genericOutboundGET(String[] attributeNames, List<List<String>> attr, List<List<String>> protAttr, List<Map<String, String>> loadedDataBase, Map<String,SplitPoint> points, Criteria[] originalCriteria, List<Criteria[]> criteria) {
        List<DataOperationCommand> commands = new ArrayList<>();
        int clouds = loadedDataBase.size();

        for (int i = 0; i < clouds; i++) {
            String[] attrNames = new String[attr.get(i).size()];
            String[] protAttrNames = new String[protAttr.get(i).size()];
            attrNames = attr.get(i).toArray(attrNames);
            protAttrNames = protAttr.get(i).toArray(protAttrNames);
            SplitModuleCommand command = new SplitModuleCommand(attributeNames, protAttrNames, loadedDataBase.get(i), points, null, null);
            if (criteria != null) {
                command.setCriteria(criteria.get(i));
                command.setOriginalCriteria(originalCriteria);
            }
            commands.add(command);
        }
        return commands;
    }

    private List<Criteria[]> splitCriteria(Criteria[] criteria, List<Map<String, String>> loadedDatabase, Map<String, SplitPoint> points) {
        int clouds = loadedDatabase.size();
        List<List<Criteria>> splitCriteria = new LinkedList<>();
        for (int i = 0; i < clouds; i++) {
            splitCriteria.add(new LinkedList<>());
        }
        for (Criteria c : criteria) {
            // Special case for area queries
            if (c.getOperator().equals(Constants.area)) {
                List<Criteria> splitAreaCriteria = splitAreaCriteria(c, loadedDatabase, points);
                for (int i = 0; i < splitAreaCriteria.size(); i++) {
                    splitCriteria.get(i).add(splitAreaCriteria.get(i));
                }
            } else {
                for (int i = 0; i < clouds; i++) {
                    if (loadedDatabase.get(i).containsKey(c.getAttributeName())) {
                        splitCriteria.get(i).add(new Criteria(loadedDatabase.get(i).get(c.getAttributeName()), c.getOperator(), c.getValue()));
                    }
                }
            }
        }

        return splitCriteria.stream().map(list -> {
            Criteria[] c = list.toArray(new Criteria[list.size()]);
            return c;
        }).collect(Collectors.toList());
    }

    private List<Criteria> splitAreaCriteria(Criteria criteria, List<Map<String, String>> loadedDataBase, Map<String,SplitPoint> points) {
        List<Criteria> criterias = new ArrayList<>();

        for (int i = 0; i < loadedDataBase.size(); i++) {
            criterias.add(new Criteria(null, null, null));
        }
        int x = points.get(criteria.getAttributeName()).getX();
        int y = points.get(criteria.getAttributeName()).getY();

        String[] area = criteria.getValue().split(",");

        String[] areaX = {area[0], Constants.MIN_Y, area[2], Constants.MAX_Y};
        criterias.get(x).setAttributeName(loadedDataBase.get(x).get(criteria.getAttributeName()));
        criterias.get(x).setOperator(criteria.getOperator());
        criterias.get(x).setValue(String.join(",", areaX));

        String[] areaY = {Constants.MIN_X, area[1], Constants.MAX_X, area[3]};
        criterias.get(y).setAttributeName(loadedDataBase.get(y).get(criteria.getAttributeName()));
        criterias.get(y).setOperator(criteria.getOperator());
        criterias.get(y).setValue(String.join(",", areaY));

        return criterias;
    }

    private static List<DBRecord> createDBRecords(Map<String, String[]> table){
        List<DBRecord>records = new ArrayList<>();
        double x, y, zinc;
        String valuesGeom[];
        String valuesZinc[];

        valuesGeom = table.get("meuseDB/meuse/geom");
        valuesZinc = table.get("meuseDB/meuse/zinc");
        for(int i=0; i<valuesGeom.length; i++){
            x = getXFromGeom(valuesGeom[i]);
            y = getYFromGeom(valuesGeom[i]);
            zinc = Double.parseDouble(valuesZinc[i]);
            records.add(new DBRecord(new BigDecimal(x), new BigDecimal(y), new BigDecimal(zinc)));
        }

        return records;
    }

    private static double getXFromGeom(String wkbHex){
        Geometry geom = null;
        WKBReader reader = new WKBReader();
        double x = 0;

        try {
            geom = reader.read(WKBReader.hexToBytes(wkbHex));
            x = geom.getCoordinate().x;
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return x;
    }

    private static double getYFromGeom(String wkbHex){
        Geometry geom = null;
        WKBReader reader = new WKBReader();
        double y = 0;

        try {
            geom = reader.read(WKBReader.hexToBytes(wkbHex));
            y = geom.getCoordinate().x;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return y;
    }

    private Map<String, String[]> datasetByColumns(String[] attributeNames, String[][] content) {
        String[][] transposedContent = transpose(content);
        Map<String, String[]> table = new HashMap<>();
        for (int i = 0; i < attributeNames.length; i++) {
            table.put(attributeNames[i], transposedContent[i]);
        }
        return table;
    }

    private String[][] datasetByRows(String[] attributeNames, Map<String, String[]> dataset) {
        int rows = dataset.get(attributeNames[0]).length;
        int columns = attributeNames.length;
        String[][] table = new String[columns][rows];

        for (int i = 0; i < attributeNames.length; i++) {
            table[i] = dataset.get(attributeNames[i]);
        }

        return transpose(table);
    }

    private String[][] transpose(String[][] content) {
        String[][] transposedContent = new String[content[0].length][content.length];
        for (int i = 0; i < content.length; i++) {
            for (int j = 0; j < content[0].length; j++) {
                transposedContent[j][i] = content[i][j];
            }
        }
        return transposedContent;
    }

    private String[] encryptColumn(String[] column) {
        Base64.Encoder encoder = Base64.getEncoder();
        String[] encryptedColumn = new String[column.length];
        for (int i = 0; i < column.length; i++) {
            try {
                byte[] enc = SymmetricCrypto.encrypt(column[i].getBytes("UTF-8"), "KEY?");
                encryptedColumn[i] = encoder.encodeToString(enc);
            } catch (Exception e) {/*Sorry*/}
        }
        return encryptedColumn;
    }

    private String[] decryptColumn(String[] column) {
        Base64.Decoder decoder = Base64.getDecoder();
        String[] decryptedColumn = new String[column.length];
        for (int i = 0; i < column.length; i++) {
            try {
                byte[] dec = SymmetricCrypto.decrypt(decoder.decode(column[i]), "KEY?");
                decryptedColumn[i] = new String(dec, "UTF-8");
            } catch (Exception e) {/*Sorry*/}
        }
        return decryptedColumn;
    }

    private String[] splitGeomColumn(String[] column, String coordinate) {
        String[] ret = new String[column.length];
        switch (coordinate) {
            case "x":
                return Arrays.stream(column).map(this::separateX).toArray(size -> new String[size]);
            case "y":
                return Arrays.stream(column).map(this::separateY).toArray(size -> new String[size]);
            default:
                return null;
        }
    }

    private String[] joinGeomColumn(String[] x, String[] y) {
        return IntStream.range(0, x.length).mapToObj(i -> joinCoords(x[i], y[i])).toArray(size -> new String[size]);
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
        } catch (ParseException e) {
            e.printStackTrace();
        }
        boolean inX = x >= boundary[0] && x <= boundary[2];
        boolean inY = y >= boundary[1] && y <= boundary[3];
        return inX && inY;
    }

    //Returns a String that has a correct X value and a random Y
    private String separateX(String wkbHex) {
        Geometry geom = null;
        WKBReader reader = new WKBReader();
        WKBWriter writer = new WKBWriter(2, 2, true);
        Random rnd = new Random();

        try {
            geom = reader.read(WKBReader.hexToBytes(wkbHex));
            geom.getCoordinate().y = rnd.nextInt(180) - 90;
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return WKBWriter.toHex(writer.write(geom));
    }

    //Returns a String that has a correct Y value and a random X
    private String separateY(String wkbHex) {
        Geometry geom = null;
        WKBReader reader = new WKBReader();
        WKBWriter writer = new WKBWriter(2, 2, true);
        Random rnd = new Random();

        try {
            geom = reader.read(WKBReader.hexToBytes(wkbHex));
            geom.getCoordinate().x = rnd.nextInt(180) - 90;
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return WKBWriter.toHex(writer.write(geom));
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

    private String buildProtectedAttributeName(String attributeName, int cloud) {
        String[] s = attributeName.split("/");
        s[0] = "" + (char) ('A' + cloud);
        return String.join("/", s);
    }
}

