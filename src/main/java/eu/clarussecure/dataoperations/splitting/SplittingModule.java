package eu.clarussecure.dataoperations.splitting;

import eu.clarussecure.dataoperations.*;
import eu.clarussecure.dataoperations.geometry.GeometryBuilder;
import eu.clarussecure.dataoperations.geometry.ProjectedCRS;
import eu.clarussecure.dataoperations.metadataStorage.MetadataStorage;
import org.postgis.Geometry;
import org.postgis.LineString;
import org.postgis.PGbox2d;
import org.postgis.Point;
import org.w3c.dom.Document;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SplittingModule implements DataOperation {
    // Testing data base
    // private List<Map<String, String>> dataBase;
    // private Map<String, SplitPoint> splitPoints;
    private MetadataStorage storage = MetadataStorage.getInstance();
    //Todo: temporary
    private String dataID = "meuse";

    public SplittingModule(Document document) {
        Functions.readProperties(document);
        // AKKA fix: initialize dataBase (to allow call of any operation at any
        // time)
        // TODO: Number of clouds in policy and, where should an attribute go?
        int clouds = 2;

        // Create mapping database
        List<Map<String, String>> mapping = new ArrayList<>();
        for (int i = 0; i < clouds; i++) {
            mapping.add(new HashMap<>());
        }

        // For now, assignment is random, should the policy specify WHERE each
        // attribute goes?
        Random rng = new Random();
        Map<String, SplitPoint> points = new HashMap<>();
        for (int i = 0; i < Record.refNumAttr; i++) {
            String a = Record.refListNames.get(i);
            if (Record.refListAttrTypes.get(i).equals(Constants.technicalIdentifier)) {
                // PRIMARY KEYS ARE REPLICATED IN ALL CLOUDS
                for (int csp = 0; csp < clouds; csp++) {
                    mapping.get(csp).put(a, buildProtectedAttributeName(a, csp));
                }
            } else if (Record.refListAttrTypes.get(i).equals(Constants.identifier)
                    && Record.refListDataTypes.get(i).equals(Constants.geometricObject)) {
                // ALL GEOMETRIC OBJECTS ARE SPLIT
                int xCoordLocation = rng.nextInt(clouds);
                int yCoordLocation;
                do {
                    yCoordLocation = rng.nextInt(clouds);
                } while (yCoordLocation == xCoordLocation);

                points.put(a, new SplitPoint(xCoordLocation, yCoordLocation));
                mapping.get(xCoordLocation).put(a, buildProtectedAttributeName(a, xCoordLocation));
                mapping.get(yCoordLocation).put(a, buildProtectedAttributeName(a, yCoordLocation));
            } else {
                // THE REST ARE ASSIGNED TO RANDOM CLOUDS (not really random at
                // the moment)
                // int cloud = rng.nextInt(clouds);
                int cloud = i % clouds;
                mapping.get(cloud).put(a, buildProtectedAttributeName(a, cloud));
            }
        }

        // Save DB
        dataID = Record.refListNames.get(0).split("/")[0];
        storage.storeMetadata(dataID, mapping, points);
    }

    @Override
    public List<DataOperationCommand> get(String[] attributeNames, Criteria[] criterias) {
        // AKKA fix: reorder policy definition according to the request
        // attributes
        Functions.reOrderListsAccordingAttributeParameter(attributeNames);

        // Load database
        List<Map<String, String>> loadedDataBase = storage.retrieveMetadata(dataID);
        Map<String, SplitPoint> points = storage.retrieveSplitPoints(dataID);

        if (criterias != null && criterias.length > 0) {
            if (criterias[0].getOperator().equals(Constants.kriging)) {
                String a = criterias[0].getAttributeName();
                String g = criterias[0].getValue().split(":")[0];
                String p = criterias[0].getValue().split(":")[1];
                return krigingFirst(a, g, p);
            }
        }

        // List of Commands to return
        List<DataOperationCommand> commands;

        // Todo: get the number of clouds in the policy, for now the number of
        // mappings will do.
        int clouds = loadedDataBase.size();

        // Initialize list of attribute names of each cloud
        List<List<String>> attr = new ArrayList<>();
        List<List<String>> protAttr = new ArrayList<>();
        // AKKA fix: Initialize list of attribute mapping of each cloud
        List<Map<String, String>> datasetHeaders = new ArrayList<>();
        Map<String, SplitPoint> datasetHeaderSplitPoints = new HashMap<>();
        for (int i = 0; i < clouds; i++) {
            attr.add(new ArrayList<>());
            protAttr.add(new ArrayList<>());
            // AKKA fix: Initialize list of attribute mapping of each cloud
            datasetHeaders.add(new LinkedHashMap<>());
        }

        // Split call attributes among clouds
        // AKKA fix: process splitting according to the policy
        for (int i = 0; i < Record.numAttr; i++) {
            String a = Record.listNames.get(i);
            if (Record.listAttrTypes.get(i).equals(Constants.identifier)
                    && Record.listDataTypes.get(i).equals(Constants.geometricObject)) {
                // IF ATTRIBUTE IS GEOM
                SplitPoint splitPoint = points.get(a);
                datasetHeaderSplitPoints.put(attributeNames[i], splitPoint);
                int x = splitPoint.getX();
                int y = splitPoint.getY();
                attr.get(x).add(attributeNames[i]);
                String protectedAttributeNameX = AttributeNamesUtilities
                        .resolveProtectedAttributeName(loadedDataBase.get(x).get(a), attributeNames[i]);
                protAttr.get(x).add(protectedAttributeNameX);
                datasetHeaders.get(x).put(attributeNames[i], protectedAttributeNameX);
                attr.get(y).add(attributeNames[i]);
                String protectedAttributeNameY = AttributeNamesUtilities
                        .resolveProtectedAttributeName(loadedDataBase.get(y).get(a), attributeNames[i]);
                protAttr.get(y).add(protectedAttributeNameY);
                datasetHeaders.get(y).put(attributeNames[i], protectedAttributeNameY);
            } else {
                for (int csp = 0; csp < clouds; csp++) {
                    if (loadedDataBase.get(csp).containsKey(a)) {
                        attr.get(csp).add(attributeNames[i]);
                        String protectedAttributeName = AttributeNamesUtilities
                                .resolveProtectedAttributeName(loadedDataBase.get(csp).get(a), attributeNames[i]);
                        protAttr.get(csp).add(protectedAttributeName);
                        datasetHeaders.get(csp).put(attributeNames[i], protectedAttributeName);
                    }
                }
            }
        }

        // Split criteria among clouds
        // AKKA fix: mapping in commands depends on the request attributes
        List<Criteria[]> splitCriteria = null;

        if (criterias != null && criterias.length > 0) {
            List<Map<String, String>> datasetCriterias = new ArrayList<>();
            Map<String, SplitPoint> datasetCriteriaSplitPoints = new HashMap<>();
            for (int i = 0; i < clouds; i++) {
                datasetCriterias.add(new HashMap<>());
            }
            String[] criteriaAttributeNames = Arrays.stream(criterias).map(Criteria::getAttributeName)
                    .toArray(String[]::new);
            Functions.reOrderListsAccordingAttributeParameter(criteriaAttributeNames);
            for (int i = 0; i < Record.numAttr; i++) {
                String criteriaAttributeName = criteriaAttributeNames[i];
                String recordName = Record.listNames.get(i);
                if (Record.listAttrTypes.get(i).equals(Constants.identifier)
                        && Record.listDataTypes.get(i).equals(Constants.geometricObject)) {
                    // ALL GEOMETRIC OBJECTS ARE SPLIT
                    SplitPoint splitPoint = points.get(recordName);
                    datasetCriteriaSplitPoints.put(criteriaAttributeName, splitPoint);
                    int x = splitPoint.getX();
                    int y = splitPoint.getY();
                    String protectedAttributeNameX = AttributeNamesUtilities.resolveProtectedAttributeName(
                            loadedDataBase.get(x).get(recordName), criteriaAttributeName);
                    String protectedAttributeNameY = AttributeNamesUtilities.resolveProtectedAttributeName(
                            loadedDataBase.get(y).get(recordName), criteriaAttributeName);
                    datasetCriterias.get(x).put(criteriaAttributeName, protectedAttributeNameX);
                    datasetCriterias.get(y).put(criteriaAttributeName, protectedAttributeNameY);
                } else {
                    // THE REST ARE SPLIT ACCORDING TO EXISTING MAPPING
                    for (int csp = 0; csp < clouds; csp++) {
                        if (loadedDataBase.get(csp).containsKey(recordName)) {
                            String protectedAttributeName = AttributeNamesUtilities.resolveProtectedAttributeName(
                                    loadedDataBase.get(csp).get(recordName), criteriaAttributeName);
                            datasetCriterias.get(csp).put(criteriaAttributeName, protectedAttributeName);
                        }
                    }
                }
            }
            splitCriteria = splitCriteria(criterias, datasetCriterias, datasetCriteriaSplitPoints);
        }

        commands = genericOutboundGET(attributeNames, protAttr, datasetHeaders, datasetHeaderSplitPoints, criterias,
                splitCriteria);

        return commands;
    }

    @Override
    public List<DataOperationResult> get(List<DataOperationCommand> promise, List<String[][]> contents) {

        if (promise.stream().anyMatch(p -> p instanceof KrigingModuleCommand)) {
            return kriging(promise, contents);
        }

        List<DataOperationResult> result = new ArrayList<>();
        Map<String, String[]> table = new HashMap<>();

        // Base case
        // Look for the primary key
        String[] attributeNames = promise.get(0).getAttributeNames();
        // AKKA fix: reorder policy definition according to the request
        // attributes
        Functions.reOrderListsAccordingAttributeParameter(attributeNames);

        String keyAttribute = null;
        // AKKA fix: resolve keyAttribute according to the policy
        for (int i = 0; i < Record.numAttr; i++) {
            String a = attributeNames[i];
            if (Record.listAttrTypes.get(i).equals(Constants.technicalIdentifier)) {
                keyAttribute = a;
            }
        }

        // Remove records which are not in results from all clouds
        // AKKA fix: no intersection if no key attribute
        List<String[][]> contentsIntersection = contents;
        if (keyAttribute != null) {
            contentsIntersection = filterContentsByPrimaryKey(keyAttribute, promise, contents);
        } else {
            contentsIntersection = contents;
        }

        // Transform table of rows to table of named columns
        List<Map<String, String[]>> tables = new ArrayList<>();
        for (int i = 0; i < promise.size(); i++) {
            tables.add(datasetByColumns(promise.get(i).getProtectedAttributeNames(), contentsIntersection.get(i)));
        }

        // Reconstruct original table
        // AKKA fix: resolve attribute according to the policy
        for (int i = 0; i < Record.numAttr; i++) {
            String attributeName = attributeNames[i];
            if (Record.listAttrTypes.get(i).equals(Constants.identifier)
                    && Record.listDataTypes.get(i).equals(Constants.geometricObject)) {
                int x = ((SplitModuleCommand) promise.get(0)).getSplitPoints().get(attributeName).getX();
                int y = ((SplitModuleCommand) promise.get(0)).getSplitPoints().get(attributeName).getY();
                String columnXName = promise.get(x).getMapping().get(attributeName);
                String columnYName = promise.get(y).getMapping().get(attributeName);
                String[] columnX = tables.get(x).get(columnXName);
                String[] columnY = tables.get(y).get(columnYName);
                table.put(attributeName, joinGeomColumn(columnX, columnY));
            } else {
                for (int csp = 0; csp < promise.size(); csp++) {
                    if (promise.get(csp).getMapping().containsKey(attributeName)) {
                        String[] values = tables.get(csp).get(promise.get(csp).getMapping().get(attributeName));
                        if (values != null && values.length > 0) {
                            table.put(attributeName, values);
                        }
                    }
                }
            }
        }

        // TODO: decrypt needed columns

        // Transform table of named columns to table of rows
        String[][] datasetByRows = datasetByRows(attributeNames, table);

        datasetByRows = filter(attributeNames, datasetByRows,
                ((SplitModuleCommand) promise.get(0)).getOriginalCriteria());
        result.add(new SplitModuleResponse(attributeNames, datasetByRows));

        return result;
    }

    @Override
    public List<DataOperationCommand> post(String[] attributeNames, String[][] content) {
        // AKKA fix: reorder policy definition according to the request
        // attributes
        Functions.reOrderListsAccordingAttributeParameter(attributeNames);

        // Load database
        List<Map<String, String>> loadedDataBase = storage.retrieveMetadata(dataID);
        Map<String, SplitPoint> loadedSplitPoints = storage.retrieveSplitPoints(dataID);

        // AKKA fix: database initialized at boot time
        int clouds = loadedDataBase.size();

        // Transform table of rows to table of named columns
        Map<String, String[]> dataset = datasetByColumns(attributeNames, content);

        // List of commands to return
        List<DataOperationCommand> commands = new ArrayList<>();
        // Split dataset
        List<Map<String, String[]>> splitDataset;
        // AKKA fix: we need to split dataset header as well
        List<Map<String, String>> datasetHeaders;
        Map<String, SplitPoint> datasetHeaderSplitPoints = new HashMap<>();

        // AKKA fix: database initialized at boot time
        // New data on existing database

        // Initialize split dataset
        splitDataset = new ArrayList<>();
        // AKKA fix: Initialize split dataset header
        datasetHeaders = new ArrayList<>();
        for (int i = 0; i < clouds; i++) {
            splitDataset.add(new LinkedHashMap<>());
            // AKKA fix: Initialize split dataset header
            datasetHeaders.add(new LinkedHashMap<>());
        }

        // Fill in split dataset
        // AKKA fix: rely on Record to split data
        for (int i = 0; i < Record.numAttr; i++) {
            String attributeName = attributeNames[i];
            String recordName = Record.listNames.get(i);
            if (Record.listAttrTypes.get(i).equals(Constants.technicalIdentifier)) {
                // PRIMARY KEYS ARE REPLICATED IN ALL CLOUDS
                for (int csp = 0; csp < clouds; csp++) {
                    String protectedAttributeName = AttributeNamesUtilities
                            .resolveProtectedAttributeName(loadedDataBase.get(csp).get(recordName), attributeName);
                    datasetHeaders.get(csp).put(attributeName, protectedAttributeName);
                    splitDataset.get(csp).put(protectedAttributeName, dataset.get(attributeName));
                }
            } else if (Record.listAttrTypes.get(i).equals(Constants.identifier)
                    && Record.listDataTypes.get(i).equals(Constants.geometricObject)) {
                // ALL GEOMETRIC OBJECTS ARE SPLIT
                SplitPoint splitPoint = loadedSplitPoints.get(recordName);
                datasetHeaderSplitPoints.put(attributeNames[i], splitPoint);
                int x = splitPoint.getX();
                int y = splitPoint.getY();
                String protectedAttributeNameX = AttributeNamesUtilities
                        .resolveProtectedAttributeName(loadedDataBase.get(x).get(recordName), attributeName);
                String protectedAttributeNameY = AttributeNamesUtilities
                        .resolveProtectedAttributeName(loadedDataBase.get(y).get(recordName), attributeName);
                datasetHeaders.get(x).put(attributeName, protectedAttributeNameX);
                datasetHeaders.get(y).put(attributeName, protectedAttributeNameY);
                splitDataset.get(x).put(protectedAttributeNameX, splitGeomColumn(dataset.get(attributeName), "x"));
                splitDataset.get(y).put(protectedAttributeNameY, splitGeomColumn(dataset.get(attributeName), "y"));
            } else {
                // THE REST ARE SPLIT ACCORDING TO EXISTING MAPPING
                for (int csp = 0; csp < clouds; csp++) {
                    if (loadedDataBase.get(csp).containsKey(recordName)) {
                        String protectedAttributeName = AttributeNamesUtilities
                                .resolveProtectedAttributeName(loadedDataBase.get(csp).get(recordName), attributeName);
                        datasetHeaders.get(csp).put(attributeName, protectedAttributeName);
                        splitDataset.get(csp).put(protectedAttributeName, dataset.get(attributeName));
                    }
                }
            }
        }

        // Build post Commands
        // AKKA fix: rely on Record to split data
        for (int i = 0; i < clouds; i++) {
            Map<String, String> mapping = datasetHeaders.get(i);
            String[] protectedAttributeNames = mapping.values().toArray(new String[mapping.size()]);
            String[][] protectedContent = datasetByRows(protectedAttributeNames, splitDataset.get(i));
            commands.add(new SplitModuleCommand(attributeNames, protectedAttributeNames, mapping,
                    datasetHeaderSplitPoints, content, protectedContent));
        }

        return commands;
    }

    @Override
    public List<DataOperationCommand> put(String[] attributeNames, Criteria[] criterias, String[][] content) {
        // AKKA fix: reorder policy definition according to the request
        // attributes
        Functions.reOrderListsAccordingAttributeParameter(attributeNames);

        // Load database
        List<Map<String, String>> loadedDataBase = storage.retrieveMetadata(dataID);
        Map<String, SplitPoint> points = storage.retrieveSplitPoints(dataID);

        List<DataOperationCommand> commands = new ArrayList<>();
        int clouds = loadedDataBase.size();

        // Parse dataset into column form
        Map<String, String[]> dataset = datasetByColumns(attributeNames, content);

        // Initialize split dataset
        List<Map<String, String[]>> splitDataset = new ArrayList<>();
        // AKKA fix: Initialize split dataset header
        List<Map<String, String>> datasetHeaders = new ArrayList<>();
        Map<String, SplitPoint> datasetHeaderSplitPoints = new HashMap<>();
        for (int i = 0; i < clouds; i++) {
            splitDataset.add(new HashMap<>());
            // AKKA fix: Initialize split dataset header
            datasetHeaders.add(new HashMap<>());
        }

        // AKKA fix: postpone processing criteria (see below)

        // AKKA fix: rely on Record to split data
        for (int i = 0; i < Record.numAttr; i++) {
            String attributeName = attributeNames[i];
            String recordName = Record.listNames.get(i);
            if (Record.listAttrTypes.get(i).equals(Constants.technicalIdentifier)) {
                // PRIMARY KEYS ARE REPLICATED IN ALL CLOUDS
                for (int csp = 0; csp < clouds; csp++) {
                    String protectedAttributeName = AttributeNamesUtilities
                            .resolveProtectedAttributeName(loadedDataBase.get(csp).get(recordName), attributeName);
                    datasetHeaders.get(csp).put(attributeName, protectedAttributeName);
                    splitDataset.get(csp).put(protectedAttributeName, dataset.get(attributeName));
                }
            } else if (Record.listAttrTypes.get(i).equals(Constants.identifier)
                    && Record.listDataTypes.get(i).equals(Constants.geometricObject)) {
                // ALL GEOMETRIC OBJECTS ARE SPLIT
                SplitPoint splitPoint = points.get(recordName);
                datasetHeaderSplitPoints.put(attributeNames[i], splitPoint);
                int x = splitPoint.getX();
                int y = splitPoint.getY();
                String protectedAttributeNameX = AttributeNamesUtilities
                        .resolveProtectedAttributeName(loadedDataBase.get(x).get(recordName), attributeName);
                String protectedAttributeNameY = AttributeNamesUtilities
                        .resolveProtectedAttributeName(loadedDataBase.get(y).get(recordName), attributeName);
                datasetHeaders.get(x).put(attributeName, protectedAttributeNameX);
                datasetHeaders.get(y).put(attributeName, protectedAttributeNameY);
                splitDataset.get(x).put(protectedAttributeNameX, splitGeomColumn(dataset.get(attributeName), "x"));
                splitDataset.get(y).put(protectedAttributeNameY, splitGeomColumn(dataset.get(attributeName), "y"));
            } else {
                // THE REST ARE SPLIT ACCORDING TO EXISTING MAPPING
                for (int csp = 0; csp < clouds; csp++) {
                    if (loadedDataBase.get(csp).containsKey(recordName)) {
                        String protectedAttributeName = AttributeNamesUtilities
                                .resolveProtectedAttributeName(loadedDataBase.get(csp).get(recordName), attributeName);
                        datasetHeaders.get(csp).put(attributeName, protectedAttributeName);
                        splitDataset.get(csp).put(protectedAttributeName, dataset.get(attributeName));
                    }
                }
            }
        }

        // AKKA fix: split criteria among clouds
        List<Criteria[]> splitCriteria = null;
        if (criterias != null && criterias.length > 0) {
            List<Map<String, String>> datasetCriterias = new ArrayList<>();
            Map<String, SplitPoint> datasetCriteriaSplitPoints = new HashMap<>();
            for (int i = 0; i < clouds; i++) {
                datasetCriterias.add(new HashMap<>());
            }
            String[] criteriaAttributeNames = Arrays.stream(criterias).map(Criteria::getAttributeName)
                    .toArray(String[]::new);
            Functions.reOrderListsAccordingAttributeParameter(criteriaAttributeNames);
            for (int i = 0; i < Record.numAttr; i++) {
                String criteriaAttributeName = criteriaAttributeNames[i];
                String recordName = Record.listNames.get(i);
                if (Record.listAttrTypes.get(i).equals(Constants.identifier)
                        && Record.listDataTypes.get(i).equals(Constants.geometricObject)) {
                    // ALL GEOMETRIC OBJECTS ARE SPLIT
                    SplitPoint splitPoint = points.get(recordName);
                    datasetCriteriaSplitPoints.put(criteriaAttributeName, splitPoint);
                    int x = splitPoint.getX();
                    int y = splitPoint.getY();
                    String protectedAttributeNameX = AttributeNamesUtilities.resolveProtectedAttributeName(
                            loadedDataBase.get(x).get(recordName), criteriaAttributeName);
                    String protectedAttributeNameY = AttributeNamesUtilities.resolveProtectedAttributeName(
                            loadedDataBase.get(y).get(recordName), criteriaAttributeName);
                    datasetCriterias.get(x).put(criteriaAttributeName, protectedAttributeNameX);
                    datasetCriterias.get(y).put(criteriaAttributeName, protectedAttributeNameY);
                } else {
                    // THE REST ARE SPLIT ACCORDING TO EXISTING MAPPING
                    for (int csp = 0; csp < clouds; csp++) {
                        if (loadedDataBase.get(csp).containsKey(recordName)) {
                            String protectedAttributeName = AttributeNamesUtilities.resolveProtectedAttributeName(
                                    loadedDataBase.get(csp).get(recordName), criteriaAttributeName);
                            datasetCriterias.get(csp).put(criteriaAttributeName, protectedAttributeName);
                        }
                    }
                }
            }
            splitCriteria = splitCriteria(criterias, datasetCriterias, datasetCriteriaSplitPoints);
        }

        // AKKA fix: rely on Record to split data
        for (int i = 0; i < clouds; i++) {
            Map<String, String> mapping = datasetHeaders.get(i);
            String[] protectedAttributeNames = mapping.values().toArray(new String[mapping.size()]);
            String[][] protectedContent = datasetByRows(protectedAttributeNames, splitDataset.get(i));
            SplitModuleCommand command = new SplitModuleCommand(attributeNames, protectedAttributeNames, mapping,
                    datasetHeaderSplitPoints, content, protectedContent);
            if (splitCriteria != null) {
                command.setCriteria(splitCriteria.get(i));
                command.setOriginalCriteria(criterias);
            }
            commands.add(command);
        }

        return commands;
    }

    @Override
    public List<DataOperationCommand> delete(String[] attributeNames, Criteria[] criterias) {
        // AKKA fix: reorder policy definition according to the request
        // attributes
        Functions.reOrderListsAccordingAttributeParameter(attributeNames);

        // Load database
        List<Map<String, String>> loadedDataBase = storage.retrieveMetadata(dataID);
        Map<String, SplitPoint> points = storage.retrieveSplitPoints(dataID);

        List<DataOperationCommand> commands = new ArrayList<>();
        int clouds = loadedDataBase.size();

        // AKKA fix: Initialize dataset headers
        List<Map<String, String>> datasetHeaders = new ArrayList<>();
        Map<String, SplitPoint> datasetHeaderSplitPoints = new HashMap<>();
        for (int i = 0; i < clouds; i++) {
            datasetHeaders.add(new HashMap<>());
        }

        // AKKA fix: postpone processing criteria (see below)

        // AKKA fix: rely on Record to delete data
        for (int i = 0; i < Record.numAttr; i++) {
            String attributeName = attributeNames[i];
            String recordName = Record.listNames.get(i);
            if (Record.listAttrTypes.get(i).equals(Constants.identifier)
                    && Record.listDataTypes.get(i).equals(Constants.geometricObject)) {
                // ALL GEOMETRIC OBJECTS ARE SPLIT
                SplitPoint splitPoint = points.get(recordName);
                datasetHeaderSplitPoints.put(attributeNames[i], splitPoint);
                int x = splitPoint.getX();
                int y = splitPoint.getY();
                String protectedAttributeNameX = AttributeNamesUtilities
                        .resolveProtectedAttributeName(loadedDataBase.get(x).get(recordName), attributeName);
                String protectedAttributeNameY = AttributeNamesUtilities
                        .resolveProtectedAttributeName(loadedDataBase.get(y).get(recordName), attributeName);
                datasetHeaders.get(x).put(attributeName, protectedAttributeNameX);
                datasetHeaders.get(y).put(attributeName, protectedAttributeNameY);
            } else {
                // THE REST ARE SPLIT ACCORDING TO EXISTING MAPPING
                for (int csp = 0; csp < clouds; csp++) {
                    if (loadedDataBase.get(csp).containsKey(recordName)) {
                        String protectedAttributeName = AttributeNamesUtilities
                                .resolveProtectedAttributeName(loadedDataBase.get(csp).get(recordName), attributeName);
                        datasetHeaders.get(csp).put(attributeName, protectedAttributeName);
                    }
                }
            }
        }

        // AKKA fix: split criteria among clouds
        List<Criteria[]> splitCriteria = null;
        if (criterias != null && criterias.length > 0) {
            List<Map<String, String>> datasetCriterias = new ArrayList<>();
            Map<String, SplitPoint> datasetCriteriaSplitPoints = new HashMap<>();
            for (int i = 0; i < clouds; i++) {
                datasetCriterias.add(new HashMap<>());
            }
            String[] criteriaAttributeNames = Arrays.stream(criterias).map(Criteria::getAttributeName)
                    .toArray(String[]::new);
            Functions.reOrderListsAccordingAttributeParameter(criteriaAttributeNames);
            for (int i = 0; i < Record.numAttr; i++) {
                String criteriaAttributeName = criteriaAttributeNames[i];
                String recordName = Record.listNames.get(i);
                if (Record.listAttrTypes.get(i).equals(Constants.identifier)
                        && Record.listDataTypes.get(i).equals(Constants.geometricObject)) {
                    // ALL GEOMETRIC OBJECTS ARE SPLIT
                    SplitPoint splitPoint = points.get(recordName);
                    datasetCriteriaSplitPoints.put(criteriaAttributeName, splitPoint);
                    int x = splitPoint.getX();
                    int y = splitPoint.getY();
                    String protectedAttributeNameX = AttributeNamesUtilities.resolveProtectedAttributeName(
                            loadedDataBase.get(x).get(recordName), criteriaAttributeName);
                    String protectedAttributeNameY = AttributeNamesUtilities.resolveProtectedAttributeName(
                            loadedDataBase.get(y).get(recordName), criteriaAttributeName);
                    datasetCriterias.get(x).put(criteriaAttributeName, protectedAttributeNameX);
                    datasetCriterias.get(y).put(criteriaAttributeName, protectedAttributeNameY);
                } else {
                    // THE REST ARE SPLIT ACCORDING TO EXISTING MAPPING
                    for (int csp = 0; csp < clouds; csp++) {
                        if (loadedDataBase.get(csp).containsKey(recordName)) {
                            String protectedAttributeName = AttributeNamesUtilities.resolveProtectedAttributeName(
                                    loadedDataBase.get(csp).get(recordName), criteriaAttributeName);
                            datasetCriterias.get(csp).put(criteriaAttributeName, protectedAttributeName);
                        }
                    }
                }
            }
            splitCriteria = splitCriteria(criterias, datasetCriterias, datasetCriteriaSplitPoints);
        }

        // AKKA fix: rely on Record to delete data
        for (int i = 0; i < clouds; i++) {
            Map<String, String> mapping = datasetHeaders.get(i);
            String[] protectedAttributeNames = mapping.values().toArray(new String[mapping.size()]);
            SplitModuleCommand command = new SplitModuleCommand(attributeNames, protectedAttributeNames, mapping,
                    datasetHeaderSplitPoints, null, null);
            if (splitCriteria != null) {
                command.setCriteria(splitCriteria.get(i));
                command.setOriginalCriteria(criterias);
            }
            commands.add(command);
        }

        return commands;
    }

    @Override
    public List<Map<String, String>> head(String[] strings) {
        // AKKA fix: not so simple... we need the head function returns the
        // mapping for the input attribute names
        // Create mapping for each CSP

        // Load database
        List<Map<String, String>> loadedDataBase = storage.retrieveMetadata(dataID);
        Map<String, SplitPoint> points = storage.retrieveSplitPoints(dataID);

        List<Map<String, String>> result = Stream.<Map<String, String>>generate(() -> new HashMap<>())
                .limit(loadedDataBase.size()).collect(Collectors.toList());
        // resolve the fully qualified attribute names (replace any asterisk by
        // the matching attribute)
        String[] fqAttributeNames = AttributeNamesUtilities.resolveOperationAttributeNames(strings,
                Record.refListNames);
        // for each fully qualified attribute name
        for (String attributeName : fqAttributeNames) {
            // resolve the attribute protection (defined in the policy)
            String refAttributeName = IntStream.range(0, Record.refNumAttr)
                    .filter(i -> Record.refListNamePatterns.get(i).matcher(attributeName).matches())
                    .mapToObj(i -> Record.refListNames.get(i)).findFirst().orElse(null);
            if (refAttributeName != null) {
                // save mapping for each involved CSP
                for (int csp = 0; csp < loadedDataBase.size(); csp++) {
                    Map<String, String> mapping = result.get(csp);
                    // get the protected attribute name
                    String protectedAttributeName = loadedDataBase.get(csp).get(refAttributeName);
                    if (protectedAttributeName != null) {
                        // remove asterisks in the protected attribute name
                        protectedAttributeName = AttributeNamesUtilities
                                .resolveProtectedAttributeName(protectedAttributeName, attributeName);
                        mapping.put(attributeName, protectedAttributeName);
                    }
                }
            }
        }
        return result;
    }

    private List<DataOperationCommand> krigingFirst(String attributeName, String geoAttributeName, String point) {
        // Load database
        List<Map<String, String>> loadedDataBase = storage.retrieveMetadata(dataID);
        Map<String, SplitPoint> points = storage.retrieveSplitPoints(dataID);

        int clouds = loadedDataBase.size();
        List<DataOperationCommand> commands = new ArrayList<>();
        for (int i = 0; i < clouds; i++) {
            commands.add(null);
        }

        System.out.println(points.keySet().toString());

        // Bring in the X coordinate
        // where is X coordinate?
        int x = points.get(geoAttributeName).getX();
        // what's its name?
        String xAttributeName = loadedDataBase.get(x).get(geoAttributeName);
        KrigingModuleCommand kmc = new KrigingModuleCommand(new String[] { geoAttributeName },
                new String[] { xAttributeName }, loadedDataBase.get(x), points, attributeName, geoAttributeName, point);

        commands.set(x, kmc);

        // Bring in the Y coordinate
        // where is Y coordinate?
        int y = points.get(geoAttributeName).getY();
        // what's its name
        String yAttributeName = loadedDataBase.get(y).get(geoAttributeName);
        kmc = new KrigingModuleCommand(new String[] { geoAttributeName }, new String[] { yAttributeName },
                loadedDataBase.get(y), points, attributeName, geoAttributeName, point);

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
            ((KrigingModuleCommand) commands.get(x)).addAttributeName(attributeName);
            ((KrigingModuleCommand) commands.get(x)).addProtectedAttributeName(measureAttributeName);
        } else if (y == measureCloud) {
            ((KrigingModuleCommand) commands.get(y)).addAttributeName(attributeName);
            ((KrigingModuleCommand) commands.get(y)).addProtectedAttributeName(measureAttributeName);
        } else {
            commands.set(measureCloud, new SplitModuleCommand(new String[] { attributeName },
                    new String[] { measureAttributeName }, loadedDataBase.get(measureCloud), points, null, null));
        }

        return commands;
    }

    private List<DataOperationResult> kriging(List<DataOperationCommand> commands, List<String[][]> contents) {
        // Load database
        List<Map<String, String>> loadedDataBase = storage.retrieveMetadata(dataID);
        Map<String, SplitPoint> points = storage.retrieveSplitPoints(dataID);

        int clouds = loadedDataBase.size();
        List<DataOperationResult> results = new ArrayList<>();
        for (int i = 0; i < clouds; i++) {
            results.add(null);
        }

        // One of the KMC will be used for next calls
        KrigingModuleCommand kmc = null;
        for (DataOperationCommand c : commands) {
            if (c != null && c instanceof KrigingModuleCommand) {
                kmc = (KrigingModuleCommand) c;
                break;
            }
        }

        // We should never get here
        if (kmc == null) {
            return null;
        }

        int x = points.get(kmc.getGeomAttribute()).getX();
        int y = points.get(kmc.getGeomAttribute()).getY();

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
            kmc.setProtectedAttributeNames(new String[] {
                    Constants.krigingCalculateX + "(" + loadedDataBase.get(x).get(kmc.getGeomAttribute()) + ")" });
            results.set(x, kmc);
            break;
        case 2:
            kmc.nextStep();
            String[] calculateX = transpose(contents.get(x))[0];
            kmc.setProtectedAttributeNames(
                    new String[] { Constants.krigingCalculateY + "(" + loadedDataBase.get(y).get(kmc.getGeomAttribute())
                            + ", {" + String.join(",", calculateX) + "})" });
            results.set(y, kmc);
            break;
        case 3:
            String[] calculatedOnCloud = transpose(contents.get(y))[0];
            kmc.setCalculatedOnCloud(calculatedOnCloud);
            String[][] k = kmc.calculateKriging();
            results = new ArrayList<>();
            results.add(new KrigingModuleResponse(new String[] { "value", "variance" }, k));
            break;
        }

        return results;
    }

    private List<String[][]> filterContentsByPrimaryKey(String keyAttribute, List<DataOperationCommand> promise,
            List<String[][]> contents) {

        // Index table of results by primary key
        final List<Map<String, String[]>> indexedTables = new ArrayList<>();
        for (int i = 0; i < promise.size(); i++) {
            String cloudKeyAttribute = promise.get(i).getMapping().get(keyAttribute);
            int column;
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
        Set<String> ids = indexedTables.stream().map(Map::keySet).flatMap(Set::stream).collect(Collectors.toSet());
        Map<String, Boolean> inAll = ids.stream().collect(Collectors.toMap(s -> s,
                s -> indexedTables.stream().map(m -> m.containsKey(s)).reduce(true, (a, b) -> a && b)));
        for (Map<String, String[]> m : indexedTables) {
            for (String id : inAll.keySet()) {
                if (!inAll.get(id))
                    m.remove(id);
            }
        }

        // Reconstruct filtered results
        List<String[][]> contentsIntersection = new ArrayList<>();
        for (Map<String, String[]> m : indexedTables) {
            String[][] t = m.entrySet().stream().sorted(Comparator.comparing(e -> Integer.valueOf(e.getKey())))
                    .map(Map.Entry::getValue).toArray(String[][]::new);
            contentsIntersection.add(t);
        }
        return contentsIntersection;
    }

    private String[][] filter(String[] attributeNames, String[][] contents, Criteria[] criteria) {
        if (criteria == null) {
            return contents;
        } else {
            for (Criteria c : criteria) {
                int pos;
                if ((pos = haveAttribute(attributeNames, c.getAttributeName())) != -1) {
                    contents = Arrays.stream(contents).filter(getPredicate(c, pos)).toArray(String[][]::new);
                }
            }

        }
        return contents;
    }

    private int haveAttribute(String[] attributes, String attributeName) {
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

    private List<DataOperationCommand> genericOutboundGET(String[] attributeNames, List<List<String>> protAttr,
            List<Map<String, String>> loadedDataBase, Map<String, SplitPoint> points, Criteria[] originalCriteria,
            List<Criteria[]> criteria) {
        List<DataOperationCommand> commands = new ArrayList<>();
        int clouds = loadedDataBase.size();

        for (int i = 0; i < clouds; i++) {
            String[] protAttrNames = new String[protAttr.get(i).size()];
            protAttrNames = protAttr.get(i).toArray(protAttrNames);
            SplitModuleCommand command = new SplitModuleCommand(attributeNames, protAttrNames, loadedDataBase.get(i),
                    points, null, null);
            if (criteria != null) {
                command.setCriteria(criteria.get(i));
                command.setOriginalCriteria(originalCriteria);
            }
            commands.add(command);
        }
        return commands;
    }

    private List<Criteria[]> splitCriteria(Criteria[] criteria, List<Map<String, String>> loadedDatabase,
            Map<String, SplitPoint> points) {
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
                        splitCriteria.get(i).add(new Criteria(loadedDatabase.get(i).get(c.getAttributeName()),
                                c.getOperator(), c.getValue()));
                    }
                }
            }
        }

        return splitCriteria.stream().map(list -> list.toArray(new Criteria[list.size()])).collect(Collectors.toList());
    }

    private List<Criteria> splitAreaCriteria(Criteria criteria, List<Map<String, String>> loadedDataBase,
            Map<String, SplitPoint> points) {
        List<Criteria> criterias = new ArrayList<>();

        for (int i = 0; i < loadedDataBase.size(); i++) {
            criterias.add(new Criteria(null, null, null));
        }
        int x = points.get(criteria.getAttributeName()).getX();
        int y = points.get(criteria.getAttributeName()).getY();

        String[] area = criteria.getValue().split(",");

        // AKKA fix: don't forget SRID
        double minX, maxX, minY, maxY;
        int srid = Integer.parseInt(area[4].trim());
        if (srid != 0) {
            //TODO 3857 -> srid
            ProjectedCRS crs = ProjectedCRS.resolve(3857);
            minX = crs.getAxis("x").getMin();
            maxX = crs.getAxis("x").getMax();
            minY = crs.getAxis("y").getMin();
            maxY = crs.getAxis("y").getMax();
        } else {
            minX = -Double.MAX_VALUE;
            maxX = Double.MAX_VALUE;
            minY = -Double.MAX_VALUE;
            maxY = Double.MAX_VALUE;
        }
        String[] areaX = { area[0], Double.toString(minY), area[2], Double.toString(maxY), Integer.toString(srid) };
        criterias.get(x).setAttributeName(loadedDataBase.get(x).get(criteria.getAttributeName()));
        criterias.get(x).setOperator(criteria.getOperator());
        criterias.get(x).setValue(String.join(",", areaX));

        // AKKA fix: don't forget SRID
        String[] areaY = { Double.toString(minX), area[1], Double.toString(maxX), area[3], Integer.toString(srid) };
        criterias.get(y).setAttributeName(loadedDataBase.get(y).get(criteria.getAttributeName()));
        criterias.get(y).setOperator(criteria.getOperator());
        criterias.get(y).setValue(String.join(",", areaY));

        return criterias;
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
        // AKKA fix: content may be empty for a CSP
        String[][] transposedContent = new String[content.length > 0 ? content[0].length : 0][content.length];
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
            } catch (Exception e) {
                /* Sorry */}
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
            } catch (Exception e) {
                /* Sorry */}
        }
        return decryptedColumn;
    }

    private String[] splitGeomColumn(String[] column, String coordinate) {
        switch (coordinate) {
        case "x":
            return Arrays.stream(column).map(this::separateX).toArray(String[]::new);
        case "y":
            return Arrays.stream(column).map(this::separateY).toArray(String[]::new);
        default:
            return null;
        }
    }

    private String[] joinGeomColumn(String[] x, String[] y) {
        int len = x.length > y.length ? x.length : y.length;
        return IntStream.range(0, len).mapToObj(i -> joinCoords(i < x.length ? x[i] : null, i < y.length ? y[i] : null))
                .toArray(String[]::new);
    }

    private boolean inArea(String geomStr, double[] boundary) {
        double x = 0;
        double y = 0;

        GeometryBuilder builder = new GeometryBuilder();
        Geometry geom = builder.decodeGeometry(geomStr);
        if (geom != null && geom.numPoints() > 0) {
            x = geom.getFirstPoint().x;
            y = geom.getFirstPoint().y;
        }

        boolean inX = x >= boundary[0] && x <= boundary[2];
        boolean inY = y >= boundary[1] && y <= boundary[3];
        return inX && inY;
    }

    // Returns a String that has a correct X value and a random Y
    private String separateX(String geomStr) {
        GeometryBuilder builder = new GeometryBuilder();
        Object geom = builder.decode(geomStr);
        int srid = 0;
        if (geom instanceof Point) {
            Point point = (Point) geom;
            srid = point.getSrid();
        } else if (geom instanceof PGbox2d) {
            PGbox2d box = (PGbox2d) geom;
            srid = box.getLLB().getSrid();
        }
        double minY, maxY;
        if (srid != 0) {
            //TODO 3857 -> srid
            ProjectedCRS crs = ProjectedCRS.resolve(3857);
            minY = crs.getAxis("y").getMin();
            maxY = crs.getAxis("y").getMax();
        } else {
            minY = -Double.MAX_VALUE;
            maxY = Double.MAX_VALUE;
        }
        if (geom instanceof Point) {
            Point point = (Point) geom;
            if (Record.splittingType.equals("points")) {
                Random rnd = new Random();
                point.setY((rnd.nextDouble() * (maxY - minY)) - maxY);
                geomStr = builder.encode(point);
            } else if (Record.splittingType.equals("lines")) {
                Point min = new Point(point.getX(), minY);
                min.setSrid(srid);
                Point max = new Point(point.getX(), maxY);
                max.setSrid(srid);
                LineString line = new LineString(new Point[] { min, max });
                line.setSrid(srid);
                geomStr = builder.encode(line);
            }
        } else if (geom instanceof PGbox2d) {
            PGbox2d box = (PGbox2d) geom;
            box.getLLB().setY(minY);
            box.getURT().setY(maxY);
            geomStr = builder.encode(box);
        }

        return geomStr;
    }

    // Returns a String that has a correct Y value and a random X
    private String separateY(String geomStr) {
        GeometryBuilder builder = new GeometryBuilder();
        Object geom = builder.decode(geomStr);
        int srid = 0;
        if (geom instanceof Point) {
            Point point = (Point) geom;
            srid = point.getSrid();
        } else if (geom instanceof PGbox2d) {
            PGbox2d box = (PGbox2d) geom;
            srid = box.getLLB().getSrid();
        }
        double minX, maxX;
        if (srid != 0) {
            //TODO 3857 -> srid
            ProjectedCRS crs = ProjectedCRS.resolve(3857);
            minX = crs.getAxis("x").getMin();
            maxX = crs.getAxis("x").getMax();
        } else {
            minX = -Double.MAX_VALUE;
            maxX = Double.MAX_VALUE;
        }
        if (geom instanceof Point) {
            Point point = (Point) geom;
            if (Record.splittingType.equals("points")) {
                Random rnd = new Random();
                point.setX((rnd.nextDouble() * (maxX - minX)) - maxX);
                geomStr = builder.encode(point);
            } else if (Record.splittingType.equals("lines")) {
                Point min = new Point(minX, point.getY());
                min.setSrid(srid);
                Point max = new Point(maxX, point.getY());
                max.setSrid(srid);
                LineString line = new LineString(new Point[] { min, max });
                line.setSrid(srid);
                geomStr = builder.encode(line);
            }
        } else if (geom instanceof PGbox2d) {
            PGbox2d box = (PGbox2d) geom;
            box.getLLB().setX(minX);
            box.getURT().setX(maxX);
            geomStr = builder.encode(box);
        }

        return geomStr;
    }

    // Joins two String coordinates
    private String joinCoords(String geomStrX, String geomStrY) {
        // get coordinates for X
        GeometryBuilder builderX = null;
        Object geomX = null;
        if (geomStrX != null) {
            builderX = new GeometryBuilder();
            geomX = builderX.decode(geomStrX);
        }

        // get coordinates for Y
        GeometryBuilder builderY = null;
        Object geomY = null;
        if (geomStrY != null) {
            builderY = new GeometryBuilder();
            geomY = builderY.decode(geomStrY);
        }

        String geomStr;
        if (Record.splittingType.equals("points") && geomX instanceof Point && geomY instanceof Point) {
            // merge coordinates and build a new point
            Point pointX = (Point) geomX;
            Point pointY = (Point) geomY;
            Point newPoint = new Point(pointX.getX(), pointY.getY());
            newPoint.setSrid(pointX.getSrid());
            geomStr = builderX.encodeGeometry(newPoint);
        } else if (Record.splittingType.equals("lines") && geomX instanceof LineString && geomY instanceof LineString) {
            // merge coordinates and build a new point
            LineString lineX = (LineString) geomX;
            LineString lineY = (LineString) geomY;
            Point newPoint = new Point(lineX.getFirstPoint().getX(), lineY.getFirstPoint().getY());
            newPoint.setSrid(lineX.getSrid());
            geomStr = builderX.encodeGeometry(newPoint);
        } else if (geomX instanceof PGbox2d && geomY instanceof PGbox2d) {
            // merge coordinates and build a new box2d
            PGbox2d boxX = (PGbox2d) geomX;
            PGbox2d boxY = (PGbox2d) geomY;
            Point newLLB = new Point(boxX.getLLB().getX(), boxY.getLLB().getY());
            newLLB.setSrid(boxX.getLLB().getSrid());
            Point newURT = new Point(boxX.getURT().getX(), boxY.getURT().getY());
            newURT.setSrid(boxX.getURT().getSrid());
            PGbox2d newPGbox2d = new PGbox2d(newLLB, newURT);
            geomStr = builderX.encodePGboxbase(newPGbox2d);
        } else if (geomStrX != null) {
            geomStr = geomStrX;
        } else if (geomStrY != null) {
            geomStr = geomStrY;
        } else {
            geomStr = null;
        }

        return geomStr;
    }

    private String buildProtectedAttributeName(String attributeName, int cloud) {
        // AKKA fix: preserve first part
        return /*"csp" + (cloud + 1) + "/" +*/ attributeName;
    }
}
