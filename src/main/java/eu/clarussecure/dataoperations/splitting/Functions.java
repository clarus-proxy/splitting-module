package eu.clarussecure.dataoperations.splitting;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import com.vividsolutions.jts.geom.*;
import org.geotools.geometry.jts.GeometryBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import eu.clarussecure.dataoperations.AttributeNamesUtilities;

public class Functions {

    public static String[][][] anonymize(String[] attributes, String[][] content) {
        String[][][] dataAnom = null;

        reOrderListsAccordingAttributeParameter(attributes);

        if (Record.attrTypes.get(Constants.identifier).equalsIgnoreCase(Constants.splitting)) {
            dataAnom = splitting(content);
            return dataAnom;
        }

        return dataAnom;
    }

    public static String[][] retrieve(String attributeNames[], String[][][] strings) {
        String[][] plainData;
        PrecisionModel pmodel = new PrecisionModel(); // No podem especificar un
                                                      // SRID al GeometryFactory
                                                      // sense passarli un
                                                      // PrecisionModel
        GeometryFactory builder = new GeometryFactory(pmodel, 4326); // GeometryFactory
                                                                     // crea
                                                                     // objectes
                                                                     // geometrics
                                                                     // de gis
        WKBReader reader = new WKBReader(); // Parseja objectes en format WKB
                                            // (Well Known Binary)
        WKBWriter writer = new WKBWriter(2, 2, true); // Converteix objectes de
                                                      // GeoTools
        Geometry geom; // Objecte geometric basic
        int posGeom;
        String attrType, dataType, geomStr, valueX, valueY;
        Coordinate coordX, coordY, newCoord;

        System.out.println("Retrieving...");
        // attributeNames = promise.getAttributeNames();
        reOrderListsAccordingAttributeParameter(attributeNames);

        posGeom = 0;
        for (int i = 0; i < Record.numAttr; i++) { // geometric_object position
            attrType = Record.listAttrTypes.get(i);
            if (attrType.equalsIgnoreCase(Constants.identifier)) {
                dataType = Record.listDataTypes.get(i);
                if (dataType.equalsIgnoreCase(Constants.geometricObject)) {
                    posGeom = i;
                    break;
                }
            }
        }

        plainData = strings[0];
        for (int i = 0; i < plainData.length; i++) {
            try {
                valueX = strings[0][i][posGeom];
                geom = reader.read(WKBReader.hexToBytes(valueX));
                coordX = geom.getCoordinate();
                valueY = strings[1][i][posGeom];
                geom = reader.read(WKBReader.hexToBytes(valueY));
                coordY = geom.getCoordinate();
                newCoord = new Coordinate(coordX.x, coordY.y);
                geom = builder.createPoint(newCoord);
                plainData[i][posGeom] = WKBWriter.toHex(writer.write(geom));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        return plainData;
    }

    public static void reOrderListsAccordingAttributeParameter(String[] attributes) {
        ArrayList<String> newListNames = new ArrayList<String>();
        // AKKA fix: use pattern for attribute matching
        ArrayList<Pattern> newListNamePatterns = new ArrayList<Pattern>();
        ArrayList<String> newListAttrTypes = new ArrayList<String>();
        ArrayList<String> newListDataTypes = new ArrayList<String>();
        String attr, name;
        boolean ok;

        for (int i = 0; i < attributes.length; i++) {
            attr = attributes[i];
            ok = false;
            // AKKA fix: take refListNames, refListNamePatterns, refListAttrTypes and refListDataTypes as reference
            for (int j = 0; j < Record.refListNames.size(); j++) {
                name = Record.refListNames.get(j);
                Pattern pattern = Record.refListNamePatterns.get(j);
                if (pattern.matcher(attr).matches()) {
                    newListNames.add(name);
                    newListNamePatterns.add(Record.refListNamePatterns.get(j));
                    newListAttrTypes.add(Record.refListAttrTypes.get(j));
                    newListDataTypes.add(Record.refListDataTypes.get(j));
                    ok = true;
                    break;
                }
            }
            if (!ok) { // this attribute does not appear in the security policy
                newListNames.add(attr); // it is added as categorical
                                        // non_confidential

                // AKKA fix: use pattern for attribute matching
                newListNamePatterns.add(Pattern.compile(AttributeNamesUtilities.escapeRegex(attr)));
                newListAttrTypes.add(Constants.non_confidential);
                newListDataTypes.add(Constants.categoric);
            }
        }
        Record.listNames = newListNames;
        // AKKA fix: use pattern for attribute matching
        Record.listNamePatterns = newListNamePatterns;
        Record.listAttrTypes = newListAttrTypes;
        Record.listDataTypes = newListDataTypes;
        Record.numAttr = newListNames.size();

    }

    /**
     * This function applies splitting to a dataset
     *
     * @param dataOri,
     *            the dataset
     * @return two anonymized versions of the dataset
     */
    public static String[][][] splitting(String[][] dataOri) {
        // devolver lista de hashmaps<Cabecera, atributo (lista de atributos)>
        ArrayList<Record> data;
        ArrayList<ArrayList<Record>> dataAnom;
        String[][][] dataAnomStr;

        data = createRecords(dataOri);
        dataAnom = splitting(data);
        dataAnomStr = createMatrixStringFromRecords(dataAnom);

        return dataAnomStr;
    }

    public static ArrayList<ArrayList<Record>> splitting(ArrayList<Record> dataOri) {
        ArrayList<ArrayList<Record>> dataAnom = new ArrayList<ArrayList<Record>>();
        ArrayList<String> geometricObjects = new ArrayList<String>();
        ArrayList<String> geometricObjectsX = new ArrayList<String>();
        ArrayList<String> geometricObjectsY = new ArrayList<String>();
        PrecisionModel pmodel = new PrecisionModel(); // No podem especificar un
                                                      // SRID al GeometryFactory
                                                      // sense passarli un
                                                      // PrecisionModel
        GeometryFactory builder = new GeometryFactory(pmodel, 4326); // GeometryFactory
                                                                     // crea
                                                                     // objectes
                                                                     // geometrics
                                                                     // de gis
        WKBReader reader = new WKBReader(); // Parseja objectes en format WKB
                                            // (Well Known Binary)
        WKBWriter writer = new WKBWriter(2, 2, true); // Converteix objectes de
                                                      // GeoTools
        Geometry geom; // Objecte geometric basic
        int posGeom;
        String attrType, dataType, geomStr, value;
        Record record, recordX, recordY;
        Random rnd = new Random();
        Coordinate coordX, coordY;

        System.out.println("Splitting...");
        posGeom = 0;
        for (int i = 0; i < Record.numAttr; i++) { // geometric_object position
            attrType = Record.listAttrTypes.get(i);
            if (attrType.equalsIgnoreCase(Constants.identifier)) {
                dataType = Record.listDataTypes.get(i);
                if (dataType.equalsIgnoreCase(Constants.geometricObject)) {
                    posGeom = i;
                    break;
                }
            }
        }

        for (Record reg : dataOri) {
            geomStr = reg.attrValues[posGeom];
            geometricObjects.add(geomStr);
        }

        for (String s : geometricObjects) {
            try {
                geom = reader.read(WKBReader.hexToBytes(s));
                geom.getCoordinate().y = rnd.nextInt(180) - 90;
                // coordX = new Coordinate(geom.getCoordinate().x,
                // rnd.nextInt(180) - 90, geom.getCoordinate().z);
                // coordY = new Coordinate(rnd.nextInt(180) - 90,
                // geom.getCoordinate().y, geom.getCoordinate().z);
                // geom = builder.createPoint(coordX);
                geometricObjectsX.add(WKBWriter.toHex(writer.write(geom)));
                geom = reader.read(WKBReader.hexToBytes(s));
                geom.getCoordinate().x = rnd.nextInt(180) - 90;
                // geom = builder.createPoint(coordY);
                geometricObjectsY.add(WKBWriter.toHex(writer.write(geom)));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        dataAnom.add(new ArrayList<Record>()); // X
        dataAnom.add(new ArrayList<Record>()); // Y
        for (int i = 0; i < dataOri.size(); i++) {
            record = dataOri.get(i);
            recordX = new Record(record.id);
            recordY = new Record(record.id);
            for (int j = 0; j < Record.numAttr; j++) {
                value = record.attrValues[j];
                if (j == posGeom) {
                    recordX.attrValues[j] = geometricObjectsX.get(i);
                    recordY.attrValues[j] = geometricObjectsY.get(i);
                } else {
                    recordY.attrValues[j] = value; // ojo encriptar
                                                   // confidenciales
                    recordX.attrValues[j] = value; // ojo encriptar
                                                   // confidenciales
                }
            }
            dataAnom.get(0).add(recordX);
            dataAnom.get(1).add(recordY);
        }
        System.out.println("done");

        return dataAnom;
    }

    public static void readProperties(String xml) {
        Document document;

        document = readDocument(xml);
        readProperties(document);
    }

    private static Document readDocumentFromFile(String fileProperties) {
        Document document = null;

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            document = db.parse(new File(fileProperties));
            document.getDocumentElement().normalize();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return document;
    }

    private static Document readDocument(String xml) {
        Document document = null;

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            InputSource is = new InputSource(new StringReader(xml));
            document = db.parse(is);
            document.getDocumentElement().normalize();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return document;
    }

    public static Document readDocument(byte[] xml) {
        Document document = null;

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            InputSource is = new InputSource(new StringReader(new String(xml)));
            document = db.parse(is);
            document.getDocumentElement().normalize();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return document;
    }

    public static void readProperties(Document document) {
        int numQuasis;

        // URV fix: removed header, attribute_separator and record_separator
        // URV fix: elements are retrieved by their name

        Record.attrTypes = getAttributeTypes(document);
        for (String s : Record.attrTypes.values()) {
            if (s.equalsIgnoreCase(Constants.kAnonymity)) {
                Record.k = Integer.parseInt(getK(document));
            }
            if (s.equalsIgnoreCase(Constants.tCloseness)) {
                Record.t = Double.parseDouble(getT(document));
            }
            if (s.equalsIgnoreCase(Constants.splitting)) {
                Record.clouds = Integer.parseInt(getClouds(document));
            }
            if (s.equalsIgnoreCase(Constants.encryption)) {
                Record.idKey = getIdKey(document);
            }
            if (s.equalsIgnoreCase(Constants.coarsening)) {
                Record.coarsening_type = getCoarseningType(document);
                if (Record.coarsening_type.equalsIgnoreCase(Constants.shift)) {
                    Record.radius = Double.parseDouble(getRadius(document));
                }
                if (Record.coarsening_type.equalsIgnoreCase(Constants.microaggregation)) {
                    Record.k = Integer.parseInt(getCoarseningK(document));
                }
            }
        }
        // AKKA fix: replace unqualified attribute name by a generic qualified
        // one (with asterisks):
        List<String> attributeNames = getAtributeNames(document);
        attributeNames = AttributeNamesUtilities.fullyQualified(attributeNames);
        List<Pattern> attributePatterns = attributeNames.stream().map(AttributeNamesUtilities::escapeRegex)
                .map(Pattern::compile).collect(Collectors.toList());
        // AKKA fix: keep original listNames, listNamePatterns and listAttrTypes
        Record.refListNames = Record.listNames = (ArrayList<String>) attributeNames;
        Record.refListNamePatterns = Record.listNamePatterns = (ArrayList<Pattern>) attributePatterns;
        Record.refListAttrTypes = Record.listAttrTypes = getAtributeTypes(document);
        numQuasis = 0;
        for (String s : Record.listAttrTypes) {
            if (s.equals(Constants.quasiIdentifier)) {
                numQuasis++;
            }
        }
        Record.numQuasi = numQuasis;
        if (Record.numQuasi == 0) {
            Record.attrTypes.put(Constants.quasiIdentifier, "null");
        }
        // AKKA fix: keep original listDataTypes and numAttr
        Record.refListDataTypes = Record.listDataTypes = getAttributeDataTypes(document);
        Record.refNumAttr = Record.numAttr = Record.listAttrTypes.size();
    }

    private static HashMap<String, String> getAttributeTypes(Document document) {
        HashMap<String, String> attrTypes = new HashMap<String, String>();
        Node node;
        NamedNodeMap attributes;
        String type, protection;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attributeType);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.type);
            type = node.getNodeValue();
            node = attributes.getNamedItem(Constants.protection);
            protection = node.getNodeValue();
            attrTypes.put(type, protection);
        }

        return attrTypes;
    }

    private static String getK(Document document) {
        Node node;
        NamedNodeMap attributes;
        String protection;
        String k = null;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attributeType);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.protection);
            protection = node.getNodeValue();
            if (protection.equalsIgnoreCase(Constants.kAnonymity)) {
                node = attributes.getNamedItem(Constants.k);
                k = node.getNodeValue();
                break;
            }
        }

        return k;
    }

    private static String getT(Document document) {
        Node node;
        NamedNodeMap attributes;
        String protection;
        String t = null;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attributeType);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.protection);
            protection = node.getNodeValue();
            if (protection.equalsIgnoreCase(Constants.tCloseness)) {
                node = attributes.getNamedItem(Constants.t);
                t = node.getNodeValue();
                break;
            }
        }

        return t;
    }

    private static String getClouds(Document document) {
        Node node;
        NamedNodeMap attributes;
        String protection;
        String clouds = null;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attributeType);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.protection);
            protection = node.getNodeValue();
            if (protection.equalsIgnoreCase(Constants.splitting)) {
                node = attributes.getNamedItem(Constants.clouds);
                clouds = node.getNodeValue();
                break;
            }
        }

        return clouds;
    }

    private static String getIdKey(Document document) {
        Node node;
        NamedNodeMap attributes;
        String protection;
        String idKey = null;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attributeType);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.protection);
            protection = node.getNodeValue();
            if (protection.equalsIgnoreCase(Constants.encryption)) {
                node = attributes.getNamedItem(Constants.id_key);
                idKey = node.getNodeValue();
                break;
            }
        }

        return idKey;
    }

    private static String getRadius(Document document) {
        Node node;
        NamedNodeMap attributes;
        String protection;
        String radius = null;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attributeType);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.protection);
            protection = node.getNodeValue();
            if (protection.equalsIgnoreCase(Constants.coarsening)) {
                node = attributes.getNamedItem(Constants.radius);
                radius = node.getNodeValue();
                break;
            }
        }

        return radius;
    }

    private static String getCoarseningType(Document document) {
        Node node;
        NamedNodeMap attributes;
        String protection;
        String type = null;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attributeType);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.protection);
            protection = node.getNodeValue();
            if (protection.equalsIgnoreCase(Constants.coarsening)) {
                node = attributes.getNamedItem(Constants.coarseningType);
                type = node.getNodeValue();
                break;
            }
        }

        return type;
    }

    private static String getCoarseningK(Document document) {
        Node node;
        NamedNodeMap attributes;
        String protection;
        String k = null;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attributeType);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.protection);
            protection = node.getNodeValue();
            if (protection.equalsIgnoreCase(Constants.coarsening)) {
                node = attributes.getNamedItem(Constants.k);
                k = node.getNodeValue();
                break;
            }
        }

        return k;
    }

    private static ArrayList<String> getAtributeNames(Document document) {
        ArrayList<String> names = new ArrayList<String>();
        Node node;
        NamedNodeMap attributes;
        String name;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attribute);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.name);
            name = node.getNodeValue();
            names.add(name);
        }

        return names;
    }

    private static ArrayList<String> getAtributeTypes(Document document) {
        ArrayList<String> attrTypes = new ArrayList<String>();
        Node node;
        NamedNodeMap attributes;
        String attrType;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attribute);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.attributeType);
            attrType = node.getNodeValue();
            attrTypes.add(attrType);
        }

        return attrTypes;
    }

    private static ArrayList<String> getAttributeDataTypes(Document document) {
        ArrayList<String> attrTypes = new ArrayList<String>();
        Node node;
        NamedNodeMap attributes;
        String attrType;
        NodeList nodeList;

        // URV fix: elements are retrieved by their name
        nodeList = document.getElementsByTagName(Constants.attribute);

        for (int i = 0; i < nodeList.getLength(); i++) {
            node = nodeList.item(i);
            attributes = node.getAttributes();
            node = attributes.getNamedItem(Constants.dataType);
            if (node == null) {
                attrTypes.add("");
            } else {
                attrType = node.getNodeValue();
                attrTypes.add(attrType);
            }
        }

        return attrTypes;
    }

    public static ArrayList<Record> createRecords(String data) {
        ArrayList<Record> records = new ArrayList<Record>();
        String recordsStr[];
        String strTemp[];
        Record record;
        int id;

        recordsStr = data.split(Record.recordSeparator);
        id = 0;
        for (int i = 0; i < recordsStr.length; i++) {
            strTemp = recordsStr[i].split(Record.attributeSeparator);
            record = new Record(id);
            id++;
            for (int j = 0; j < Record.numAttr; j++) {
                record.attrValues[j] = strTemp[j];
            }
            records.add(record);
        }

        System.out.println("Records loaded: " + records.size());
        return records;
    }

    public static ArrayList<Record> createRecords(String[][] data) {
        ArrayList<Record> records = new ArrayList<Record>();
        Record record = null;
        int id;

        id = 0;
        for (int i = 0; i < data.length; i++) {
            record = new Record(id);
            id++;
            for (int j = 0; j < data[i].length; j++) {
                record.attrValues[j] = data[i][j];
            }
            records.add(record);
        }

        System.out.println("Records loaded: " + records.size());
        return records;
    }

    public static String[][][] createMatrixStringFromRecords(ArrayList<ArrayList<Record>> records) {
        String data[][][];
        String dataTemp[][];
        Record record;
        ArrayList<Record> dataList;

        dataList = records.get(0);
        dataTemp = new String[dataList.size()][];
        for (int i = 0; i < dataList.size(); i++) {
            record = dataList.get(i);
            dataTemp[i] = record.toVectorString();
        }

        data = new String[2][dataTemp.length][];
        data[0] = dataTemp;

        dataList = records.get(1);
        dataTemp = new String[dataList.size()][];
        for (int i = 0; i < dataList.size(); i++) {
            record = dataList.get(i);
            dataTemp[i] = record.toVectorString();
        }
        data[1] = dataTemp;

        System.out.println(data.length + " records converted to String matrix");
        return data;
    }

    @Deprecated
    public static void writeFile(ArrayList<ArrayList<Record>> data) {
        File file;
        FileWriter fw;
        BufferedWriter bw;
        String fileName;
        int cont;

        for (int i = 0; i < data.size(); i++) {
            cont = 0;
            if (Record.header) {
                addCabecera(data.get(i));
                cont = -1;
            }
            fileName = "data_clarus_anom_" + (i + 1) + ".txt";
            file = new File(fileName);
            try {
                fw = new FileWriter(file);
                bw = new BufferedWriter(fw);
                for (Record r : data.get(i)) {
                    bw.write(r.toString());
                    bw.newLine();
                    cont++;
                }
                bw.close();
                fw.close();

                System.out.println("Records saved: " + cont);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private static void addCabecera(ArrayList<Record> lista) {
        Record record;

        record = new Record(0);
        for (int i = 0; i < Record.listNames.size(); i++) {
            record.attrValues[i] = Record.listNames.get(i);
        }
        lista.add(0, record);
    }

}
