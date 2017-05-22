package eu.clarussecure.dataoperations.kriging;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKBWriter;
import eu.clarussecure.dataoperations.*;
import eu.clarussecure.dataoperations.kriging.Cloud;
import eu.clarussecure.dataoperations.splitting.*;
import org.w3c.dom.Document;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class Demo {

	public static void main(String[] args) throws IOException {
		List<Cloud> clouds = new ArrayList<>();

		// Load data at client application
        File file = new File("./datasets/meuse2.txt");
        DatasetParser datasetParser = new DatasetParser(file , ",");
        String[] attributes = datasetParser.parseHeaders();
        String[][] dataOri = datasetParser.parseDataset();

        // Instantiate Splitting Module
		byte[] xmlProperties = loadXmlFile("./datasets/properties3.xml");
		Document document = Functions.readDocument(xmlProperties);
		DataOperation splittingModule = new SplittingModule(document);

		// Post new dataset operation
	    List<DataOperationCommand> commands = splittingModule.post(attributes, dataOri);

	    // Create db in cloud
	    for (DataOperationCommand c : commands) {
	        clouds.add(new Cloud(c.getProtectedAttributeNames(), c.getProtectedContents()));
        }

        // Show what it is in the cloud
        System.out.println("Contents in the cloud after first post.");
        for (Cloud c : clouds) {
	        c.printContents();
        }
        System.out.println("---------------------------------------");

        // Post new record operation
        String[][] append = {{"500","1.800000000000000","25.000000000000000","97.000000000000000","251.000000000000000","9.073000000000000","0.228123000000000","9.000000000000000","1","1","0","Ag","300.000000000000000","0101000020E61000000000000040190641000000009C531441"}};
        commands = splittingModule.post(attributes, append);

        // Update database
        for (int i = 0; i < commands.size(); i++) {
            clouds.get(i).post(commands.get(i).getProtectedAttributeNames(), commands.get(i).getProtectedContents());
        }

        // Show what it is in the cloud
        System.out.println("Contents in the cloud after second post.");
        for (Cloud c : clouds) {
            c.printContents();
        }
        System.out.println("---------------------------------------");

        // GET without criteria
        commands = splittingModule.get(attributes, null);

        // Operation at the CLOUD
        List<String[][]> contents = queryClouds(clouds, commands);

        // Back at CLARUS again
        List<DataOperationResult>  results = splittingModule.get(commands, contents);

        // Client application
        System.out.println("Response from get with no criteria.");
        printResponse(results);
        System.out.println("---------------------------------------");

        //Retrieve an area from splitting example

        Criteria criteria = new Criteria("meuseDB/meuse/geom", eu.clarussecure.dataoperations.splitting.Constants.area, "180199,331700,180700,332570");
        Criteria[] criterias = new Criteria[1];
        criterias[0] = criteria;

        commands = splittingModule.get(attributes, criterias);
        contents = queryClouds(clouds, commands);
        results = splittingModule.get(commands, contents);

        System.out.println("Response from get with area criteria. -> geom in area(180199,331700,180700,332570)");
        printResponse(results);
        System.out.println("---------------------------------------");

        // Retrieve record by GID

        criteria = new Criteria("meuseDB/meuse/gid", "=", "37");
        criterias = new Criteria[1];
        criterias[0] = criteria;

        commands = splittingModule.get(attributes, criterias);
        contents = queryClouds(clouds, commands);
        results = splittingModule.get(commands, contents);

        System.out.println("Response from get by id. -> gid == 37");
        printResponse(results);
        System.out.println("---------------------------------------");

        // Retrieve record by COPPER >= 100 AND LEAD == 405.0

        criteria = new Criteria("meuseDB/meuse/copper", ">=", "100.0");
        Criteria criteria2 = new Criteria("meuseDB/meuse/lead", ">", "405");
        criterias = new Criteria[2];
        criterias[0] = criteria;
        criterias[1] = criteria2;

        commands = splittingModule.get(attributes, criterias);
        contents = queryClouds(clouds, commands);
        results = splittingModule.get(commands, contents);

        System.out.println("Response from get by criteria. -> copper >= 100.0 AND lead == 405.0");
        printResponse(results);
        System.out.println("---------------------------------------");


        // Retrieve record by list of ids

        criteria = new Criteria("meuseDB/meuse/gid", eu.clarussecure.dataoperations.splitting.Constants.in, "1,3,4,8");
        criterias = new Criteria[1];
        criterias[0] = criteria;

        commands = splittingModule.get(attributes, criterias);
        contents = queryClouds(clouds, commands);
        results = splittingModule.get(commands, contents);

        System.out.println("Response from get by list criteria. -> gid in 1,3,4,8");
        printResponse(results);
        System.out.println("---------------------------------------");

        // Delete records

        criteria = new Criteria("meuseDB/meuse/gid", eu.clarussecure.dataoperations.splitting.Constants.in, "1,2,3,4,5");
        criterias = new Criteria[1];
        criterias[0] = criteria;

        commands = splittingModule.delete(attributes, criterias);
        deleteInClouds(clouds, commands);

        System.out.println("State of clouds after delete. -> gid in 1,2,3,4,5");
        for (Cloud c: clouds) {
            c.printContents();
        }
        System.out.println("---------------------------------------");

        // Update a record

        String[][] update = {{"500","2.800000000000000","24.000000000000000","100.000000000000000","250.000000000000000","10.073000000000000","1.228123000000000","8.000000000000000","2","2","2","Ag","300.000000000000000","0101000020E610000000000000F8DD054100000000004054C0"}};
        criteria = new Criteria("meuseDB/meuse/gid", "=", "500");
        criterias = new Criteria[1];
        criterias[0] = criteria;

        commands = splittingModule.put(attributes, criterias, update);
        updateInClouds(clouds, commands);

        // Retrieve updated record by GID

        criteria = new Criteria("meuseDB/meuse/gid", "=", "500");
        criterias = new Criteria[1];
        criterias[0] = criteria;

        commands = splittingModule.get(attributes, criterias);
        contents = queryClouds(clouds, commands);
        results = splittingModule.get(commands, contents);

        System.out.println("Response from get by id after update. -> gid == 500");
        printResponse(results);
        System.out.println("---------------------------------------");


        // KRIGINF TEST
        System.out.println("KRIGING TEST");

        // cleanup clouds
        clouds = new ArrayList<>();

        //load new dataset
        attributes = new String[3];
        attributes[0] = "meuseDB/meuse/gid";
        attributes[1] = "meuseDB/meuse/zinc";
        attributes[2] = "meuseDB/meuse/geom";
        dataOri = loadData();

        // instance new splitting module
        xmlProperties = loadXmlFile("./datasets/propertiesKriging.xml");
        document = Functions.readDocument(xmlProperties);
        splittingModule = new SplittingModule(document);

        // post new dataset
        commands = splittingModule.post(attributes, dataOri);

        // Create DB in clouds
        for (DataOperationCommand c: commands) {
            clouds.add(new Cloud(c.getProtectedAttributeNames(), c.getProtectedContents()));
        }

        // Show contents in cloud
        System.out.println("Kriging dataset in clouds.");
        for (Cloud c: clouds) {
            c.printContents();
        }
        System.out.println("--------------------------");


        //kriging of zinc at point (1,0)
        criteria = new Criteria("meuseDB/meuse/zinc", eu.clarussecure.dataoperations.splitting.Constants.kriging, "meuseDB/meuse/geom:1,0");
        criterias = new Criteria[1];
        criterias[0] = criteria;

        // Orchestrated Kriging
        //commands = splittingModule.get(attributes, criterias);
        commands = splittingModule.get(null, criterias);
        contents = queryClouds(clouds, commands);
        results = splittingModule.get(commands, contents);
        while (true) {
            if (results.stream().anyMatch(p -> p instanceof DataOperationCommand)) {
                commands = toCommands(results);
                contents = queryClouds(clouds, commands);
                results = splittingModule.get(commands, contents);
            } else if (results.stream().anyMatch(p -> p instanceof DataOperationResponse)) {
                printResponse(results);
                break;
            }
        }
	}

	private static List<String[][]> queryClouds(List<Cloud> clouds, List<DataOperationCommand> commands) {
        List<String[][]> contents = new ArrayList<>();
        for (int i = 0; i < commands.size(); i++) {
            if (commands.get(i) == null) {
                contents.add(null);
            } else {
                String[] protectedAttributeNames = commands.get(i).getProtectedAttributeNames();
                Criteria[] criteria = commands.get(i).getCriteria();
                InputStream[] extraBinaryContent = commands.get(i).getExtraBinaryContent();
                String[] extraProtectedAttributeNames = commands.get(i).getExtraProtectedAttributeNames();
                contents.add(clouds.get(i).get(protectedAttributeNames, criteria, extraBinaryContent, extraProtectedAttributeNames));
            }
        }
        return contents;
    }

    private static void deleteInClouds(List<Cloud> clouds, List<DataOperationCommand> commands) {
	    for (int i = 0; i < commands.size(); i++) {
            clouds.get(i).delete(commands.get(i).getCriteria());
        }
    }

    private static void updateInClouds(List<Cloud> clouds, List<DataOperationCommand> commands) {
        for (int i = 0; i < commands.size(); i++) {
            clouds.get(i).update(commands.get(i).getCriteria(), commands.get(i).getProtectedContents());
        }
    }

	private static void printResponse(List<DataOperationResult> results) {
	    for (DataOperationResult r: results) {
	        DataOperationResponse re = (DataOperationResponse) r;
	        System.out.println(String.join(", ", re.getAttributeNames()));
	        for (String[] row : re.getContents()) {
	            System.out.println(String.join(", ", row));
            }
        }
    }

    private static List<DataOperationCommand> toCommands(List<DataOperationResult> results) {
	    List<DataOperationCommand> commands = new ArrayList<>();
	    for (DataOperationResult r: results) {
	        if (r == null) {
	            commands.add(null);
            } else {
	            commands.add((DataOperationCommand) r);
            }
        }
        return commands;
    }

	private static byte[] loadXmlFile(String filePropertiesName){
		FileReader2 file;
		String linea;

		file = new FileReader2(filePropertiesName);
		StringBuilder sb = new StringBuilder();
		while((linea=file.readLine())!=null){
            sb.append(linea);
		}

		String xml = sb.toString();
		file.closeFile();
		Functions.readProperties(xml);
		System.out.println("Xml loaded");
		return xml.getBytes();
	}

    private static String[][] loadData(){
        String data[][];
        PrecisionModel pmodel = new PrecisionModel();
        GeometryFactory builder = new GeometryFactory(pmodel, 4326);
        WKBWriter writer = new WKBWriter(2, 2, true);
        Geometry geom;
        Coordinate newCoord;

        data = new String[3][3];
        data[0][0] = "1";   //gid
        data[0][1] = "9";   //zinc
        newCoord = new Coordinate(0, 1);
        geom = builder.createPoint(newCoord);
        data[0][2] = WKBWriter.toHex(writer.write(geom));   //geom
        data[1][0] = "2";   //gid
        data[1][1] = "3";   //zinc
        newCoord = new Coordinate(0, 0);
        geom = builder.createPoint(newCoord);
        data[1][2] = WKBWriter.toHex(writer.write(geom));   //geom
        data[2][0] = "3";   //gid
        data[2][1] = "4";   //zinc
        newCoord = new Coordinate(3, 0);
        geom = builder.createPoint(newCoord);
        data[2][2] = WKBWriter.toHex(writer.write(geom));   //geom

        return data;
    }
}