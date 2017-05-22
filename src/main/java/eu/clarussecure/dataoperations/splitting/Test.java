package eu.clarussecure.dataoperations.splitting;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import eu.clarussecure.dataoperations.*;
import eu.clarussecure.dataoperations.kriging.*;
import org.w3c.dom.Document;

public class Test {

	public static void main(String[] args) throws IOException {
		String[] attributes;
		String[][] dataOri;
        List<DataOperationResult> dataOri2;
        List<DataOperationCommand> dataAnom;
        List<String[][]> dataAnom2;
		byte[] xmlProperties;
		Document document;
		DatasetParser datasetParser;
		File file;

//        splitting example
		xmlProperties = loadXmlFile("./datasets/properties3.xml");
		document = Functions.readDocument(xmlProperties);
		DataOperation interFace = new SplittingModule(document);

		file = new File("./datasets/meuse2.txt");
		datasetParser = new DatasetParser(file , ",");

		attributes = datasetParser.parseHeaders();
		dataOri = datasetParser.parseDataset();
		dataAnom = interFace.post(attributes, dataOri);
        writeFile(dataAnom, false);
        String[][] append = {{"500","1.800000000000000","25.000000000000000","97.000000000000000","251.000000000000000","9.073000000000000","0.228123000000000","9.000000000000000","1","1","0","Ag","300.000000000000000","0101000020E61000000000000040190641000000009C531441"}};
        dataAnom = interFace.post(attributes, append);
        writeFile(dataAnom, true);


        // retrieve from splitting example

        List<DataOperationCommand>commands = interFace.get(attributes, null);
        dataAnom2 = loadData(commands);
        dataOri2 = interFace.get(commands, dataAnom2);
        if(dataOri2.get(0) instanceof DataOperationResponse){
            DataOperationResponse dataOperationResponse = (DataOperationResponse)dataOri2.get(0);
            writeFile(dataOperationResponse.getContents());
        }
//
//        //retrieve an area from splitting example
//
//        Criteria criteria = new Criteria("meuseDB/meuse/geom", Constants.area, "180199,331700,180700,332570");
//        Criteria criterias[] = new Criteria[1];
//        criterias[0] = criteria;
//
//        List<DataOperationCommand>commands = interFace.get(attributes, criterias);
//        dataAnom2 = loadData(commands);
//        dataOri2 = interFace.get(commands, dataAnom2);
//        if(dataOri2.get(0) instanceof DataOperationResponse){
//            DataOperationResponse dataOperationResponse = (DataOperationResponse)dataOri2.get(0);
//            writeFile(dataOperationResponse.getContents());
//        }
	}
	
	public static byte[] loadXmlFile(String filePropertiesName){
		FileReader2 file;
		String linea;
		String xml;
		
		file = new FileReader2(filePropertiesName);
		xml = "";
		while((linea=file.readLine())!=null){
			xml += linea;
		}
		file.closeFile();
		Functions.readProperties(xml);
		System.out.println("Xml loaded");
		return xml.getBytes();
	}

	public static void writeFile(String[][][] data) {
		File file;
		FileWriter fw;
		BufferedWriter bw;
		String fileName;
		int cont;
		String line;
		String cabecera = "meuseDB/meuse/gid,meuseDB/meuse/cadmium,meuseDB/meuse/copper," +
				"meuseDB/meuse/lead,meuseDB/meuse/zinc,meuseDB/meuse/elev,meuseDB/meuse/dist," +
				"meuseDB/meuse/om,meuseDB/meuse/ffreq,meuseDB/meuse/soil,meuseDB/meuse/lime," +
				"meuseDB/meuse/landuse,meuseDB/meuse/dist.m,meuseDB/meuse/geom";


		for (int i = 0; i < data.length; i++) {
			fileName = "./datasets/meuse2_anom_" + (i) + ".txt";
			file = new File(fileName);
			try {
				fw = new FileWriter(file);
				bw = new BufferedWriter(fw);
				bw.write(cabecera);
				bw.newLine();
				for (int j = 0; j < data[0].length; j++) {
					line = "";
					for (int k = 0; k < data[i][j].length; k++) {
						line += data[i][j][k] + ",";
					}
					line = line.substring(0, line.length() - 1);
					bw.write(line);
					bw.newLine();
				}
				bw.close();
				fw.close();

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

    public static void writeFile(String[][] data) {
        File file;
        FileWriter fw;
        BufferedWriter bw;
        String fileName;
        int cont;
        String line;
        String cabecera = "meuseDB/meuse/gid,meuseDB/meuse/cadmium,meuseDB/meuse/copper," +
                "meuseDB/meuse/lead,meuseDB/meuse/zinc,meuseDB/meuse/elev,meuseDB/meuse/dist," +
                "meuseDB/meuse/om,meuseDB/meuse/ffreq,meuseDB/meuse/soil,meuseDB/meuse/lime," +
                "meuseDB/meuse/landuse,meuseDB/meuse/dist.m,meuseDB/meuse/geom";


        fileName = "./datasets/meuse2_retrieved.txt";
        file = new File(fileName);
        try {
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            bw.write(cabecera);
            bw.newLine();
            for (int j = 0; j < data.length; j++) {
                line = "";
                for (int k = 0; k < data[j].length; k++) {
                    line += data[j][k] + ",";
                }
                line = line.substring(0, line.length() - 1);
                bw.write(line);
                bw.newLine();
                System.out.println(line);
            }
            bw.close();
            fw.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void writeFile(List<DataOperationCommand>data, boolean append) {
        File file;
        FileWriter fw;
        BufferedWriter bw;
        String fileName;
        int cont;
        String line;
        String records[][];


        for (int i = 0; i < data.size(); i++) {
            fileName = "./datasets/meuse2_anom_" + (i) + ".txt";
            file = new File(fileName);
            try {
                fw = new FileWriter(file, append);
                bw = new BufferedWriter(fw);
                if (!append) {
                    bw.write(String.join(",", data.get(i).getProtectedAttributeNames()));
                    bw.newLine();
                }
                records = data.get(i).getProtectedContents();
                for (int j = 0; j < records.length; j++) {
                    line = "";
                    for (int k = 0; k < records[j].length; k++) {
                        line += records[j][k] + ",";
                    }
                    line = line.substring(0, line.length() - 1);
                    bw.write(line);
                    bw.newLine();
                }
                bw.close();
                fw.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static List<String[][]> loadData(List<DataOperationCommand> commands) {
        ArrayList<String>lines = new ArrayList<String>();
        File file;
        FileReader fr;
        BufferedReader br;
        String fileName;
        String line;
        String str[];
        String dataTemp[][];
        List <String[][]> data ;

//        data = new String[2][][];
        data = new ArrayList<String[][]>();
        for (int i = 0; i < 2; i++) {
            fileName = "./datasets/meuse2_anom_" + (i) + ".txt";
            file = new File(fileName);
            try {
                fr = new FileReader(file);
                br = new BufferedReader(fr);
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }

                br.close();
                fr.close();

                String[] headers = lines.get(0).split(",");
                lines.remove(0);
                str = lines.get(0).split(",");
                dataTemp = new String[lines.size()][str.length];
                for(int j=0; j<lines.size(); j++){
                    str = lines.get(j).split(",");
                    dataTemp[j] = str;
                }
                Map<String, String[]> table = datasetByColumns(headers, dataTemp);
                dataTemp = datasetByRows(commands.get(i).getProtectedAttributeNames(), table);
                data.add(dataTemp);
                lines.clear();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return data;
    }

    public static Map<String, String[]> datasetByColumns(String[] attributeNames, String[][] content) {
        // Have content in matrix[rows][columns] need content in matrix[columns][rows]
        String[][] transposedContent = transpose(content);
        Map<String, String[]> table = new HashMap<>();
        for (int i = 0; i < attributeNames.length; i++) {
            table.put(attributeNames[i], transposedContent[i]);
        }
        return table;
    }

    public static String[][] datasetByRows(String[] attributeNames, Map<String, String[]> dataset) {
        int rows = dataset.get(attributeNames[0]).length;
        int columns = attributeNames.length;
        String[][] table = new String[columns][rows];

        for (int i = 0; i < attributeNames.length; i++) {
            table[i] = dataset.get(attributeNames[i]);
        }

        return transpose(table);
    }

    public static String[][] transpose(String[][] content) {
        String[][] transposedContent = new String[content[0].length][content.length];
        for (int i = 0; i < content.length; i++) {
            for (int j = 0; j < content[0].length; j++) {
                transposedContent[j][i] = content[i][j];
            }
        }
        return transposedContent;
    }
}