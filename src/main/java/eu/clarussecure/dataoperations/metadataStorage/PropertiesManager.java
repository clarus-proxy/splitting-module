package eu.clarussecure.dataoperations.metadataStorage;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class PropertiesManager {

    private String server;
    private int port;
    private String database;
    private String dbuser;
    private String dbpassword;
    private String table;
    private String collectionName;
    private boolean usesAuth;

    public PropertiesManager() {
        Properties prop = new Properties();
        InputStream input = null;

        try {

            input = new FileInputStream("config.cfg");

            prop.load(input);

            server = prop.getProperty("server");
            port = Integer.parseInt(prop.getProperty("port"));
            database = prop.getProperty("database");
            usesAuth = Boolean.parseBoolean(prop.getProperty("useauth"));
            dbuser = prop.getProperty("dbuser");
            dbpassword = prop.getProperty("dbpassword");
            table = prop.getProperty("table");
            collectionName = prop.getProperty("collectionName");

        } catch (IOException ex) {
            //ex.printStackTrace();
            createProperties();

        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public String getServer() {
        return server;
    }

    public String getDatabase() {
        return database;
    }

    public boolean getUsesAuth() {
        return usesAuth;
    }

    public String getDbuser() {
        return dbuser;
    }

    public String getDbpassword() {
        return dbpassword;
    }

    public String getTable() {
        return table;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public int getPort() {
        return port;
    }

    private void createProperties() {
        Properties prop = new Properties();
        OutputStream output = null;

        try {

            output = new FileOutputStream("config.cfg");

            prop.setProperty("server", "localhost");
            prop.setProperty("port", "27017");
            prop.setProperty("database", "mydb");
            prop.setProperty("useauth", "true");
            prop.setProperty("dbuser", "user");
            prop.setProperty("dbpassword", "password");
            prop.setProperty("table", "tablename");
            prop.setProperty("collectionName", "collectionname");

            // save properties to project root folder
            prop.store(output, null);

        } catch (IOException io) {
            io.printStackTrace();
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

}
