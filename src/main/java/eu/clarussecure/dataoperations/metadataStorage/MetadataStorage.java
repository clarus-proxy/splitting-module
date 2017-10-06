package eu.clarussecure.dataoperations.metadataStorage;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import eu.clarussecure.dataoperations.splitting.SplitPoint;
import org.bson.Document;

import java.io.*;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

public class MetadataStorage {

    private static MetadataStorage metaStorage = null;
    private PropertiesManager props;
    private final MongoClient mongoClient;
    private final MongoDatabase db;
    private final MongoCollection<Document> dbCollection;
    private MongoCredential creds;
    private int instancesNumber;

    private MetadataStorage() {

        props = new PropertiesManager();
        if (props.getUsesAuth()) {
            creds = MongoCredential.createCredential(props.getDbuser(), props.getDatabase(),
                    props.getDbpassword().toCharArray());
            mongoClient = new MongoClient(new ServerAddress(props.getServer(), props.getPort()), Arrays.asList(creds));
        } else {
            mongoClient = new MongoClient(props.getServer(), props.getPort());
        }

        db = mongoClient.getDatabase(props.getDatabase());
        dbCollection = db.getCollection(props.getCollectionName());

        instancesNumber++;
    }

    public static MetadataStorage getInstance() {
        if (MetadataStorage.metaStorage == null) {
            MetadataStorage.metaStorage = new MetadataStorage();
        }
        return MetadataStorage.metaStorage;
    }

    public void deleteInstance() {
        this.instancesNumber--;

        if (this.instancesNumber <= 0) {
            this.mongoClient.close();
            MetadataStorage.metaStorage = null;
        }
    }

    public void storeMetadata(String dataID, List<Map<String, String>> metadata, Map<String, SplitPoint> splitPoints) {
        // Prepare the document into the dabase
        Document doc = new Document("dataID", dataID);
        doc.append("metadata", objectToString(metadata));
        doc.append("splitPoints", objectToString(splitPoints));

        boolean ack = this.dbCollection.replaceOne(eq("dataID", dataID), doc, new UpdateOptions().upsert(true)).wasAcknowledged();
    }

    public List<Map<String, String>> retrieveMetadata(String dataID) {
        List<Map<String, String>> metadata = null;
        if (this.dbCollection.count(eq("dataID", dataID)) <= 0) {
            return null;
        }
        MongoCursor<Document> cursor = this.dbCollection.find(eq("dataID", dataID)).iterator();
        if (cursor.hasNext()) {
            Document doc = cursor.next();
            Object md = stringToObject(doc.getString("metadata"));
            metadata = (List<Map<String, String>>) md;
        }

        return metadata;
    }

    public Map<String, SplitPoint> retrieveSplitPoints(String dataID) {
        Map<String, SplitPoint> splitPoints = null;
        if (this.dbCollection.count(eq("dataID", dataID)) <= 0) {
            return null;
        }
        MongoCursor<Document> cursor = this.dbCollection.find(eq("dataID", dataID)).iterator();
        if (cursor.hasNext()) {
            Document doc = cursor.next();
            Object md = stringToObject(doc.getString("splitPoints"));
            splitPoints = (Map<String, SplitPoint>) md;
        }
        return splitPoints;
    }

    public void deleteCollection() {
        dbCollection.drop();
    }

    private String objectToString(Object o) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutput oo = null;
        String string = null;
        try {
            oo = new ObjectOutputStream(baos);
            oo.writeObject(o);
            oo.flush();
            string = Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                baos.close();
            } catch (Exception e) {
                //Ignore
            }
        }
        return string;
    }

    private Object stringToObject(String b) {
        ByteArrayInputStream bais = new ByteArrayInputStream(Base64.getDecoder().decode(b));
        ObjectInput oi =  null;
        Object o = null;
        try {
            oi = new ObjectInputStream(bais);
            o = oi.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (oi != null) {
                    oi.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return o;
    }
}
