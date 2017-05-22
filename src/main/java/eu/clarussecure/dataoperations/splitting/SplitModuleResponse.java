package eu.clarussecure.dataoperations.splitting;

import eu.clarussecure.dataoperations.DataOperationResponse;
import java.util.Random;

/**
 * Created by Alberto on 20/04/2017.
 */
public class SplitModuleResponse extends DataOperationResponse{
    public SplitModuleResponse(String[] attributeNames, String[][] contents) {

        super.id = new Random().nextInt();
        super.attributeNames = attributeNames;  //headers originales
        super.contents = contents;
    }
}
