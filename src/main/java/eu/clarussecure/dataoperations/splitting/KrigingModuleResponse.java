package eu.clarussecure.dataoperations.kriging;

import eu.clarussecure.dataoperations.DataOperationResponse;

import java.util.Random;

/**
 * Created by sergi on 15/05/2017.
 */
public class KrigingModuleResponse extends DataOperationResponse{
    KrigingResult result;

    public KrigingModuleResponse(String[] attributeNames, String[][] contents) {

        super.id = new Random().nextInt();
        super.attributeNames = attributeNames;  //headers originales
        super.contents = contents;
    }

    public KrigingResult getResult() {
        return result;
    }

    public void setResult(KrigingResult result) {
        this.result = result;
    }
}
