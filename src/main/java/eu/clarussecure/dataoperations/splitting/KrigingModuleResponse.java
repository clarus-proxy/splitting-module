package eu.clarussecure.dataoperations.splitting;

import eu.clarussecure.dataoperations.DataOperationResponse;

import java.util.Random;

public class KrigingModuleResponse extends DataOperationResponse{
    private KrigingResult result;

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
