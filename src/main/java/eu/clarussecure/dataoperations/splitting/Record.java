package eu.clarussecure.dataoperations.splitting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

public class Record {
    static String attributeSeparator;
    static String recordSeparator;
    static boolean header;
    static int numAttr;
    static int numQuasi;
    static HashMap<String, String> attrTypes;
    static int k;
    static double t;
    static int clouds;
    static String splittingType;
    static String idKey;
    static String coarsening_type;
    // AKKA fix: radius value depends on SRID. it could be a real
    // static int radius;
    static double radius;
    // AKKA fix: keep original numAttr, listNames, listAttrTypes and
    // listDataTypes as reference
    static int refNumAttr;
    static ArrayList<String> refListNames;
    static ArrayList<Pattern> refListNamePatterns;
    static ArrayList<String> refListAttrTypes;
    static ArrayList<String> refListDataTypes;
    // numAttr, listNames, listNamePatterns, listAttrTypes and listDataTypes are
    // resolved for each data operation according to the original refNumAttr,
    // refListNames, refListAttrTypes and refListDataTypes
    static ArrayList<String> listNames;
    // AKKA fix: attribute matching done with patterns
    static ArrayList<Pattern> listNamePatterns;
    static ArrayList<String> listAttrTypes;
    static ArrayList<String> listDataTypes;
    String attrValues[];
    int id;

    public Record(int id) {
        this.attrValues = new String[numAttr];
        this.id = id;
    }

    @Override
    public Record clone() {
        Record record;

        record = new Record(this.id);
        for (int i = 0; i < this.attrValues.length; i++) {
            record.attrValues[i] = this.attrValues[i];
        }
        return record;
    }

    @Override
    public String toString() {
        String str;
        str = "";
        for (String s : attrValues) {
            str += s + attributeSeparator;
        }

        if (str.equals(""))
            return "";
        return str.substring(0, str.length() - 1) + recordSeparator;
    }

    public String[] toVectorString() {
        String str[];

        str = new String[numAttr];
        for (int i = 0; i < numAttr; i++) {
            str[i] = attrValues[i];
        }

        return str;
    }

}
