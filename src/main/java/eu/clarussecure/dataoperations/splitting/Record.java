package eu.clarussecure.dataoperations.splitting;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

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
    static String idKey;
    static String coarsening_type;
    //AKKA fix: radius value depends on SRID. it could be a real
    //static int radius;
    static double radius;
    static ArrayList<String> listNames;
    static ArrayList<String> listAttrTypes;
    static ArrayList<String> listDataTypes;
    String attrValues[];
    int id;

    public Record(int id) {
        this.attrValues = new String[numAttr];
        this.id = id;
    }

    public Record clone() {
        Record record;

        record = new Record(this.id);
        for (int i = 0; i < this.attrValues.length; i++) {
            record.attrValues[i] = this.attrValues[i];
        }
        return record;
    }

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
