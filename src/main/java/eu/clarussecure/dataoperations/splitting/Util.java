package eu.clarussecure.dataoperations.splitting;

import java.util.Map;

public final class Util {

    public final static String stringArrayToString(String[][] array) {
        String s = "\n[\n";
        for (String[] r : array) {
            s += "  " + stringArrayToString(r) + "\n";
        }
        s += "\n]";
        return s;
    }

    public final static String stringArrayToString(String[] array) {
        if (array == null) {
            return "array is null";
        }
        String s = "[";
        for (String r : array) {
            s += " " + r;
        }
        s += " ]";
        return s;
    }

    public static boolean isNullOrContainsNullString(Map<String, String> mapping) {
        if (mapping == null) {
            return true;
        }
        for (String key : mapping.keySet()) {
            if (key == null || mapping.get(key) == null) {
                return true;
            }
        }
        return false;
    }

    public static boolean isNullOrContainsNullString(String[][] array) {
        if (array == null) {
            return true;
        }
        for (String[] a : array) {
            if (isNullOrContainsNullString(a)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isNullOrContainsNullString(String[] array) {
        if (array == null) {
            return true;
        }
        for (String s : array) {
            if (s == null) {
                return true;
            }
        }
        return false;
    }
}
