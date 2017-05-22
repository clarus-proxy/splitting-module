package eu.clarussecure.dataoperations.kriging;

import java.io.File;

public class Constants {
	final static public String username = "postgres";
	final static public String username2 = "postgres";
	final static public String password = "12345";
	final static public String password2 = "12345";
	final static public String host = "postgresql://localhost:5432/";
	final static public String host2 = "postgresql://localhost:5431/";
	final static public String database = "geo_datasets";
	final static public int precision = 9;
	final static public int a = 10;
	final static public int b = 8;
	final static public int c = 2;
	final static public String area = "area";
	final static public String in = "in";
	final static public String algoritmeXifrat = "RSA/ECB/PKCS1Padding";
	final static public String carpetaKeys = "."+File.separator+"keys"+File.separator;
	final static public String kriging = "kriging";
	final static public String krigingCalculateX = "calculateX";
	final static public String krigingCalculateY = "calculateY";
}
