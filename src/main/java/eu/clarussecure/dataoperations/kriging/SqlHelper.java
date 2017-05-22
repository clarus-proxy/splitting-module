package eu.clarussecure.dataoperations.kriging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SqlHelper {
	public static Connection connect(String host, String username, String password) {
		Connection con = null;
	      try {
	         Class.forName("org.postgresql.Driver");
	         con = DriverManager
	            .getConnection("jdbc:"+host,
	            username, password);
	      } catch (Exception e) {
	         e.printStackTrace();
	         System.err.println(e.getClass().getName()+": "+e.getMessage());
	         System.exit(0);
	      }
	      return con;
	}
	
	public static ResultSet select(Connection con, String table) throws SQLException {
		return select(con, null, table, null);
	}
	public static ResultSet select(Connection con, String table, int limit) throws SQLException {
		return select(con, null, table, null, limit);
	}
	
	public static ResultSet select(Connection con, String table, List<String> conditions) throws SQLException {
		return select(con, null, table, conditions);
	}
	public static ResultSet select(Connection con, String table, List<String> conditions, int limit) throws SQLException {
		return select(con, null, table, conditions, limit);
	}
	
	public static ResultSet select(Connection con, List<String> columns, String table) throws SQLException {
		return select(con, columns, table, null);
	}
	public static ResultSet select(Connection con, List<String> columns, String table, int limit) throws SQLException {
		return select(con, columns, table, null, limit);
	}
	
	public static ResultSet select(Connection con, List<String> columns, String table, List<String> conditions) throws SQLException {
		return select(con, columns, table, conditions, -1);
	}
	
	public static ResultSet select(Connection con, List<String> columns, String table, List<String> conditions, int limit) throws SQLException {
		ResultSet results = null;
		String query = generateSelect(columns, table, conditions, limit);
		System.out.println(query);
		results = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY).executeQuery(query);
		return results;
	}
	
	
	
	private static String generateSelect(List<String> columns, String table, List<String> conditions, int limit){
		//We start by generating the SELECT statement depending on the function input
		String query = "SELECT ";
		if(columns instanceof List) {
			query += String.join(", ", columns);
			query += " ";
		}
		else query += "* ";
		//From table
		query += "FROM "+table+" ";
		//Here we use 1=1 as the first WHERE statement (always true) for code simplicity when we add (or not) WHERE conditions
		query += "WHERE 1=1";
		if(conditions instanceof List) {
			for(String condition : conditions) {
				query += " AND "+condition;
			}
		}
		query += " ORDER BY gid";
		if(limit != -1) query += " LIMIT "+limit+";";
		else query += ";";
		return query;
	}
	
	public static List<DBRecord> extractEncryptedRegisters(ResultSet result, ResultSet result2, String valueToExtract) throws SQLException, FileNotFoundException, ClassNotFoundException, IOException {
		List<DBRecord> registers = new ArrayList<DBRecord>();
		result.beforeFirst();
		result2.beforeFirst();
		
		while(result.next() && result2.next()) {
			if(result.getInt("gid") == result2.getInt("gid")) {
				registers.add(new DBRecord(
						result.getBigDecimal("x"), 
						result2.getBigDecimal("y"), 
						new BigDecimal(ByteBuffer.wrap(Crypto.decrypt(result2.getBytes(valueToExtract),Crypto.PRIVATE_KEY)).getDouble())
					));
			}
		}
		return registers;
	}
	public static List<DBRecord> extractRegisters(ResultSet result, ResultSet result2, String valueToExtract) throws SQLException {
		List<DBRecord> registers = new ArrayList<DBRecord>();
		result.beforeFirst();
		result2.beforeFirst();
		
		while(result.next() && result2.next()) {
			if(result.getInt("gid") == result2.getInt("gid")) {
				registers.add(new DBRecord(
						result.getBigDecimal("x"), 
						result2.getBigDecimal("y"), 
						result2.getBigDecimal(valueToExtract)
					));
			}
		}
		return registers;
	}
}
