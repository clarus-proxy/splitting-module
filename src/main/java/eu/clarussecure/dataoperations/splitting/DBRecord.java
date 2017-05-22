package eu.clarussecure.dataoperations.kriging;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

import java.math.BigDecimal;

public class DBRecord {
	
	private BigDecimal x;
	private BigDecimal y;
	private BigDecimal z;
	private BigDecimal distanceToPoint0;
	
	
	
	public DBRecord(BigDecimal x, BigDecimal y, BigDecimal z) {
		this.x = x;
		this.y = y;
		this.z = z;
	}
	public DBRecord(String x, String y, String z) {
		this.x = new BigDecimal(getXFromGeom(x));
		this.y = new BigDecimal(getYFromGeom(y));
		this.z = new BigDecimal(z);
	}
	public BigDecimal getX() {
		return x;
	}
	public BigDecimal getY() {
		return y;
	}
	public BigDecimal getZ() {
		return z;
	}
	public void setZ(BigDecimal z) {
		this.z = z;
	}
	public void setDistanceToPoint0(BigDecimal distance) {
		distanceToPoint0 = distance;
	}
	public BigDecimal getDistanceToPoint0(){
		return distanceToPoint0;
	}

	private static double getXFromGeom(String wkbHex){
		Geometry geom = null;
		WKBReader reader = new WKBReader();
		double x = 0;

		try {
			geom = reader.read(WKBReader.hexToBytes(wkbHex));
			x = geom.getCoordinate().x;
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return x;
	}

	private static double getYFromGeom(String wkbHex){
		Geometry geom = null;
		WKBReader reader = new WKBReader();
		double y = 0;

		try {
			geom = reader.read(WKBReader.hexToBytes(wkbHex));
			y = geom.getCoordinate().y;
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return y;
	}
}
