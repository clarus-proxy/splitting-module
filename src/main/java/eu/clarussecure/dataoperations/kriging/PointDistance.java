package eu.clarussecure.dataoperations.kriging;

import java.math.BigDecimal;

public class PointDistance {
	
	private DBRecord point1;
	private DBRecord point2;
	private BigDecimal distance;
	private BigDecimal gamma;
	private BigDecimal associatedCovariance;

	public PointDistance(DBRecord point1, DBRecord point2, BigDecimal distance) {
		super();
		this.point1 = point1;
		this.point2 = point2;
		this.distance = distance;
	}

	public DBRecord getPoint1() {
		return point1;
	}
	public DBRecord getPoint2() {
		return point2;
	}
	public BigDecimal getDistance() {
		return distance;
	}
	public BigDecimal getGamma() {
		return gamma;
	}
	public void setGamma(BigDecimal gamma) {
		this.gamma = gamma;
	}
	public BigDecimal getAssociatedCovariance() {
		return associatedCovariance;
	}
	public void setAssociatedCovariance(BigDecimal associatedCovariance) {
		this.associatedCovariance = associatedCovariance;
	}
	@Override
	public String toString(){
		return distance.toString();
	}
	
}
