package eu.clarussecure.dataoperations.testing;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class CloudOperations {
	public static List<BigDecimal> calculateX(List<BigDecimal> xPoints) {
		
		List<BigDecimal> resultList = new ArrayList<BigDecimal>();
		
		for(int i=0;i<xPoints.size();i++) {			
			for(int j=i;j<xPoints.size();j++) {
				BigDecimal x = (xPoints.get(j).subtract(xPoints.get(i))).pow(2);
				resultList.add(x);
			}
		}
		
		
		return resultList;
	}
	
	public static List<BigDecimal> calculateY(List<BigDecimal> yPoints, List<BigDecimal> xPoints) {
		List<BigDecimal> yList = new ArrayList<BigDecimal>();
		
		for(int i=0;i<yPoints.size();i++) {			
			for(int j=i;j<yPoints.size();j++) {
				BigDecimal y = (yPoints.get(j).subtract(yPoints.get(i))).pow(2);
				yList.add(y);
			}
		}
		
		return calculateDistance(xPoints, yList);
	}
	
	private static List<BigDecimal> calculateDistance(List<BigDecimal> xPoints, List<BigDecimal> yPoints) {
		List<BigDecimal> result = new ArrayList<BigDecimal>();
		for(int i=0;i<xPoints.size();i++) {
			result.add(sqrt(xPoints.get(i).add(yPoints.get(i))));
		}
		
		return result;
	}
	
	private static BigDecimal sqrt(BigDecimal x) {
		return BigDecimal.valueOf(StrictMath.sqrt(x.doubleValue()));
	}
	
//	private static BigDecimal calculateDistance(DBRegister point1, DBRegister point2)
//	{
//		return sqrt(point2.getX().subtract(point1.getX()).pow(2).add(point2.getY().subtract(point1.getY()).pow(2)));
//	}
}
