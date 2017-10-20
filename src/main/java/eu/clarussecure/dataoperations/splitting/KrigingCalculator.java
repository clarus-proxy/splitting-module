package eu.clarussecure.dataoperations.splitting;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KrigingCalculator {

    public static KrigingResult calculate(String[] x, String[] y, String[] z, String[] c, String p) {
        List<DBRecord> points = IntStream.range(0, x.length).mapToObj(i -> new DBRecord(x[i], y[i], z[i]))
                .collect(Collectors.toList());
        BigDecimal xToCalculate = new BigDecimal(p.split(",")[0]);
        BigDecimal yToCalculate = new BigDecimal(p.split(",")[1]);
        List<BigDecimal> calculatedOncloud = Arrays.stream(c).map(BigDecimal::new).collect(Collectors.toList());

        return calculateKrigingStep3(points, xToCalculate, yToCalculate, calculatedOncloud);
    }

    public static KrigingResult calculateKrigingStep3(List<DBRecord> points, BigDecimal xToCalculate,
            BigDecimal yToCalculate, List<BigDecimal> calculatedOnCloud) {
        BigDecimal zVariance = calculateVariance(points, calculateAverage(points, "z"), "z");
        List<DBRecord> pointsWithM0 = new ArrayList<DBRecord>(points);
        pointsWithM0.add(0, new DBRecord(xToCalculate, yToCalculate, new BigDecimal(0)));

        BigDecimal xVariance = calculateVariance(pointsWithM0, calculateAverage(points, "x"), "x");
        BigDecimal yVariance = calculateVariance(pointsWithM0, calculateAverage(points, "y"), "y");
        List<PointDistance> pointsAllCloud = new ArrayList<PointDistance>();
        BigDecimal maxDistance = sqrt(xToCalculate.add(xVariance).add(xToCalculate).pow(2)
                .add(yToCalculate.add(yVariance).add(yToCalculate).pow(2)));
        BigDecimal minDistance = sqrt(xToCalculate.subtract(xVariance).add(xToCalculate).pow(2)
                .add(yToCalculate.subtract(yVariance).add(yToCalculate).pow(2)));
        int numberOfPoints = pointsWithM0.size();

        List<PointDistance> pointsInRangeCloud = new ArrayList<PointDistance>();
        int furthestDistanceCloud = 0;

        // Distancias punto 0

        List<BigDecimal> m0Distances = new ArrayList<BigDecimal>();

        for (int j = 0; j < pointsWithM0.size(); j++) {
            m0Distances.add(calculateDistance(pointsWithM0.get(0), pointsWithM0.get(j)));
        }

        // unir con dists calculadas en cloud
        calculatedOnCloud.addAll(0, m0Distances);

        int index = 0;
        for (int i = 0; i < pointsWithM0.size(); i++) {
            for (int j = i; j < pointsWithM0.size(); j++) {
                BigDecimal distance = calculatedOnCloud.get(index);
                pointsAllCloud.add(new PointDistance(pointsWithM0.get(i), pointsWithM0.get(j), distance));
                if (i != 0 && distance.compareTo(minDistance) == 1 && distance.compareTo(maxDistance) == -1) {
                    pointsInRangeCloud.add(new PointDistance(pointsWithM0.get(i), pointsWithM0.get(j), distance));
                    if (distance.intValue() > furthestDistanceCloud)
                        furthestDistanceCloud = distance.intValue();
                }
                index++;
            }
        }

        calculateExperimentalGamma(pointsInRangeCloud, furthestDistanceCloud);

        calculateSphericalGamma(pointsAllCloud);

        calculateAssociatedVariance(pointsAllCloud, zVariance);

        // For the rest of operations we need a matrix.
        double[][] matrix = MatrixOperations.prepareMatrix(pointsAllCloud, numberOfPoints);

        double[][] kMatrix = MatrixOperations.prepareKMatrix(matrix);

        double[] v0Matrix = MatrixOperations.prepareV0Matrix(matrix);

        double[][] inverseKMatrix = MatrixOperations.invert(kMatrix);

        double[] lambda0 = MatrixOperations.producto(inverseKMatrix, v0Matrix);

        double zEstimation = MatrixOperations.zEstimation(lambda0, points);

        // Estimation of value Z on point 0
        // System.out.println(zEstimation);

        // Here we calculate the Kriegage variance.
        double[][] lambda0Transposed = MatrixOperations.traspuesta(lambda0);

        double[] lambdaProduct = MatrixOperations.producto(lambda0Transposed, v0Matrix);

        double kriegage = zVariance.doubleValue() - lambdaProduct[0];

        KrigingResult res = new KrigingResult(zEstimation, kriegage);
        res.ZEstimation = zEstimation;

        return res;
    }

    private static BigDecimal calculateAverage(List<DBRecord> points, String field) {
        BigDecimal avg = new BigDecimal(0);
        Iterator<DBRecord> iterator = points.iterator();
        while (iterator.hasNext()) {
            DBRecord reg = iterator.next();
            switch (field) {
            case "x":
                avg = avg.add(reg.getX());
                break;
            case "y":
                avg = avg.add(reg.getY());
                break;
            case "z":
                avg = avg.add(reg.getZ());
                break;
            }
        }
        avg = avg.divide(new BigDecimal(points.size()), Constants.precision, RoundingMode.HALF_UP);
        return avg;
    }

    private static BigDecimal calculateVariance(List<DBRecord> points, BigDecimal avg, String field) {
        BigDecimal sumDiffsSquared = new BigDecimal(0);
        for (DBRecord reg : points) {
            BigDecimal diff = null;
            switch (field) {
            case "x":
                diff = reg.getX().subtract(avg);
                break;
            case "y":
                diff = reg.getY().subtract(avg);
                break;
            case "z":
                diff = reg.getZ().subtract(avg);
                break;
            }

            diff = diff.multiply(diff);
            sumDiffsSquared = sumDiffsSquared.add(diff);
        }
        return sumDiffsSquared.divide(new BigDecimal(points.size() - 1), Constants.precision, RoundingMode.HALF_UP);
    }

    private static BigDecimal calculateDistance(DBRecord point1, DBRecord point2) {
        return sqrt(point2.getX().subtract(point1.getX()).pow(2).add(point2.getY().subtract(point1.getY()).pow(2)));
    }

    private static void calculateExperimentalGamma(List<PointDistance> points, int furthestDistance) {

        for (int i = 1; i < furthestDistance + 1; i++) {
            List<PointDistance> bufferOfPoints = new ArrayList<PointDistance>();
            for (int j = 0; j < points.size(); j++) {
                PointDistance p = points.get(j);
                if (p.getDistance().intValue() == i) {
                    bufferOfPoints.add(p);
                }
            }
            BigDecimal result = new BigDecimal(0);
            if (bufferOfPoints.size() > 0) {
                for (PointDistance p : bufferOfPoints) {
                    result = result.add((p.getPoint1().getZ().subtract(p.getPoint2().getZ())).pow(2));
                }
                result = result.divide(new BigDecimal(bufferOfPoints.size() * 2));
            }
            // System.out.println("Result for "+i+": "+result);
        }
    }

    private static void calculateSphericalGamma(List<PointDistance> points) {
        for (int i = 0; i < points.size(); i++) {
            BigDecimal test1 = points.get(i).getDistance().multiply(new BigDecimal(3))
                    .divide(new BigDecimal(2 * Constants.c));
            BigDecimal test2 = (points.get(i).getDistance().pow(3))
                    .divide(new BigDecimal(Constants.c).pow(3).multiply(new BigDecimal(2)));
            BigDecimal result = new BigDecimal(0);
            if (test1.compareTo(result) != 0 && test2.compareTo(result) != 0)
                result = new BigDecimal(Constants.a).add((test1.subtract(test2)).multiply(new BigDecimal(Constants.b)));
            // System.out.println( ((points.get(i).getDistance().multiply(new
            // BigDecimal(3)).divide(new
            // BigDecimal(2*Constants.c))).subtract((points.get(i).getDistance().pow(3).divide(new
            // BigDecimal(2*(Constants.c^3)))))).multiply(new
            // BigDecimal(Constants.a+Constants.b)) );
            points.get(i).setGamma(result);
        }
    }

    private static void calculateAssociatedVariance(List<PointDistance> points, BigDecimal zVariance) {
        for (int i = 0; i < points.size(); i++) {
            points.get(i).setAssociatedCovariance(zVariance.subtract(points.get(i).getGamma()));
        }
    }

    // TODO: Comprovar la precisi�. (15-03-2016 Sembla prou precis)
    private static BigDecimal sqrt(BigDecimal x) {
        return BigDecimal.valueOf(StrictMath.sqrt(x.doubleValue()));
    }

}
