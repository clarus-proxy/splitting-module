package eu.clarussecure.dataoperations.splitting;

/**
 * Created by sergi on 25/04/2017.
 */
public class KrigingResult {
    double ZEstimation;
    double kriegageVvariance;

    public KrigingResult(double zEstimation, double kriegageVvariance) {
        this.ZEstimation = zEstimation;
        this.kriegageVvariance = kriegageVvariance;
    }

    public String toString() {
        String s;

        s = "Z estimation: " + this.ZEstimation + "\n";
        s += "Kriegage variance: " + this.kriegageVvariance;

        return s;
    }
}
