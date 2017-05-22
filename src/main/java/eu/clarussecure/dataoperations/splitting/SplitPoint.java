package eu.clarussecure.dataoperations.splitting;

/**
 * Created by Alberto Blanco on 28/04/2017.
 */
public class SplitPoint {
    private int x;
    private int y;

    public SplitPoint() {
        this.x = 0;
        this.y = 0;
    }

    public SplitPoint(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }
}
