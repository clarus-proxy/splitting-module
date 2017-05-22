package eu.clarussecure.dataoperations.kriging;

import eu.clarussecure.dataoperations.splitting.DBRecord;

import java.util.List;

public class MatrixOperations 
{
	public static double[][] prepareMatrix(List<PointDistance> points, int numberOfPoints) {
		double [][] result = new double[numberOfPoints][numberOfPoints];
		int index = 0;
		for(int i=0;i<numberOfPoints;i++)
		{
			for(int j=i;j<numberOfPoints;j++)
			{
				result[i][j] = points.get(index).getAssociatedCovariance().doubleValue();
				result[j][i] = points.get(index).getAssociatedCovariance().doubleValue();
				index++;
			}
		}
		return result;
	}
	
	//Transposing
	public static double[][] traspuesta(double a[][]){
		int fil_a = a.length;
		int col_a = a[0].length;
        double resultado[][] = new double[col_a][fil_a];
        
        for(int i=0; i<fil_a; i++){
            for(int j=0; j<col_a; j++){
                resultado[j][i] = a[i][j];
            }
        } 
        return resultado;
    }
	public static double[][] traspuesta(double a[]) {
		int fil_a = a.length;
        double resultado[][] = new double[1][fil_a];
        
        for(int i=0; i<fil_a; i++){
            resultado[0][i] = a[i];
        } 
        return resultado;
	}
	//Multiplication
	public static double[] producto(double a[][], double v[]){
		int fil_a = a.length;
		int col_a = a[0].length;
		int fil_v = v.length;
		
		if (col_a != fil_v){
			System.out.println("No se pueden multiplicar matriz por vector");
		}
		
        double b[] = new double[fil_a];
        
        for(int i=0; i<fil_a; i++){
            for(int j=0; j<col_a; j++){
                b[i] += a[i][j] * v[j];
            }
        }
        return b;
    }
	
	public static double[][] prepareKMatrix(double[][] matrix) {
		double [][] result = new double[matrix[0].length][matrix[0].length];
		for(int i=1;i < matrix[0].length;i++) {
			for(int j = 1;j < matrix[0].length;j++) {
				result[i-1][j-1] = matrix[i][j];
			}
			result[i-1][matrix[0].length-1] = 1;
		}
		for(int i = 0; i < matrix[0].length; i++) {
			if(i != matrix[0].length-1) result[matrix[0].length-1][i] = 1;
			else result[matrix[0].length-1][i] = 0.00;
		}
		return result;
	}
	
	public static double[] prepareV0Matrix(double[][] matrix) {
		double[] result = new double[matrix[0].length];
		for(int i=0;i<matrix[0].length;i++) {
			if(i != matrix[0].length-1) result[i] = matrix[0][i+1];
			else result[i] = 1;
		}
		return result;
	}
	
	public static double zEstimation(double[] lambda, List<DBRecord> points) {
		double result = 0;
		for(int i=0;i<points.size();i++) {
			result += lambda[i]*points.get(i).getZ().doubleValue();
		}
		return result;
	}
 
    public static double[][] invert(double a[][]) 
    {
        int n = a.length;
        double x[][] = new double[n][n];
        double b[][] = new double[n][n];
        int index[] = new int[n];
        for (int i=0; i<n; ++i) 
            b[i][i] = 1;
 
 // Transform the matrix into an upper triangle
        gaussian(a, index);
 
 // Update the matrix b[i][j] with the ratios stored
        for (int i=0; i<n-1; ++i)
            for (int j=i+1; j<n; ++j)
                for (int k=0; k<n; ++k)
                    b[index[j]][k]
                    	    -= a[index[j]][i]*b[index[i]][k];
 
 // Perform backward substitutions
        for (int i=0; i<n; ++i) 
        {
            x[n-1][i] = b[index[n-1]][i]/a[index[n-1]][n-1];
            for (int j=n-2; j>=0; --j) 
            {
                x[j][i] = b[index[j]][i];
                for (int k=j+1; k<n; ++k) 
                {
                    x[j][i] -= a[index[j]][k]*x[k][i];
                }
                x[j][i] /= a[index[j]][j];
            }
        }
        return x;
    }
 
// Method to carry out the partial-pivoting Gaussian
// elimination.  Here index[] stores pivoting order.
 
    public static void gaussian(double a[][], int index[]) 
    {
        int n = index.length;
        double c[] = new double[n];
 
 // Initialize the index
        for (int i=0; i<n; ++i) 
            index[i] = i;
 
 // Find the rescaling factors, one from each row
        for (int i=0; i<n; ++i) 
        {
            double c1 = 0;
            for (int j=0; j<n; ++j) 
            {
                double c0 = Math.abs(a[i][j]);
                if (c0 > c1) c1 = c0;
            }
            c[i] = c1;
        }
 
 // Search the pivoting element from each column
        int k = 0;
        for (int j=0; j<n-1; ++j) 
        {
            double pi1 = 0;
            for (int i=j; i<n; ++i) 
            {
                double pi0 = Math.abs(a[index[i]][j]);
                pi0 /= c[index[i]];
                if (pi0 > pi1) 
                {
                    pi1 = pi0;
                    k = i;
                }
            }
 
   // Interchange rows according to the pivoting order
            int itmp = index[j];
            index[j] = index[k];
            index[k] = itmp;
            for (int i=j+1; i<n; ++i) 	
            {
                double pj = a[index[i]][j]/a[index[j]][j];
 
 // Record pivoting ratios below the diagonal
                a[index[i]][j] = pj;
 
 // Modify other elements accordingly
                for (int l=j+1; l<n; ++l)
                    a[index[i]][l] -= pj*a[index[j]][l];
            }
        }
    }
}