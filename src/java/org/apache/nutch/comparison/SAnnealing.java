/**
	 * @author sertug
	 * @author aykut
	 * @author cengiz
	 */

package org.apache.nutch.comparison;

import java.io.IOException;

public class SAnnealing {
	double[] coefficients = null;
	double[] newCoefficients = null;
	int iteration;
	double oldFitness;
	int temperature;
	double energydiff;
	static Kendall tau = null;
	
	public SAnnealing(double[] coefficients, String[] args) throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException{
		this.coefficients = new double[coefficients.length];
		this.newCoefficients = new double[coefficients.length];
		this.coefficients[0] = 0.33;
		this.coefficients[1] = 0.33;
		this.coefficients[2] = 0.33;
		newCoefficients[0] = this.coefficients[0];
		newCoefficients[1] = this.coefficients[1];
		newCoefficients[2] = this.coefficients[2];
		iteration = 0;
		tau = new Kendall();
		oldFitness = fitness(this.coefficients, args, tau);
		temperature = 100;
		
	}
	
	
	public double[] getNeighbor(){
			int index = (int)(Math.random() * coefficients.length);
			if((int)(Math.random() * 100) % 2 == 0){
				newCoefficients[index] = coefficients[index] + 0.05;
			} else {
				newCoefficients[index] = coefficients[index] - 0.05 >= 0 ? coefficients[index] - 0.05 : coefficients[index];
			}
		return newCoefficients;
	}
	
	public void checkNeighbor(String[] args, Kendall tau) throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException{
		double r = fitness(newCoefficients, args, tau);
		if(r > oldFitness){
			coefficients[0] = newCoefficients[0];
			coefficients[1] = newCoefficients[1];
			coefficients[2] = newCoefficients[2];
		} else {
			if(probability(temperature)){
				coefficients[0] = newCoefficients[0];
				coefficients[1] = newCoefficients[1];
				coefficients[2] = newCoefficients[2];
			} 
		}
		oldFitness = r;
	}
	
	public void start(String[] args, Kendall tau) throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException{
		while(temperature > 0){
			getNeighbor();
			checkNeighbor(args, tau);
			temperature--;
			//System.out.println();
		}
	}
	
	public double fitness(double[] c, String[] args, Kendall tau) throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException{
		double result = tau.start(c, args);
		
		energydiff = result - oldFitness;
//		System.out.println("result: " + result + " oldFitness: " + oldFitness + " energyDiff: " + energydiff + " temp: " + temperature);
	//	if(energydiff > 0){
			System.out.println("coeffs: " + newCoefficients[0] + " " + newCoefficients[1] + " " + newCoefficients[2]);
			System.out.println("result: " + result);
		//}
		
		return result;
	}
	
	public boolean probability(int interation){
		double d = Math.exp(-1 * (10000 - temperature) * 0.0003);
		//System.out.println(" prob: " + d);
		if(Math.random() < d){
			return false;
		} else {
			//System.out.println("kabul edilmedi");
			return false;
		}
		
	}
	
	public static void main(String[] args) throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException{
		int cCount = 3;
		double[] coefficients = new double[cCount];
		
		SAnnealing sa = new SAnnealing(coefficients, args);
		sa.start(args, tau);
	}
}
