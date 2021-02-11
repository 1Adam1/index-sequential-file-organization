package model;

import java.io.Serializable;
import java.util.Objects;

public class ComplexNumber implements Serializable {
  private double realPart;
  private double imaginaryPart;
  private double modulus;

  public ComplexNumber(double realPart, double imaginaryPart) {
    this.realPart = realPart;
    this.imaginaryPart = imaginaryPart;
    this.modulus = countModulus();
  }

  public double getRealPart() {
    return realPart;
  }

  public double getImaginaryPart() {
    return imaginaryPart;
  }

  public double getModulus() {
    return modulus;
  }

  private double countModulus() {
    return Math.sqrt(Math.pow(realPart, 2) + Math.pow(imaginaryPart, 2));
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj instanceof ComplexNumber) {
      ComplexNumber otherComplexNumber = (ComplexNumber) obj;
      return realPart == otherComplexNumber.getRealPart() && imaginaryPart == otherComplexNumber.getImaginaryPart();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(realPart, imaginaryPart);
  }
}
