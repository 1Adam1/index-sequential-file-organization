package model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class ComplexNumberSet implements Serializable {
  private static final int setMaxSize = 15;
  private ComplexNumber[] set = new ComplexNumber[setMaxSize];
  private int numberOfItemsInSet;
  private double variablesRange;
  private int key;

  public ComplexNumberSet(ComplexNumber[] set, int key) {
    if (set.length < setMaxSize) {
      numberOfItemsInSet = set.length;
      System.arraycopy(set, 0, this.set, 0, set.length);

      for (int i = 0; i < (setMaxSize - set.length); i++) {
        this.set[set.length + i] = new ComplexNumber(0, 0);
      }
    } else {
      numberOfItemsInSet = setMaxSize;
      System.arraycopy(set, 0, this.set, 0, setMaxSize);
    }
    variablesRange = calculateVariableRange();
    this.key = key;
  }

  public ComplexNumber[] getSet() {
    return set;
  }

  public static int getSetMaxSize() {
    return setMaxSize;
  }

  public int getNumberOfItemsInSet() {
    return numberOfItemsInSet;
  }

  public double getVariablesRange() {
    return variablesRange;
  }

  public int getKey() {
    return key;
  }

  private double calculateVariableRange() {
    return getMaxNumber().getModulus() - getMinNumber().getModulus();
  }

  private ComplexNumber getMinNumber() {
    if (numberOfItemsInSet == 0) {
      return null;
    }

    ComplexNumber minElement = set[0];
    for (int i = 0; i < numberOfItemsInSet; i++) {
      ComplexNumber number = set[i];
      minElement = number.getModulus() < minElement.getModulus() ? number : minElement;
    }
    return minElement;
  }

  private ComplexNumber getMaxNumber() {
    if (numberOfItemsInSet == 0) {
      return null;
    }

    ComplexNumber maxElement = set[0];
    for (int i = 0; i < numberOfItemsInSet; i++) {
      ComplexNumber number = set[i];
      maxElement = number.getModulus() > maxElement.getModulus() ? number : maxElement;
    }
    return maxElement;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj instanceof ComplexNumberSet) {
      ComplexNumberSet otherComplexNumberSet = (ComplexNumberSet) obj;
      boolean result = getNumberOfItemsInSet() == otherComplexNumberSet.getNumberOfItemsInSet() &&
          getVariablesRange() == otherComplexNumberSet.getVariablesRange() &&
          Arrays.equals(getSet(), otherComplexNumberSet.getSet());

      return result;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSetMaxSize(), getSet(), getNumberOfItemsInSet(), getVariablesRange());
  }

  @Override
  public String toString() {
    String result = "";
    result += "rozpiętość zbioru: " + variablesRange + " { ";
    for (int i = 0; i < numberOfItemsInSet; i ++) {
      result += "r = " + set[i].getRealPart() + ", ";
      result += "i = " + set[i].getImaginaryPart() + "; ";
    }
    result += "}";
    return result;
  }
}
