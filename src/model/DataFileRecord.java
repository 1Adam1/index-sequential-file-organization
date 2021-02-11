package model;

import java.io.Serializable;

public class DataFileRecord implements Serializable, Comparable {
  private ComplexNumberSet record;
  private int pointer;

  public DataFileRecord(ComplexNumberSet record, int pointer) {
    this.record = record;
    this.pointer = pointer;
  }

  public ComplexNumberSet getRecord() {
    return record;
  }

  public void setRecord(ComplexNumberSet record) {
    this.record = record;
  }

  public int getPointer() {
    return pointer;
  }

  public void setPointer(int pointer) {
    this.pointer = pointer;
  }

  @Override
  public int compareTo(Object otherRecord) {
    return record.getKey() - ((DataFileRecord) otherRecord).getRecord().getKey();
  }
}
