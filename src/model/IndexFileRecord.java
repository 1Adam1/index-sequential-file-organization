package model;

import java.io.Serializable;

public class IndexFileRecord implements Serializable {
  private int key;
  private int pointer;

  public IndexFileRecord(int key, int pointer) {
    this.pointer = pointer;
    this.key = key;
  }

  public int getPointer() {
    return pointer;
  }

  public void setPointer(int pointer) {
    this.pointer = pointer;
  }

  public int getKey() {
    return key;
  }
}
