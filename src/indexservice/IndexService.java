package indexservice;

import model.IndexFileRecord;

import java.io.IOException;

public class IndexService {
  private IndexServiceReadBuffer readBuffer;
  private IndexServiceWriteBuffer writeBuffer;
  private String filename;
  private boolean readingStarted = false;
  private boolean writingStarted = false;

  public IndexService(String filename, boolean fileShouldBeCleared) throws IOException {
    this.filename = filename;
    if (fileShouldBeCleared) {
      startWriting();
      endWriting();
    }
  }

  public void startWriting() throws IOException {
    if (writingStarted) {
      endWriting();
    }
    writeBuffer = new IndexServiceWriteBuffer(filename);
    writeBuffer.startWriting();
    writingStarted = true;
  }

  public void endWriting() throws IOException {
    writeBuffer.endWriting();
    writingStarted = false;
  }

  public void writeRecord(int key, int pointer) throws IOException {
    IndexFileRecord record = new IndexFileRecord(key, pointer);
    writeBuffer.addRecord(record);
  }

  public int getAddressOfProperPage(int key) throws IOException, ClassNotFoundException {
    int pageAddress = 0;
    IndexFileRecord record;

    startReading();
    while (true) {
      record = readRecord();
      if (record == null || key < record.getKey()) {
        endReading();
        return pageAddress;
      } else if (key == record.getKey()) {
        endReading();
        return record.getPointer();
      }
      pageAddress = record.getPointer();
    }
  }

  public void startReading() throws IOException {
    if (readingStarted) {
      endReading();
    }
    readBuffer = new IndexServiceReadBuffer(filename);
    readBuffer.startReading();
    readingStarted = true;
  }

  public void endReading() throws IOException {
    readBuffer.endReading();
    readingStarted = false;
  }

  public IndexFileRecord readRecord() throws IOException, ClassNotFoundException {
    return readBuffer.readRecord();
  }

  public void printFile() throws IOException, ClassNotFoundException {
    IndexFileRecord record;
    int counter = 1;

    System.out.println();
    System.out.println("Zawartość indeksu:");
    startReading();
    try {
      while ((record = readRecord()) != null) {
        System.out.println(counter + ") Klucz: " + record.getKey() + ", wskaźnik na stronę: " + record.getPointer());
        counter++;
      }
    } finally {
      endReading();
    }
    System.out.println();
  }

  public int countNumberOfPrimaryPages() throws IOException, ClassNotFoundException {
    int counter = 0;

    startReading();
    try {
      while (readRecord() != null) {
        counter++;
      }
    } finally {
      endReading();
    }
    return counter;
  }
}
