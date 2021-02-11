package indexservice;

import model.IndexFileRecord;
import utilities.SerializationService;

import java.io.*;

public class IndexServiceReadBuffer {
  private int bufferMaxSize = 10000;
  private int indexOfNextByteToRead;
  private byte[] buffer = new byte[bufferMaxSize];
  private boolean endOfFileOccurred = false;
  private int numberOfReadBytes;
  private SerializationService serializationService = new SerializationService();
  private InputStream inputStream;
  private int numberOfReads = 0;
  private String filename;
  private boolean readingStarted = false;

  public IndexServiceReadBuffer(String filename) {
    this.filename = filename;
  }

  public void startReading() throws IOException {
    if (readingStarted) {
      endReading();
    }
    inputStream = new BufferedInputStream(new FileInputStream(filename));
    numberOfReadBytes = 0;
    indexOfNextByteToRead = 0;
    readingStarted = true;
  }

  public void endReading() throws IOException {
    inputStream.close();
    readingStarted = false;
  }

  public IndexFileRecord readRecord() throws IOException, ClassNotFoundException {
    byte[] readBytes = readRecordBytesFromBuffer();
    if (readBytes == null) {
      return null;
    } else {
      return serializationService.deserializeIndexFileRecord(readBytes);
    }
  }

  private byte[] readRecordBytesFromBuffer() throws IOException {
    int recordSize = SerializationService.getIndexFileRecordSize();
    byte[] recordBytes = new byte[recordSize];
    for (int i = 0; i < recordSize; i++) {
      if (isNextByteAvailable()) {
        recordBytes[i] = buffer[indexOfNextByteToRead];
        indexOfNextByteToRead++;
      } else {
        return null;
      }
    }
    return recordBytes;
  }

  private boolean isNextByteAvailable() throws IOException {
    if (indexOfNextByteToRead == numberOfReadBytes) {
      loadFromFileToBuffer();
    }
    return !endOfFileOccurred;
  }

  private void loadFromFileToBuffer() throws IOException {
    numberOfReadBytes = inputStream.read(buffer);
    endOfFileOccurred = numberOfReadBytes == -1;
    indexOfNextByteToRead = 0;
    numberOfReads++;
  }

  public int getNumberOfReads() {
    return numberOfReads;
  }
}
