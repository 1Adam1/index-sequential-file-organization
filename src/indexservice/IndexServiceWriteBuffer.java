package indexservice;

import model.IndexFileRecord;
import utilities.SerializationService;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class IndexServiceWriteBuffer {
  private int bufferMaxSize = 10000;
  private int numberOfBytesInBuffer = 0;
  private byte[] buffer = new byte[bufferMaxSize];
  private SerializationService serializationService = new SerializationService();
  private OutputStream outputStream;
  private int numberOfWrites = 0;
  private String filename;
  private boolean writingStarted = false;

  public IndexServiceWriteBuffer(String filename) {
    this.filename = filename;
  }

  public void startWriting() throws IOException {
    if (writingStarted) {
      endWriting();
    }
    outputStream = new BufferedOutputStream(new FileOutputStream(filename));
    numberOfBytesInBuffer = 0;
    writingStarted = true;
  }

  public void endWriting() throws IOException {
    if (numberOfBytesInBuffer > 0) {
      saveBufferToFile();
    }
    outputStream.close();
    writingStarted = false;
  }

  public void addRecord(IndexFileRecord record) throws IOException {
    byte[] bytes = serializationService.serializeIndexFileRecord(record);
    addBytesToBuffer(bytes);
  }

  private void addBytesToBuffer(byte[] bytes) throws IOException {
    for (int i = 0; i < bytes.length; i++) {
      if (numberOfBytesInBuffer == bufferMaxSize) {
        saveBufferToFile();
      }
      buffer[numberOfBytesInBuffer] = bytes[i];
      numberOfBytesInBuffer++;
    }
  }

  private void saveBufferToFile() throws IOException {
    outputStream.write(buffer, 0, numberOfBytesInBuffer);
    buffer = new byte[bufferMaxSize];
    numberOfBytesInBuffer = 0;
    numberOfWrites++;
  }

  public int getNumberOfWrites() {
    return numberOfWrites;
  }
}
