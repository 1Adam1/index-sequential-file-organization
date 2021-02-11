package dataservice;

import model.DataFileRecord;
import utilities.SerializationService;

import java.io.IOException;
import java.util.Arrays;

public class DataFilePage {
  private DataFileRecord[] dataFileRecords;
  private int address;
  private int pageSize;
  private int dataFileRecordSize;
  private double alphaCoefficient = 0.5;
  private int maxPossibleNumberOfRecords;

  public DataFilePage(byte[] binaryData, int pageSize, int address) throws IOException, ClassNotFoundException {
    this.address = address;
    this.pageSize = pageSize;
    dataFileRecordSize = SerializationService.getDataFileRecordSize();
    maxPossibleNumberOfRecords = pageSize / dataFileRecordSize;
    translateBinaryDataToDataFileRecords(binaryData);
  }

  private void translateBinaryDataToDataFileRecords(byte[] binaryData) throws IOException, ClassNotFoundException {
    dataFileRecords = new DataFileRecord[maxPossibleNumberOfRecords];

    for (int i = 0; i < maxPossibleNumberOfRecords; i++) {
      byte[] bytes = Arrays.copyOfRange(binaryData, dataFileRecordSize * i, dataFileRecordSize * (i + 1));
      byte[] arrayOfZeroBytes = new byte[dataFileRecordSize];

      if (Arrays.equals(bytes, arrayOfZeroBytes)) {
        break;
      }
      DataFileRecord record = SerializationService.deserializeDataFileRecord(bytes);
      dataFileRecords[i] = record;
    }
  }

  public byte[] getBinaryData() throws IOException {
    return translateDataFileRecordsToBinaryData();
  }

  private byte[] translateDataFileRecordsToBinaryData() throws IOException {
    byte[] binaryData = new byte[pageSize];

    for (int i = 0; i < maxPossibleNumberOfRecords; i++) {
      byte[] bytes = new byte[dataFileRecordSize];

      if (dataFileRecords[i] != null) {
        bytes = SerializationService.serializeDataFileRecord(dataFileRecords[i]);
      }
      for (int j = 0; j < dataFileRecordSize; j++) {
        binaryData[i * dataFileRecordSize + j] = bytes[j];
      }
    }
    return binaryData;
  }

  public int getAddress() {
    return address;
  }

  public DataFileRecord[] getDataFileRecords() {
    return dataFileRecords;
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getMaxNumberPageOfRecordsAfterReorganisation() {
    return (int)(alphaCoefficient * Math.floor(pageSize / dataFileRecordSize));
  }

  public boolean isMaxNumberOfBytesAfterReorganisationReached() {
    return getSizeOfNotNullData() >= alphaCoefficient * Math.floor(pageSize / dataFileRecordSize);
  }

  private int getSizeOfNotNullData() {
    int numberOfRecords = 0;

    for (int i = 0; i < maxPossibleNumberOfRecords; i++) {
      if (dataFileRecords[i] == null) {
        break;
      }
      numberOfRecords++;
    }
    return numberOfRecords;
  }
}
