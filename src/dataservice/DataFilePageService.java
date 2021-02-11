package dataservice;

import exception.KeyAlreadyExistsException;
import exception.GuardRemovedException;
import model.ComplexNumberSet;
import model.ConstValues;
import model.DataFileRecord;
import utilities.SerializationService;

import java.io.IOException;
import java.util.Arrays;

public class DataFilePageService {
  private DataFilePage page = null;
  private DataFileRecord[] dataFileRecords;
  private int dataFileRecordSize;

  DataFilePageService() throws IOException {
    dataFileRecordSize = SerializationService.getDataFileRecordSize();
  }

  public void setPage(DataFilePage page) {
    this.page = page;
    dataFileRecords = page.getDataFileRecords();
  }

  public DataFilePage getPage() {
    return page;
  }

  public int addRecord(ComplexNumberSet record, int pointerToNextRecord, boolean pageRecordsShouldBeSorted)
      throws KeyAlreadyExistsException, GuardRemovedException {

    if (getRecordWithKey(record.getKey()) != null) {
      throw new KeyAlreadyExistsException();
    }
    if (page.getAddress() == 0 &&
        dataFileRecords[dataFileRecords.length - 1] != null &&
        record.getKey() < dataFileRecords[1].getRecord().getKey()) {
      replaceGuardWithNewRecord(record);
      return ConstValues.nullValue;
    } else if (dataFileRecords[dataFileRecords.length - 1] != null) {
      return ConstValues.nullValue;
    } else {
      return addNewRecordToDataFileRecords(record, pointerToNextRecord, pageRecordsShouldBeSorted);
    }
  }

  private void replaceGuardWithNewRecord(ComplexNumberSet record)
      throws GuardRemovedException {
    dataFileRecords[0] = new DataFileRecord(record, ConstValues.nullValue);
    throw new GuardRemovedException();
  }

  private int addNewRecordToDataFileRecords(ComplexNumberSet record, int pointerToNextRecord, boolean pageRecordsShouldBeSorted) {
    dataFileRecords[getIndexOfFirstNullDataFileRecordsElement()] = new DataFileRecord(record, pointerToNextRecord);
    if (pageRecordsShouldBeSorted) {
      sortDataFileRecords();
    }
    return getAddressOfRecord(getRecordWithKey(record.getKey()));
  }

  private void sortDataFileRecords() {
    int numberOfNotNullElements = ConstValues.nullValue;

    for (int i = dataFileRecords.length - 1; i >= 0; i--) {
      if (dataFileRecords[i] != null) {
        numberOfNotNullElements = i + 1;
        break;
      }
    }
    if (numberOfNotNullElements != ConstValues.nullValue) {
      DataFileRecord[] records = Arrays.copyOfRange(dataFileRecords, 0, numberOfNotNullElements);
      Arrays.sort(records);
      for (int i = 0; i < numberOfNotNullElements; i++) {
        dataFileRecords[i] = records[i];
      }
    }
  }

  private int getIndexOfFirstNullDataFileRecordsElement() {
    for (int i = 0; i < dataFileRecords.length; i++) {
      if (dataFileRecords[i] == null) {
        return i;
      }
    }
    return ConstValues.nullValue;
  }

  public DataFileRecord getRecordWithGreatestLowerKey(int key) {
    DataFileRecord record = null;

    for (int i = 0; i < dataFileRecords.length; i++) {
      if (dataFileRecords[i] == null || dataFileRecords[i].getRecord().getKey() > key) {
        return record;
      } else {
        record = dataFileRecords[i];
      }
    }
    return record;
  }

  public DataFileRecord getRecordWithKey(int key) {
    for (int i = 0; i < dataFileRecords.length; i++) {
      if (dataFileRecords[i] == null) {
        break;
      } else if (dataFileRecords[i].getRecord().getKey() == key) {
        return dataFileRecords[i];
      }
    }
    return null;
  }

  public DataFileRecord getRecordAtAddress(int address) {
    int pageAddress = page.getAddress();
    int index;

    if (address < pageAddress || address >= pageAddress + page.getPageSize()) {
      return null;
    } else {
      index = (address - pageAddress) / dataFileRecordSize;
      return dataFileRecords[index];
    }
  }

  public int getAddressOfRecord(DataFileRecord record) {
    int key = record.getRecord().getKey();

    for (int i = 0; i < dataFileRecords.length; i++) {
      if (dataFileRecords[i] == null) {
        break;
      } else if (dataFileRecords[i].getRecord().getKey() == key) {
        return page.getAddress() + i * dataFileRecordSize;
      }
    }
    return ConstValues.nullValue;
  }

  public void updatePointerOfDataFileRecord(int recordAddress, int pointer) {
    getRecordAtAddress(recordAddress).setPointer(pointer);
  }

  public int getAddressOfNextRecord(int address) {
    int nextRecordAddress = address + dataFileRecordSize;

    if (nextRecordAddress + dataFileRecordSize >= page.getAddress() + page.getPageSize()) {
      return ConstValues.nullValue;
    } else {
      return address + dataFileRecordSize;
    }
  }

  public void printRecords() {
    for (int i = 0; i < dataFileRecords.length; i++) {
      if (dataFileRecords[i] != null) {
        System.out.println((i + 1) + ") Klucz: " + dataFileRecords[i].getRecord().getKey() +
            ", wskaznik na obszar nadmiarowy: " + dataFileRecords[i].getPointer() +
            ", " + dataFileRecords[i].getRecord().toString());
      }
    }
  }
}
