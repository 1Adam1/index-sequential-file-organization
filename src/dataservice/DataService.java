package dataservice;

import exception.KeyAlreadyExistsException;
import exception.GuardRemovedException;
import model.ComplexNumber;
import model.ComplexNumberSet;
import model.ConstValues;
import model.DataFileRecord;
import utilities.SerializationService;

import java.io.IOException;

public class DataService {
  private DataBuffer buffer;
  private DataFilePageService dataFilePageService;
  private DataFileSerialReader dataFileSerialReader;
  private boolean guardPresent;
  private int numberOfOverflowPages;
  private int maxNumberOfOverflowPages;

  public DataService(String filename, boolean fileShouldBeCleared, int numberOfPrimaryPages) throws IOException {
    buffer = new DataBuffer(filename, fileShouldBeCleared, numberOfPrimaryPages);
    dataFilePageService = new DataFilePageService();
    dataFileSerialReader = new DataFileSerialReader(dataFilePageService, buffer);
    numberOfOverflowPages = 1;
    guardPresent = true;
  }

  public void setGuardPresent(boolean guardPresent) {
    this.guardPresent = guardPresent;
  }

  public int getMaxNumberOfOverflowPages() {
    return maxNumberOfOverflowPages;
  }

  public void setMaxNumberOfOverflowPages(int maxNumberOfOverflowPages) {
    this.maxNumberOfOverflowPages = maxNumberOfOverflowPages;
  }

  public void closeService() throws IOException {
    if (buffer != null) {
      savePageIfNotNull();
      buffer.closeBuffer();
    }
  }

  public void calculateMaxNumberOfOverflowPages(int numberOfRecords) throws IOException {
    int maxNumberOfRecordsOnPrimaryPage =
        dataFilePageService.getPage().getMaxNumberPageOfRecordsAfterReorganisation();
    int numberOfPrimaryPages = numberOfRecords / maxNumberOfRecordsOnPrimaryPage;

    this.maxNumberOfOverflowPages = 1 + (numberOfPrimaryPages / 5);
  }

  public boolean isReorganizationNeeded() {
    return !guardPresent || numberOfOverflowPages > maxNumberOfOverflowPages;
  }

  public int reserveNewPage(boolean primaryPage) throws IOException {
    return this.buffer.reserveNewPage(primaryPage);
  }

  private void savePageIfNotNullAndNewAddressIsDifferent(int newAddress) throws IOException {
    if (dataFilePageService.getPage() != null && dataFilePageService.getPage().getAddress() != newAddress) {
      DataFilePage page = dataFilePageService.getPage();
      buffer.savePage(page.getAddress(), page.getBinaryData());
    }
  }

  private void savePageIfNotNull() throws IOException {
    if (dataFilePageService.getPage() != null) {
      DataFilePage page = dataFilePageService.getPage();
      buffer.savePage(page.getAddress(), page.getBinaryData());
    }
  }

  public DataFileRecord getRecord(int key, int pageAddress) throws IOException, ClassNotFoundException {
    DataFileRecord dataFileRecord;

    savePageIfNotNullAndNewAddressIsDifferent(pageAddress);
    dataFilePageService.setPage(buffer.loadPageThatContainsAddress(pageAddress));
    if ((dataFileRecord = dataFilePageService.getRecordWithKey(key)) != null) {
      return dataFileRecord;
    }

    dataFileRecord = dataFilePageService.getRecordWithGreatestLowerKey(key);
    if (dataFileRecord.getPointer() == ConstValues.nullValue) {
      return null;
    } else {
      return getRecord(key, dataFileRecord.getPointer());
    }
  }

  public void addGuard() throws IOException, ClassNotFoundException, GuardRemovedException, KeyAlreadyExistsException {
    ComplexNumberSet guard = new ComplexNumberSet(
        new ComplexNumber[]{new ComplexNumber(1,1)}, ConstValues.nullValue);
    addRecord(guard,0);
    guardPresent = true;
  }

  public void addRecord(ComplexNumberSet record, int pageAddress) throws KeyAlreadyExistsException,
      IOException, ClassNotFoundException, GuardRemovedException {
    int addressOfAddedRecord;

    savePageIfNotNull();
    dataFilePageService.setPage(buffer.loadPage(pageAddress));
    if (dataFilePageService.getRecordWithKey(record.getKey()) != null) {
      throw new KeyAlreadyExistsException();
    }

    try {
      addressOfAddedRecord = dataFilePageService.addRecord(record, ConstValues.nullValue, true);
    } catch (GuardRemovedException e) {
      guardPresent = false;
      savePageIfNotNull();
      return;
    }

    if (addressOfAddedRecord == ConstValues.nullValue) {
      DataFileRecord recordWithGreatestOfLowerKeys = dataFilePageService.getRecordWithGreatestLowerKey(record.getKey());
      addRecordToOverflow(record,
          dataFilePageService.getAddressOfRecord(recordWithGreatestOfLowerKeys),
          recordWithGreatestOfLowerKeys.getPointer());
    }
    savePageIfNotNull();
  }

  private void addRecordToOverflow(ComplexNumberSet record, int primaryAreaRecordAddress, int recordInOverflowAddress)
      throws IOException, ClassNotFoundException, KeyAlreadyExistsException, GuardRemovedException {
    int currentAddress = recordInOverflowAddress;
    int previousAddress = primaryAreaRecordAddress;
    DataFileRecord currentRecord;

    if (recordInOverflowAddress == ConstValues.nullValue) {
      int addressOfAddedRecord = addRecordAtEndOfOverflow(record, ConstValues.nullValue);
      updatePointerOfDataFileRecord(primaryAreaRecordAddress, addressOfAddedRecord);
      return;
    }

    while (true) {
      currentRecord = dataFilePageService.getRecordAtAddress(currentAddress);
      if (currentRecord == null) {
        savePageIfNotNull();
        dataFilePageService.setPage(buffer.loadPageThatContainsAddress(currentAddress));
      } else if (currentRecord.getRecord().getKey() > record.getKey()) {
        int addressOfAddedRecord = addRecordAtEndOfOverflow(record, currentAddress);
        updatePointerOfDataFileRecord(previousAddress, addressOfAddedRecord);
        return;
      } else if (currentRecord.getPointer() == ConstValues.nullValue) {
        int addressOfAddedRecord = addRecordAtEndOfOverflow(record, ConstValues.nullValue);
        updatePointerOfDataFileRecord(currentAddress, addressOfAddedRecord);
        return;
      } else {
        previousAddress = currentAddress;
        currentAddress = currentRecord.getPointer();
      }
    }
  }

  private int addRecordAtEndOfOverflow(ComplexNumberSet record, int addressOfNextRecord) throws IOException,
      ClassNotFoundException, KeyAlreadyExistsException, GuardRemovedException {
    int addressOfAddedRecord;

    savePageIfNotNullAndNewAddressIsDifferent(buffer.getAddressOfLastPage());
    dataFilePageService.setPage(buffer.loadLastPage());
    addressOfAddedRecord = dataFilePageService.addRecord(record, addressOfNextRecord, false);
    if (addressOfAddedRecord == ConstValues.nullValue) {
      reserveNewPage(false);
      numberOfOverflowPages++;
      dataFilePageService.setPage(buffer.loadLastPage());
      return dataFilePageService.addRecord(record, addressOfNextRecord, false);
    }
    return addressOfAddedRecord;
  }

  private void updatePointerOfDataFileRecord(int address, int newPointer) throws IOException, ClassNotFoundException {
    savePageIfNotNull();
    dataFilePageService.setPage(buffer.loadPageThatContainsAddress(address));
    dataFilePageService.updatePointerOfDataFileRecord(address, newPointer);
  }

  public void loadPageForSerialReading(int address) throws IOException, ClassNotFoundException {
    dataFileSerialReader.loadPage(address);
  }

  public DataFileRecord getNextRecordForSerialReading() throws IOException, ClassNotFoundException {
    return dataFileSerialReader.getNextRecord();
  }

  public boolean isMaxNumberOfBytesAfterReorganisationReached() {
    return dataFilePageService.getPage() == null || dataFilePageService.getPage().isMaxNumberOfBytesAfterReorganisationReached();
  }

  public void printFile() throws IOException, ClassNotFoundException {
    int currentAddress = 0;
    int primaryPageCounter = 1;
    int overflowPageCounter = 1;

    savePageIfNotNullAndNewAddressIsDifferent(currentAddress);
    System.out.println("Zawartość pliku z danymi:");
    do {
      dataFilePageService.setPage(buffer.loadPage(currentAddress));
      if (currentAddress < buffer.getAddressOfOverflow()) {
        System.out.println();
        System.out.println("Strona obszaru głównego nr " + primaryPageCounter + ":");
        primaryPageCounter++;
      } else {
        System.out.println();
        System.out.println("Strona obszaru nadmiarowego nr " + overflowPageCounter + ":");
        overflowPageCounter++;
      }
      dataFilePageService.printRecords();
    } while ((currentAddress = buffer.getAddressOfNextPage(currentAddress)) != ConstValues.nullValue);
    System.out.println();
  }

  public int getNumberOfPrimaryPages() {
    return buffer.getNumberOfPrimaryPages();
  }
}
