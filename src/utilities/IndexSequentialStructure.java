package utilities;

import dataservice.DataService;
import exception.GuardRemovedException;
import exception.KeyAlreadyExistsException;
import indexservice.IndexService;
import model.*;

import java.io.File;
import java.io.IOException;

public class IndexSequentialStructure {
  private DataService dataService;
  private IndexService indexService;
  private String indexFilename;
  private String dataFilename;

  public IndexSequentialStructure(String indexFilename, String dataFilename, boolean allDataShouldBeCleared)
      throws IOException, KeyAlreadyExistsException, ClassNotFoundException, GuardRemovedException {
    int numberOfPrimaryPages;

    this.indexFilename = indexFilename;
    this.dataFilename = dataFilename;
    this.indexService = new IndexService(indexFilename, allDataShouldBeCleared);
    numberOfPrimaryPages = allDataShouldBeCleared ? indexService.countNumberOfPrimaryPages() : 0;
    this.dataService = new DataService(dataFilename, allDataShouldBeCleared, numberOfPrimaryPages);
    if (allDataShouldBeCleared || !new File(dataFilename).exists() || !new File(indexFilename).exists()) {
      initializeStructure(indexService, dataService);
    } else {
      reorganise(true);
    }
  }

  private void initializeStructure(IndexService indexService, DataService dataService)
      throws IOException, KeyAlreadyExistsException, ClassNotFoundException, GuardRemovedException {
    indexService.startWriting();
    indexService.writeRecord(ConstValues.nullValue, 0);
    indexService.endWriting();
    dataService.reserveNewPage(true);
    dataService.addGuard();
    dataService.reserveNewPage(false);
    dataService.calculateMaxNumberOfOverflowPages(0);
  }

  public void closeStructure() throws IOException {
    dataService.closeService();
  }

  public void reorganise(boolean filesShouldBePrinted) throws IOException, ClassNotFoundException, KeyAlreadyExistsException, GuardRemovedException {
    File dataFile, indexFile;
    IndexService newIndexService = new IndexService("new_" + indexFilename, true);
    DataService newDataService = new DataService("new_" + dataFilename, true, 0);
    int numberOfPrimaryPages;

    if (filesShouldBePrinted) {
      System.out.println("REORGANIZACJA");
      System.out.println("Zawartość plików przed reorganizacją:");
      printFiles();
    }

    migrateRecordsToNewFiles(newIndexService, newDataService);
    dataFile = new File(dataFilename);
    indexFile = new File(indexFilename);
    dataFile.delete();
    indexFile.delete();

    if (!new File("new_" + indexFilename).renameTo(indexFile) ||
        !new File("new_" + dataFilename).renameTo(dataFile)) {
      throw new IOException();
    }
    numberOfPrimaryPages = newDataService.getNumberOfPrimaryPages();
    this.dataService = new DataService(dataFilename, false, numberOfPrimaryPages);
    this.dataService.setGuardPresent(true);
    this.dataService.setMaxNumberOfOverflowPages(newDataService.getMaxNumberOfOverflowPages());
    this.indexService = new IndexService(indexFilename, false);

    if (filesShouldBePrinted) {
      System.out.println("Zawartość po reorganizacji:");
      printFiles();
    }
  }

  private void migrateRecordsToNewFiles(IndexService newIndexService, DataService newDataService)
      throws IOException, ClassNotFoundException, KeyAlreadyExistsException, GuardRemovedException {

    IndexFileRecord indexFileRecord;
    DataFileRecord dataFileRecord;
    int currentPageAddress;
    int currentPageAddressInNewDataFile;
    int numberOfRecords = 0;

    indexService.startReading();
    newIndexService.startWriting();
    try {
      currentPageAddressInNewDataFile = 0;
      newDataService.reserveNewPage(true);
      newDataService.addGuard();
      indexFileRecord = indexService.readRecord();
      newIndexService.writeRecord(ConstValues.nullValue, 0);
      while (true) {
        if (indexFileRecord == null) {
          break;
        }
        currentPageAddress = indexFileRecord.getPointer();
        dataService.loadPageForSerialReading(currentPageAddress);
        while (true) {
          if ((dataFileRecord = dataService.getNextRecordForSerialReading()) == null) {
            break;
          } else if (dataFileRecord.getRecord().getKey() == ConstValues.nullValue) {
            continue;
          } else if (newDataService.isMaxNumberOfBytesAfterReorganisationReached()) {
            currentPageAddressInNewDataFile = newDataService.reserveNewPage(true);
            newIndexService.writeRecord(dataFileRecord.getRecord().getKey(), currentPageAddressInNewDataFile);
          }
          newDataService.addRecord(dataFileRecord.getRecord(), currentPageAddressInNewDataFile);
          numberOfRecords++;
        }
        indexFileRecord = indexService.readRecord();
      }
      newDataService.calculateMaxNumberOfOverflowPages(numberOfRecords);
      newDataService.reserveNewPage(false);
    } finally {
      indexService.endReading();
      newIndexService.endWriting();
      dataService.closeService();
      newDataService.closeService();
    }
  }

  public ComplexNumberSet getRecord(int key) throws IOException, ClassNotFoundException {
    ComplexNumberSet record;
    DataFileRecord dataFileRecord;

    indexService.startReading();
    try {
      dataFileRecord = dataService.getRecord(key, indexService.getAddressOfProperPage(key));
      record = dataFileRecord == null ? null : dataFileRecord.getRecord();
    } finally {
      indexService.endReading();
    }
    return record;
  }

  public void addRecord(ComplexNumberSet record) throws IOException, ClassNotFoundException, GuardRemovedException {
    indexService.startReading();
    try {
      dataService.addRecord(record, indexService.getAddressOfProperPage(record.getKey()));
      indexService.endReading();
      if (dataService.isReorganizationNeeded()) {
        reorganise(true);
      }
    } catch (KeyAlreadyExistsException e) {
      System.out.println("Rekord z kluczem o wartości " + record.getKey() + " już istnieje");
      indexService.endReading();
    }
  }

  public void printData() throws IOException, ClassNotFoundException {
    int index = 1;
    IndexFileRecord indexFileRecord;
    DataFileRecord dataFileRecord;

    System.out.println();
    System.out.println("Uporządkowane rekordy bez dodatkowych informacji: ");
    indexService.startReading();
    try {
      while (true) {
        if ((indexFileRecord = indexService.readRecord()) == null) {
          break;
        }
        dataService.loadPageForSerialReading(indexFileRecord.getPointer());
        while (true) {
          if ((dataFileRecord = dataService.getNextRecordForSerialReading()) == null) {
            break;
          }
          System.out.println(index + ") Klucz: " + dataFileRecord.getRecord().getKey()
              + ", " + dataFileRecord.getRecord().toString());
          index++;
        }
      }
    } finally {
      indexService.endReading();
    }
    System.out.println();
  }

  public void printFiles() throws IOException, ClassNotFoundException {
    indexService.printFile();
    dataService.printFile();
  }
}
