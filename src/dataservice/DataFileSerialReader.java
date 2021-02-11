package dataservice;

import model.ConstValues;
import model.DataFileRecord;

import java.io.IOException;

public class DataFileSerialReader {
  private DataFilePageService pageService;
  private DataBuffer buffer;
  private DataFileRecord nextRecord = null;
  private int addressOfLastReadPrimaryRecord;
  private int primaryPageAddress;
  private boolean needToReloadPrimaryPage;

  public DataFileSerialReader(DataFilePageService pageService, DataBuffer buffer) {
    this.pageService = pageService;
    this.buffer = buffer;
  }

  public void loadPage(int address) throws IOException, ClassNotFoundException {
    pageService.setPage(buffer.loadPage(address));
    nextRecord = pageService.getRecordAtAddress(address);
    primaryPageAddress = address;
    addressOfLastReadPrimaryRecord = address;
    needToReloadPrimaryPage = false;
  }

  public DataFileRecord getNextRecord() throws IOException, ClassNotFoundException {
    DataFileRecord result = nextRecord != null ?
        new DataFileRecord(nextRecord.getRecord(), nextRecord.getPointer()) : null;
    if (nextRecord != null && nextRecord.getPointer() != ConstValues.nullValue) {
      needToReloadPrimaryPage = true;
      pageService.setPage(buffer.loadPageThatContainsAddress(nextRecord.getPointer()));
      nextRecord = pageService.getRecordAtAddress(nextRecord.getPointer());
    } else if (nextRecord != null) {
      if (needToReloadPrimaryPage) {
        int address = addressOfLastReadPrimaryRecord;
        loadPage(primaryPageAddress);
        addressOfLastReadPrimaryRecord = address;
        needToReloadPrimaryPage = false;
      }
      addressOfLastReadPrimaryRecord = pageService.getAddressOfNextRecord(addressOfLastReadPrimaryRecord);
      if (addressOfLastReadPrimaryRecord == ConstValues.nullValue) {
        nextRecord = null;
        return result;
      }
      nextRecord = pageService.getRecordAtAddress(addressOfLastReadPrimaryRecord);
    }
    return result;
  }
}
