package dataservice;

import model.ConstValues;

import java.io.IOException;
import java.io.RandomAccessFile;

public class DataBuffer {
  private final int pageSize = 10000;
  private byte[] buffer = new byte[pageSize];
  private RandomAccessFile file;
  private int currentPageAddress = ConstValues.nullValue;
  private int numberOfPrimaryPages;

  public DataBuffer(String filename, boolean fileShouldBeCleared, int initialNumberOfPrimaryPages) throws IOException {
    file = new RandomAccessFile(filename, "rw");

    if (fileShouldBeCleared) {
      file.setLength(0);
    }
    numberOfPrimaryPages = initialNumberOfPrimaryPages;
  }

  public void closeBuffer() throws IOException {
    file.close();
  }

  public DataFilePage loadPageThatContainsAddress(int address) throws IOException, ClassNotFoundException {
    int pageAddress = address - (address % pageSize);
    return loadPage(pageAddress);
  }

  public DataFilePage loadLastPage() throws IOException, ClassNotFoundException {
    return loadPage(getAddressOfLastPage());
  }

  public int getAddressOfLastPage() throws IOException {
    return ((int) file.length()) - pageSize;
  }

  public int getAddressOfNextPage(int address) throws IOException {
    if (address >= getAddressOfLastPage()) {
      return ConstValues.nullValue;
    } else {
      return address - (address % pageSize) + pageSize;
    }
  }

  public DataFilePage loadPage(int address) throws IOException, ClassNotFoundException {
    if (currentPageAddress != address) {
      currentPageAddress = address;
      file.seek(address);
      file.read(buffer);
    }
    return new DataFilePage(buffer, pageSize, address);
  }

  public void savePage(int address, byte[] binaryData) throws IOException {
    buffer = binaryData;
    file.seek(address);
    file.write(buffer);
  }

  public int reserveNewPage(boolean primaryPage) throws IOException {
    int addressOfNewPage = (int) file.length();

    savePage(addressOfNewPage, new byte[pageSize]);
    if (primaryPage) {
      numberOfPrimaryPages++;
    }
    return addressOfNewPage;
  }

  public int getAddressOfOverflow() {
    return numberOfPrimaryPages * pageSize;
  }

  public int getNumberOfPrimaryPages() {
    return numberOfPrimaryPages;
  }
}
