package utilities;

import model.ComplexNumber;
import model.ComplexNumberSet;
import model.DataFileRecord;
import model.IndexFileRecord;

import java.io.*;

public class SerializationService {
  public static byte[] serializeRecord(ComplexNumberSet record) throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutput objectOutput = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutput.writeObject(record);
      return byteArrayOutputStream.toByteArray();
    }
  }

  public static ComplexNumberSet deserializeRecord(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
         ObjectInput objectInput = new ObjectInputStream(byteArrayInputStream)) {
      return (ComplexNumberSet) objectInput.readObject();
    }
  }

  public static int getRecordSize() throws IOException {
    ComplexNumber[] cnArray = {new ComplexNumber(1.0, 2.0)};
    byte[] bytesArray = serializeRecord(
        new ComplexNumberSet(cnArray, 1));
    return bytesArray.length;
  }

  public static byte[] serializeIndexFileRecord(IndexFileRecord record) throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutput objectOutput = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutput.writeObject(record);
      return byteArrayOutputStream.toByteArray();
    }
  }

  public static IndexFileRecord deserializeIndexFileRecord(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
         ObjectInput objectInput = new ObjectInputStream(byteArrayInputStream)) {
      return (IndexFileRecord) objectInput.readObject();
    }
  }

  public static int getIndexFileRecordSize() throws IOException {
    IndexFileRecord indexFileRecord = new IndexFileRecord(100, 1000);
    byte[] bytesArray = serializeIndexFileRecord(indexFileRecord);
    return bytesArray.length;
  }

  public static byte[] serializeDataFileRecord(DataFileRecord record) throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutput objectOutput = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutput.writeObject(record);
      return byteArrayOutputStream.toByteArray();
    }
  }

  public static DataFileRecord deserializeDataFileRecord(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
         ObjectInput objectInput = new ObjectInputStream(byteArrayInputStream)) {
      return (DataFileRecord) objectInput.readObject();
    }
  }

  public static int getDataFileRecordSize() throws IOException {
    ComplexNumber[] cnArray = {new ComplexNumber(1.0, 2.0)};
    ComplexNumberSet cns = new ComplexNumberSet(cnArray, 10);
    DataFileRecord dataFileRecord = new DataFileRecord(cns,1000);
    byte[] bytesArray = serializeDataFileRecord(dataFileRecord);
    return bytesArray.length;
  }
}
