import exception.GuardRemovedException;
import exception.KeyAlreadyExistsException;
import model.ComplexNumber;
import model.ComplexNumberSet;
import utilities.IndexSequentialStructure;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class Main {
  private static IndexSequentialStructure indexSequentialStructure = null;
  private static Scanner scanner = new Scanner(System.in);

  public static void main(String[] args) {
    boolean programEnded = false;

    System.out.println();
    System.out.println("Implementacja struktury indeksowo-sekwencyjnej");
    System.out.println("Czy dane zapisane w strukturze mają zostać usunięte? (t - tak)");

    try {
      if (scanner.next().equals("t")) {
        indexSequentialStructure = new IndexSequentialStructure("index.bin", "data.bin", true);
      } else {
        indexSequentialStructure = new IndexSequentialStructure("index.bin", "data.bin", false);
      }
      while (!programEnded) {
        System.out.println();
        System.out.println("Menu:");
        System.out.println("1) Operacje podawane z klawiatury");
        System.out.println("2) Operacje wczytywane z pliku tekstowego");
        System.out.println("3) Usunięcie zapisanych danych");
        System.out.println("4) Zamknięcie programu");
        System.out.println("Proszę podać numer wybranej opcji");

        switch (scanner.nextInt()) {
          case 1:
            getOperationsFromKeyboard();
            break;
          case 2:
            getOperationsFromFile();
            break;
          case 3:
            indexSequentialStructure = new IndexSequentialStructure("index.bin", "data.bin", true);
            break;
          case 4:
            programEnded = true;
        }
      }
      indexSequentialStructure.closeStructure();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void getOperationsFromKeyboard()
      throws IOException, ClassNotFoundException, GuardRemovedException, KeyAlreadyExistsException {
    ComplexNumberSet record;
    boolean alwaysUseDefaultComplexNumberArray;
    boolean printFilesAfterModification;
    boolean returnedToMenu = false;
    int key;

    System.out.println();
    System.out.println("Czy zawartość zbioru liczb zespolonych w rekordzie ma być zawsze równa domyślnej? (t - tak)");
    alwaysUseDefaultComplexNumberArray = scanner.next().equals("t");
    System.out.println("Czy zawartość plików powinna być wyświetlana po każdej operacji zmieniającej zawartośc pliku? (t - tak)");
    printFilesAfterModification = scanner.next().equals("t");

    while (!returnedToMenu) {
      System.out.println();
      System.out.println("Opcje:");
      System.out.println("1) Odczyt rekordu");
      System.out.println("2) Dodanie rekordu");
      System.out.println("3) Aktualizacja rekordu");
      System.out.println("4) Usunięcie rekordu");
      System.out.println("5) Reorganizacja");
      System.out.println("6) Wyświetlenie wszystkich rekordów bez metadanych");
      System.out.println("7) Wyświetlenie zawartości plików");
      System.out.println("8) Powrót do menu");
      System.out.println("Proszę podać numer wybranej opcji");

      switch (scanner.nextInt()) {
        case 1:
          System.out.println("Proszę podać wartość klucza");
          record = indexSequentialStructure.getRecord(scanner.nextInt());
          if (record != null) {
            System.out.println("Odczytany rekord z kluczem: " + record.getKey() + " - " + record.toString());
          } else {
            System.out.println("Rekord nie został znaleziony");
          }
          break;
        case 2:
          System.out.println("Proszę podać wartość klucza");
          key = scanner.nextInt();
          record = createRecord(key, alwaysUseDefaultComplexNumberArray);
          indexSequentialStructure.addRecord(record);
          printFiles(printFilesAfterModification, "Zawartość plików po operacji:");
          break;
        case 3:

          break;
        case 4:

          break;
        case 5:
          indexSequentialStructure.reorganise(printFilesAfterModification);
          break;
        case 6:
          indexSequentialStructure.printData();
          break;
        case 7:
          indexSequentialStructure.printFiles();
          break;
        case 8:
          returnedToMenu = true;
      }
    }
  }

  private static void printFiles(boolean condition, String message) throws IOException, ClassNotFoundException {
    if (condition) {
      System.out.println(message);
      indexSequentialStructure.printFiles();
    }
  }

  private static ComplexNumberSet createRecord(int key, boolean defaultComplexNumberArrayUsed) {
    ComplexNumber[] defaultComplexNumberArray = new ComplexNumber[]{new ComplexNumber(1, 1)};
    ComplexNumber[] complexNumberArray;
    int numberOfElements;

    if (defaultComplexNumberArrayUsed) {
      return new ComplexNumberSet(defaultComplexNumberArray, key);
    } else {
      System.out.println("Proszę podać liczbę elementów zbioru");
      numberOfElements = scanner.nextInt();
      if (numberOfElements > ComplexNumberSet.getSetMaxSize()) {
        numberOfElements = ComplexNumberSet.getSetMaxSize();
        System.out.println("Podana liczba elementów jest większa od maksymalnego rozmiaru zbioru");
        System.out.println("Zmniejszono do " + ComplexNumberSet.getSetMaxSize());
      }
      complexNumberArray = new ComplexNumber[numberOfElements];
      for (int i = 0; i < numberOfElements; i++) {
        double imaginaryPart, realPart;
        System.out.println("Proszę podać część rzeczywistą");
        realPart = scanner.nextDouble();
        System.out.println("Proszę podać część urojoną");
        imaginaryPart = scanner.nextDouble();
        complexNumberArray[i] = new ComplexNumber(realPart, imaginaryPart);
      }
      return new ComplexNumberSet(complexNumberArray, key);
    }
  }

  private static void getOperationsFromFile()
      throws IOException, ClassNotFoundException, GuardRemovedException, KeyAlreadyExistsException {

    String filename;

    System.out.println("Prosze podać nazwę pliku testowego");
    filename = scanner.next();
    try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
      String line;
      while ((line = reader.readLine()) != null) {
        doOperation(line);
      }
    }
  }

  private static void doOperation(String text)
      throws IOException, ClassNotFoundException, GuardRemovedException, KeyAlreadyExistsException {

    String[] parsedText = text.split(" ");
    String operation = parsedText[0];
    int key = Integer.parseInt(parsedText[1]);
    ComplexNumberSet record;
    ComplexNumber[] defaultComplexNumberArray = {new ComplexNumber(1, 1)};

    if (parsedText.length > 2) {
      ComplexNumber[] complexNumbers = new ComplexNumber[(parsedText.length - 2) / 2];

      for (int i = 0; i < parsedText.length - 2; i += 2) {
        complexNumbers[i / 2] = new ComplexNumber(Double.parseDouble(parsedText[i + 2]),
            Double.parseDouble(parsedText[i + 1 + 2]));
      }
      record = new ComplexNumberSet(complexNumbers, key);
    } else {
      record = new ComplexNumberSet(defaultComplexNumberArray, key);
    }

    if (operation.equals("D")) {
      System.out.println("Dodawanie rekordu z kluczem " + record.getKey());
      indexSequentialStructure.addRecord(record);
    } else if (operation.equals("U")) {

    } else if (operation.equals("A")) {

    }
    indexSequentialStructure.printFiles();
  }
}
