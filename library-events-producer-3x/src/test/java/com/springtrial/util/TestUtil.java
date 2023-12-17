package com.springtrial.util;

import com.springtrial.domain.Book;
import com.springtrial.domain.LibraryEvent;
import com.springtrial.domain.LibraryEventType;

public class TestUtil {

  public static Book bookRecord() {

    return new Book(456, "Kafka Using Spring Boot- 3x", "yashas samaga");
  }

  public static Book bookRecordWithInvalidValues() {

    return new Book(null, "", "yashas samaga");
  }

  public static LibraryEvent libraryEventRecord() {

    return
       new LibraryEvent(null,
          LibraryEventType.NEW,
          bookRecord());
  }

  public static LibraryEvent newLibraryEventRecordWithLibraryEventId() {

    return
       new LibraryEvent(456,
          LibraryEventType.NEW,
          bookRecord());
  }

  public static LibraryEvent libraryEventRecordUpdate() {

    return
       new LibraryEvent(456,
          LibraryEventType.UPDATE,
          bookRecord());
  }

  public static LibraryEvent libraryEventRecordUpdateWithNullLibraryEventId() {

    return
       new LibraryEvent(null,
          LibraryEventType.UPDATE,
          bookRecord());
  }

  public static LibraryEvent libraryEventRecordWithInvalidBook() {

    return
       new LibraryEvent(null,
          LibraryEventType.NEW,
          bookRecordWithInvalidValues());
  }
}