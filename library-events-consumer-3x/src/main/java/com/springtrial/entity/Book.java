package com.springtrial.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Book {
  @Id
  private Integer bookId;
  private String bookName;
  private String bookAuthor;

  @OneToOne(targetEntity = LibraryEvent.class, cascade = {CascadeType.ALL})
  @JoinColumn(name = "libraryEventId")
  private LibraryEvent libraryEvent;
}