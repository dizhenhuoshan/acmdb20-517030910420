# ACM-DB Lab 1 Document

This is the document of lab1 in  **CS 392: Database Management System** project.

## Design Decisions

This part contains what I have done to finish lab1.
Here are some brief implementation ideas.

- Exercise 1
  - Use ArrayList to manage the TupeDesc of a table.
- Exercise 2
  - Create a supplemental class called `CatalogItem` to store the table infomation. 
  - Use a hashmap to store `CatalogItem`, and the index is `ID` of `DBFile`.
  - Use a hashmap to store the mapping from `name` to `ID`, in order to find the table using `name`.
- Exercise3
  - Create an `Array` to store pages, a hashmap to store the mapping from `PageId` to the index of page array, and bitsets to store the status of each page.
  - A simple cache is implemented by storing the pages which are not in the page array before into the array.
- Exercise 4
  - Use the combination of `tableId` + `pageNo` as `HeapPageId`, and `pageId` + `tupleNo` as `RecordId`.
  - Create a new Iterator class `ValidTupleIterator` to iterate the valid tuples.
- Exercise5
  - Create a new Iterator class `HeapFileTupleIterator` to iterate the tuples in the heapfile.
  - Use `RandomFileAccess` to access pages in disk files. **WARNING: Must use seek() to set the file pointer before read()**.
- Exercise 6
  - Format the `TupleDesc`, then use `TupleIterator` in `DBFile` to iterator the tuples.

## API Changes

- TupleDesc.java: 
  - Add function `getLength()` to get the number of columns of the table.
  - Add function `getTDItem(int i)` to get the i-th `TDItem` of the TupleDessc.

## Missing or Incomplete Elements

- None

## Some Commits of Myself

I spent about 2 - 3 days (I also have other works in these days) in this lab. This lab is very fundamental and I think there is nothing particularly difficult or confusing.