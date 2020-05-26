# ACM-DB Lab 4 Document

This is the document of lab4 in  **CS 392: Database Management System** project.

## Design Decisions

This part contains what I have done to finish lab4.
Here are some brief implementation ideas.

- Exercise 1
  - To store the infomation of histogram for `Integer`, I have created a new class named `HistogramBar` to record the single bar of the histogram, and stored the histogram in an array of `HistogramBar`.
  - The `HistogramBar` class contains three variables: `left`, `right` and `count`. All the values in [left, right] should be counted in this bar, and `count` represents the height of the bar.
  - The number of the bars is defined as $min(max-min+1, buckets)$, since there is no necessary to store the same value in two different buckets.
  - The width of each bar is defined as $\frac{max-min}{number ~ of ~ bars}$ except the last bar.
- Exercise 2
  - First, get the TupleDesc of the table, check the type of each field and create histograms if a field type is `Int_TYPE` or `String_TYPE`. (Notice: If there exists `Int_TYPE`, a seqscan of all tuples is needed to get the maximum value and the minimum value.)
  - Second, if there exists `Int_TYPE` or `String_TYPE`, do a seqscan of all the tuples and build histograms.
  - Finally, estimate the cost according to the histograms.
- Exercise 3
  - To estimate the cost of join, I only implemented the simple estimation. That is:
    - For equal join, if one of them has primary key, the cost is the card of another one.
    - For equal join and both of them do not have primary keys, the cost is the maximum of two cards.
    - For range scans, the cost is 30% of the product of two cards.
- Exercise 4
  - In this part, I implemented Selinger optimizer as the join optimizer. The basic idea is that using Dynamic Planning to get the join order with lowest cost.

## API Changes

- In the `DbFile` interface, I added a function called `numPages()` to get the pages that stores in the file.

## Missing or Incomplete Elements

- None

## Some Commits of Myself

I spent 2 days write and debug in this lab. The most difficult part of this lab is understanding the cost estimation and optimization algorithms rather than the code implementation. Once I have fully understood the algorithm and the APIs, I could quickly implement the missing part.