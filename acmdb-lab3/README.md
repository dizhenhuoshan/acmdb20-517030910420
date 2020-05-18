# ACM-DB Lab 3 Document

This is the document of lab3 in  **CS 392: Database Management System** project.

## Design Decisions

This part contains what I have done to finish lab3.
Here are some brief implementation ideas.

- Exercise 1
  - For filter, simply traverse the iterator, and stop until we find a tuple that meet the filter requirement.
  - For join, I have used simple nested loops join so far. That is, for every tuple in the left relation to join, traverse the right relation, and join the tuples that meet the requirements.
- Exercise 2
  - Use `Aggregator` to implement the aggregate function. So far only `Integer` and `String` type is supported by aggregators.
  - `IntegerAggregator` supports 5 aggregation opreations: `MIN`, `MAX`, `SUM`, `AVG`, `COUNT`. To implement the operations, I create a temporary result array to store the aggregation results, and update the array for every `mergeTupleIntoGroup()`. Also, the TupleDesc of the result table should be created. Finally create a `TupleIterator` using the result TupleDesc and result array to get the result of the aggregation.
  - `StringAggregator` only supports `COUNT` operation. And the implementation method is similar with the `IntegerAggregator`.
- Exercise 3
  - For `insertTuple`, it should be divided into two parts. First part is finding a page that have free slots to store the tuple, and second part is inserting the tuple into the page.
    - Finding the target page to insert: traverse the page in the file, try to insert the tuple in current page, and stop the traversing if the insert operation succeed. If all current pages are full, we should create a new page to store the tuple.
    - Insert the target tuple into a page: First check whether tupleDesc of the tuple and the page match, then try to find an empty slots to store the tuple. If all slots are full, throw an exception.
    - All modified pages should be recorded, and marked dirty in `BufferPool`.
  - For `deleteTuple`, it is more easier. Since the `RecordId` in the tuple contains the PageId where stores the tuple, there is no need to traverse the pages. To delete the tuple in the page, simply find it and make that slot empty. Also, all modified pages should be recorded and marked dirty.
- Exercise 4
  - `Insert` and `Delete` are the operators for inserting and deleting tuples. For `Insert`, traverse the iterator, and call `BufferPool.insertTuple()`. For `Delete`, modifing the source data while iterating will cause some undefined behaviors. Hence, firstly I traverse the iterator use a array to store the tuple to be deleted, and then traverse the array to call `BufferPool.deleteTuple()` to delete tuples.

## API Changes

None. I did not add any new function in this lab.

## Missing or Incomplete Elements

- None

## Some Commits of Myself

I spent 3 days write and debug in this lab. This lab is a little confusing since the java docs in the classed we need to fill is not as clear as in lab2. But it not a big problem since the testcases tells me how to do. The `Delete` operator is a little trivial for me, since I forgot that I should not modify the source data while iterating, and stuck at there to debug for several hours.