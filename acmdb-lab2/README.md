# ACM-DB Lab 2 Document

This is the document of lab2 in  **CS 392: Database Management System** project.

## Design Decisions

This part contains what I have done to finish lab2.
Here are some brief implementation ideas.

- Exercise 1
  - Use LRU strategy to implement the eviction algorithm.
  - Use an linkedList to record the access. If a page is accessed, move it to the end of the list. Then the head of the list is the page to be evicted if an eviction happens.
- Exercise 2
  - Use recursive algorithm in `findLeafPage` function.
    - If the current page is a leaf page, get the page and return it.
    - If the current page is an internal page, get the page with READ_ONLY permission, find the child tree that contains the field, and then do `findLeafPage` on the corresponding page.
- Exercise3
  - For spliting a page, do the following things:
    - Create a new page as the new right page.
    - Move half of the elements to the new page.
    - Find the middle element, use it to update the parent page. (For spliting internal page, delete the middle entry)
    - Fix the sibling ID and parent ID on pages. (For spliting internal page, use `updateParentPointers` to update parent ID for children pages.)
- Exercise 4
  - Stealing from sibling is very similar to the spliting function. For stealing, do the following things:
    - Move the elements so that both will have half of total elements.(For internal page, create a new entry to connect the rightest pointer of the left page and the leftest pointer of the right page.)
    - Find the middle element, use it to update the parent page. (For stealing internal page, delete the middle entry)
    - For stealing internal page, use `updateParentPointers` to update parent ID for children pages.
- Exercise5
  - Merging page is also similar to the two operations above, and the steps are as follows:
    - Move all the elements in the right pages to the left.
    - For leaf pages, fix sibling ID. For internal pages, update parent pointers.
    - Delete the parent for cutting the two page, and make right page useful by `setEmptyPage`.

## API Changes

None. I did not add any new function in this lab.

## Missing or Incomplete Elements

- None

## Some Commits of Myself

I spent 3 days (I also have other works in these days) in this lab. This lab is more complex than lab1, but still not very hard. Debugging for the B+ Tree is friendly since we use Java rather than C++. Spliting operations are the most hard part for me. But after I get clear steps in my mind, it is not very hard for me to finish them.