# ACM-DB Lab 5 Document

This is the document of lab5 in  **CS 392: Database Management System** project.

## Design Decisions

This part contains what I have done to finish lab5.
Here are some brief implementation ideas.

- Lock Design for Transaction
  - A `LockType` enum class to represent the type of the locks. There are two types of lock according to my design, `SHARED` and `EXCLUSIVE`.
  - A `Lock` class to store the information as follows:
    - The type of the lock, defined in `LockType`.
    - The `PageId` of the lock, one page only has one lock and one lock can be only assigned to one page.
    - All transactions that once acquired the `SHARED` lock of the page.
    - All transactions that once acquired the `EXCLUSIVE` lock of the page.
    - All transactions that once first acquired the `SHARED` lock, and then acquired the `EXCLUSIVE` lock of the page.
    - All transactions that once acquired the `EXCLUSIVE`, and then acquired the `SHARED` lock of the page. 
  - A `LockManager` class to store the information as follows:
    - All locks that a transaction holds.
    - One lock that a page corresponding to.

- Lock Design for Multi-Threads
  - I simply use `synchronized` of java for multi-threads locking.

- Exercise 1 & 2
  - To implement acquiring locks, I modified the `getPage()` method in BufferPool. When getting a page with `READ_ONLY` permission, try to assign a `SHARED` lock to this page. And when getting a page with `READ_WRITE` permission, try to assign a `EXCLUSIVE` lock to this page.
  - To implement releasing locks, simply remove transaction id in the lock. And there is no transaction id in the lock, destory the lock. Also use `notifyAll()` method to notify all blocked threads.
- Exercise 3
  - When evicting pages, check that whether the page is dirty, and do not evict dirty pages in BufferPool.
- Exercise 4
  - If commit is true, flush all the pages associated to the transaction to the disk.
  - If commit is false, discard all the dirty pages.
- Exercise 5
  - I use a simple wait-timeout method to handle the deadlock. When a transaction is blocked, it will wait for a random time from 500ms to 1000ms, and will be aborted if time limit exceeds.

## API Changes

- I do not change any API of the original classes, however there are three new classes I have created.
  - `LockType`: an enum class with `SHARED` and `EXCLUSIVE`.
  - `Lock`: a class representing the locks. APIs are as follows:
    - `getLockType()`
    - `setLockType(LockType targetType)`
    - `getLockedPageId()`
    - `getSharedTidSet()`
    - `getExclusiveTidSet()`
    - `getUpgradeTagSet()`
    - `getDowngradeTagSet()`
    - `equals(Object o)`
    - `hashCode()`
  - `LockManager`: a class representing the lock manager. APIs are as follows:
    - `acquireLock(TransactionId tid, PageId pid, LockType lockType)`
    - `releaseLock(TransactionId tid, PageId pid)`
    - `releaseAllLocks(TransactionId tid)`
    - `getExclusiveLockedPids(TransactionId tid)`
    - `getSharedLockedPids(TransactionId tid)`
    - `holdsLock(TransactionId tid)`

## Missing or Incomplete Elements

- None

## Some Commits of Myself

I spent 3 days write and debug in this lab. Luckly, due to the heavy-lock design(This means a lock will record a lot of information so it will be slower), I did not spend a lot of time debugging multi-thread transactions. I think the most difficult part is designing the lock and the lock manager. Since debugging is confusing, especially when testcase is very complex, I have to check the lock system by reading my code rather than using breakpoints to see what happened.