package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /*My Implementation Start*/
    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private TupleDesc tupleDesc;
    private boolean isCalled = false;
    /*My Implementation End*/

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        Type[] tmpArray = new Type[1];
        tmpArray[0] = Type.INT_TYPE;
        this.tupleDesc = new TupleDesc(tmpArray);
        if (!Database.getCatalog().getTupleDesc(tableId).equals(this.child.getTupleDesc()))
        {
            throw new DbException("TupleDesc between child and table not matched in Insert.java!\n");
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.isCalled)
        {
            return null;
        }
        else
        {
            int count = 0;
            while (child.hasNext())
            {
                Tuple insertingTuple = this.child.next();
                try
                {
                    Database.getBufferPool().insertTuple(this.tid, this.tableId, insertingTuple);
                    count++;
                } catch (IOException ioException)
                {
                    throw new DbException("IOException happened when inserting Tuple in fetchNext() \n");
                }
            }
            Tuple resTuple = new Tuple(this.tupleDesc);
            resTuple.setField(0, new IntField(count));
            this.isCalled = true;
            return resTuple;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[1];
        children[0] = child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
