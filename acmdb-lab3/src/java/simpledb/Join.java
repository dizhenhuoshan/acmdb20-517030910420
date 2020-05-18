package simpledb;

import java.lang.reflect.Array;
import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */

    /*My Implementation Start*/
    private JoinPredicate joinPredicate;
    private DbIterator child1;
    private DbIterator child2;
    private TupleDesc mergedTupleDesc;
    private ArrayList<Tuple> joinedArray;
    private Iterator<Tuple> joinedIterator;
    /*My Implementation End*/

    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.mergedTupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        this.joinedArray = new ArrayList<Tuple>();
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.joinPredicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return this.child1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return this.child2.getTupleDesc().getFieldName(joinPredicate.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return mergedTupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
        int lenLeftTuple = child1.getTupleDesc().getLength();
        int lenRightTuple = child2.getTupleDesc().getLength();
        while (child1.hasNext())
        {
            Tuple tmpLeft = child1.next();
            child2.rewind();
            while (child2.hasNext())
            {
                Tuple tmpRight = child2.next();
                if (this.joinPredicate.filter(tmpLeft, tmpRight))
                {
                    Tuple joinedTuple = new Tuple(this.mergedTupleDesc);
                    for (int i = 0; i < lenLeftTuple; i++)
                    {
                        joinedTuple.setField(i, tmpLeft.getField(i));
                    }
                    for (int i = 0; i < lenRightTuple; i++)
                    {
                        joinedTuple.setField(i + lenLeftTuple, tmpRight.getField(i));
                    }
                    this.joinedArray.add(joinedTuple);
                }
            }
        }
        this.joinedIterator = this.joinedArray.iterator();
    }

    public void close() {
        // some code goes here
        this.joinedArray.clear();
        this.child1.close();
        this.child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.joinedIterator.hasNext())
        {
            return this.joinedIterator.next();
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[2];
        children[0] = child1;
        children[1] = child2;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
