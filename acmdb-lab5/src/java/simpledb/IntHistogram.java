package simpledb;

import static java.lang.Math.min;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /*My Implementation Start*/

    private class HistogramBar
    {
        // Notice: this is a dual-close range, that means all values in [left. right] will be counted into this histBar
        public int left = 0;
        public int right = 0;
        public int count = 0;
    }

    private int minValue = 0;
    private int maxValue = 0;
    private int barWidthBase = 0;
    private int tableTupleNum = 0;
    private int barNum = 0;
    private HistogramBar[] histograms = null;
    /*My Implementation End*/

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.minValue = min;
        this.maxValue = max;
        this.barNum = min(buckets, max - min + 1);
        this.histograms = new HistogramBar[this.barNum];
        int widthPerBar = (max - min + 1) / this.barNum;
        this.barWidthBase = widthPerBar;
        for (int i = 0; i < this.barNum; i++)
        {
            this.histograms[i] = new HistogramBar();
            this.histograms[i].left = min + i * widthPerBar;
            if (i == this.barNum - 1)
            {
                this.histograms[i].right = max;
            }
            else
            {
                this.histograms[i].right = min + (i + 1) * widthPerBar - 1;
            }
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = min((v - this.minValue) / this.barWidthBase, this.barNum - 1);
        this.histograms[index].count += 1;
        this.tableTupleNum += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        double result = 0.0;

        if (v < this.minValue || v > this.maxValue)
        {
            switch (op)
            {
                case EQUALS:
                    return 0.0;
                case NOT_EQUALS:
                    return 1.0;
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    if (v < this.minValue)
                        return 0.0;
                    return 1.0;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    if (v > this.maxValue)
                        return 0.0;
                    return 1.0;
            }
        }

        int index = min((v - this.minValue) / this.barWidthBase, this.barNum - 1);
        HistogramBar bar;
        switch (op)
        {
            case EQUALS:
                bar = this.histograms[index];
                result = (double)bar.count / (double)(bar.right - bar.left + 1) / this.tableTupleNum;
                break;
            case NOT_EQUALS:
                bar = this.histograms[index];
                result = 1.0 - (double)bar.count / (double)(bar.right - bar.left + 1) / this.tableTupleNum;
                break;
            case LESS_THAN:
                if (v < this.minValue || v > this.maxValue)
                {
                    result = 1.0;
                    break;
                }
                bar = this.histograms[index];
                for (int i = 0; i < index; i++)
                    result += (double) this.histograms[i].count / this.tableTupleNum;
                result += (double)bar.count * (v - bar.left) / (double)(bar.right - bar.left + 1) / this.tableTupleNum;
                break;
            case GREATER_THAN:
                bar = this.histograms[index];
                for (int i = index + 1; i < this.barNum; i++)
                    result += (double) this.histograms[i].count / this.tableTupleNum;
                result += (double)bar.count * (bar.right - v) / (double)(bar.right - bar.left + 1) / this.tableTupleNum;
                break;
            case LESS_THAN_OR_EQ:
                bar = this.histograms[index];
                for (int i = 0; i < index; i++)
                    result += (double) this.histograms[i].count / this.tableTupleNum;
                result += (double)bar.count * (v - bar.left + 1) / (double)(bar.right - bar.left + 1) / this.tableTupleNum;
                break;
            case GREATER_THAN_OR_EQ:
                bar = this.histograms[index];
                for (int i = index + 1; i < this.barNum; i++)
                    result += (double) this.histograms[i].count / this.tableTupleNum;
                result += (double)bar.count * (bar.right - v + 1) / (double)(bar.right - bar.left + 1) / this.tableTupleNum;
                break;
                default:
                System.err.println("Operator of estimateSeletivity is not = > or <, unhandled calculation");
                break;
        }

        return result;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Printing the IntHistogram: \n");
        stringBuilder.append("Left, \t\tRight, \t\tValue\n");
        for (int i = 0; i < this.barNum; i++)
        {
            stringBuilder.append(this.histograms[i].left).append(", \t\t");
            stringBuilder.append(this.histograms[i].right).append(", \t\t");
            stringBuilder.append(this.histograms[i].count).append("\n");
        }
        return stringBuilder.toString();
    }
}
