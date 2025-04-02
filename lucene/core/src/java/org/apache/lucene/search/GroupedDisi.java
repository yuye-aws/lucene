package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * This is a group iterator that groups documents by the value of a field. It is used to group
 * documents by cluster_id.
 */
public class GroupedDisi implements Iterator<GroupedDisi.DocBound> {

    private final Collection<Integer> groupValues;
    private final LeafReaderContext context;
    private Iterator<Integer> groupIterator;
    private DocBound current;

    public DocBound getCurrent() {
        return current;
    }

    GroupedDisi(LeafReaderContext context, Collection<Integer> groupValues) {
        this.context = context;
        this.groupValues = groupValues;
        this.groupIterator = this.groupValues.iterator();
    }

    private DocBound nextGroup() throws IOException {
        if (!groupIterator.hasNext()) return null;
        int groupValue = groupIterator.next();

        Sort indexSort = this.context.reader().getMetaData().sort();
        if (indexSort != null
            && indexSort.getSort().length > 0
            && indexSort.getSort()[0].getField().equals("cluster_id")) {
            final SortField sortField = indexSort.getSort()[0];
            final SortField.Type sortFieldType = getSortFieldType(sortField);

            int maxDoc = this.context.reader().maxDoc();
            int lower = groupValue;
            int upper = groupValue;
            // Perform a binary search to find the first document with value >= lower.
            ValueComparator comparator = loadComparator(sortField, sortFieldType, lower, this.context);
            int low = 0;
            int high = maxDoc - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                if (comparator.compare(mid) <= 0) {
                    high = mid - 1;
                    comparator = loadComparator(sortField, sortFieldType, lower, this.context);
                } else {
                    low = mid + 1;
                }
            }
            int firstDocIdInclusive = high + 1;

            // Perform a binary search to find the first document with value > upper.
            // Since we know that upper >= lower, we can initialize the lower bound
            // of the binary search to the result of the previous search.
            comparator = loadComparator(sortField, sortFieldType, upper, this.context);
            low = firstDocIdInclusive;
            high = maxDoc - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                if (comparator.compare(mid) < 0) {
                    high = mid - 1;
                    comparator = loadComparator(sortField, sortFieldType, upper, this.context);
                } else {
                    low = mid + 1;
                }
            }

            int lastDocIdExclusive = high + 1;
            if (firstDocIdInclusive >= lastDocIdExclusive) {
                return nextGroup();
            }
            return new DocBound(firstDocIdInclusive, lastDocIdExclusive);
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return groupIterator.hasNext();
    }

    @Override
    public DocBound next() {
        try {
            current = nextGroup();
            return current;
        } catch (IOException e) {
            return null;
        }
    }

    private static SortField.Type getSortFieldType(SortField sortField) {
        // We expect the sortField to be SortedNumericSortField
        if (sortField instanceof SortedNumericSortField) {
            return ((SortedNumericSortField) sortField).getNumericType();
        } else {
            return sortField.getType();
        }
    }
    private interface ValueComparator {
        int compare(int docID) throws IOException;
    }

    private static ValueComparator loadComparator(
        SortField sortField, SortField.Type type, long topValue, LeafReaderContext context)
        throws IOException {
        @SuppressWarnings("unchecked")
        FieldComparator<Number> fieldComparator =
            (FieldComparator<Number>) sortField.getComparator(1, Pruning.NONE);
        if (type == SortField.Type.INT) {
            fieldComparator.setTopValue((int) topValue);
        } else {
            // Since we support only Type.INT and Type.LONG, assuming LONG for all other cases
            fieldComparator.setTopValue(topValue);
        }

        LeafFieldComparator leafFieldComparator = fieldComparator.getLeafComparator(context);
        int direction = sortField.getReverse() ? -1 : 1;

        return doc -> {
            int value = leafFieldComparator.compareTop(doc);
            return direction * value;
        };
    }

    /**
     * This class represents a document bound, which is a range of document IDs that belong to the
     * same group.
     */
    public class DocBound {
        DocBound(int lower, int upper) {
            this.lower = lower;
            this.upper = upper;
        }

        public int lower;
        public int upper;
    }
}

