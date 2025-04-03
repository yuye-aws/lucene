package org.apache.lucene.search;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This is a group iterator that groups documents by the value of a field. It is used to group
 * documents by cluster_id.
 */
public class GroupedDisi implements Iterator<GroupedDisi.DocBound> {
    private final LeafReaderContext context;
    private DocBound current;
    private final static String SORTED_FIELD = "cluster_id";
    private Map<Long, DocBound> clusterBound = new HashMap<>();
    private Iterator<DocBound> clusterBoundIter;

    public DocBound getCurrent() {
        return current;
    }

    private void initialize(Collection<Integer> groupValues) throws IOException {

        for (Integer groupValue : groupValues) {
            clusterBound.put((long)groupValue, new DocBound(-1, -1));
        }

        SortedNumericDocValues docValues = DocValues.getSortedNumeric(this.context.reader(), SORTED_FIELD);
        int doc = docValues.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            long value = docValues.nextValue();
            docValues.advance()
            if (clusterBound.containsKey(value)) {
                if (clusterBound.get(value).lower == -1) {
                    clusterBound.get(value).lower = doc;
                }
                clusterBound.get(value).upper = doc + 1;
            }
            doc = docValues.nextDoc();
        }
        // get values from clusterBound and sort them by lower then assign to bounds
        List<DocBound> bounds = new ArrayList<>(clusterBound.values().stream().toList());
        bounds.sort((a, b) -> a.lower - b.lower);
        clusterBoundIter = bounds.iterator();
    }

    GroupedDisi(LeafReaderContext context, Collection<Integer> groupValues) throws IOException {
        this.context = context;
        initialize(groupValues);
    }

    @Override
    public boolean hasNext() {
        return clusterBoundIter.hasNext();
    }

    @Override
    public DocBound next() {
        if (!clusterBoundIter.hasNext()) {
            // Handle the case when there are no more elements
            // For example, return null or a default value
            current = null;
            return null;
        }
        current = clusterBoundIter.next();
        return current;
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

