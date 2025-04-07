package org.apache.lucene.search;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * This is a group iterator that groups documents by the value of a field. It is used to group
 * documents by cluster_id.
 */
public class GroupedDisi implements Iterator<GroupedDisi.DocBound> {
    private final LeafReaderContext context;
    private DocBound current;
    String segmentName;
    private final static String SORTED_FIELD = "cluster_id";
    private Map<Long, DocBound> clusterBound = new TreeMap<>();
    private Map<Long, DocBound> clusterBoundPrecomputed = new TreeMap<>();;
    private Iterator<Map.Entry<Long, DocBound>> clusterBoundIter;

    public DocBound getCurrent() {
        return current;
    }

    private void initialize(Collection<Integer> groupValues) throws IOException {
        for (Integer groupValue : groupValues) {
            clusterBound.put(Long.valueOf(groupValue), new DocBound(-1, -1));
        }

        SortedNumericDocValues docValues = DocValues.getSortedNumeric(this.context.reader(), SORTED_FIELD);
        int doc = docValues.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            long value = docValues.nextValue();
            if (clusterBound.containsKey(value)) {
                if (clusterBound.get(value).lower == -1) {
                    clusterBound.get(value).lower = doc;
                }
                clusterBound.get(value).upper = doc + 1;
            }
            doc = docValues.nextDoc();
        }
        clusterBoundIter = clusterBound.entrySet().iterator();
    }

    public static SegmentReader segmentReader(LeafReader reader) {
        if (reader instanceof SegmentReader) {
            return (SegmentReader) reader;
        } else if (reader instanceof FilterLeafReader) {
            final FilterLeafReader fReader = (FilterLeafReader) reader;
            return segmentReader(FilterLeafReader.unwrap(fReader));
        } else if (reader instanceof FilterCodecReader) {
            final FilterCodecReader fReader = (FilterCodecReader) reader;
            return segmentReader(FilterCodecReader.unwrap(fReader));
        }
        // hard fail - we can't get a SegmentReader
        throw new IllegalStateException("Can not extract segment reader from given index reader [" + reader + "]");
    }

    GroupedDisi(LeafReaderContext context, Collection<Integer> groupValues) throws IOException {
        this.context = context;
        initialize(groupValues);
        this.segmentName = segmentReader(context.reader()).getSegmentName();
    }

    GroupedDisi(LeafReaderContext context, Collection<Integer> groupValues, Map<Long, DocBound> clusterBoundPrecomputed) throws IOException {
        this.context = context;
        this.clusterBoundPrecomputed = clusterBoundPrecomputed;
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
        current = clusterBoundIter.next().getValue();
        return current;
    }

    /**
     * This class represents a document bound, which is a range of document IDs that belong to the
     * same group.
     */
    public static class DocBound {
        public DocBound(int lower, int upper) {
            this.lower = lower;
            this.upper = upper;
        }

        public int lower;
        public int upper;
    }
}

