package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.Query;

public class LongPointRangeQueryParser extends MultiFieldQueryParser {
    public LongPointRangeQueryParser(String[] fields, Analyzer analyzer) {
        super(fields, analyzer);
    }

    @Override
    protected Query newRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
        if(part1.startsWith("'") || part2.startsWith("'")) return super.newRangeQuery(field,part1, part2, startInclusive, endInclusive);

        return LongPoint.newRangeQuery(field, Long.parseLong(part1), Long.parseLong(part2));
    }
}
