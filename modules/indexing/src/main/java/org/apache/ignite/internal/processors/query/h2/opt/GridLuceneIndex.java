/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.opt;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.table.Column;
import org.h2.util.JdbcUtils;
import org.jetbrains.annotations.Nullable;
import org.wltea.analyzer.lucene.IKTokenizer;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;

/**
 * Lucene fulltext index.
 */
public class GridLuceneIndex implements AutoCloseable {
    private static ConcurrentMap<String, GridLuceneIndex> luceneIndexes = new ConcurrentHashMap<>();

    /** Field name for string representation of value. */
    public static final String VAL_STR_FIELD_NAME = "_gg_val_str__";

    /** Field name for value version. */
    public static final String VER_FIELD_NAME = "_gg_ver__";

    /** Field name for value expiration time. */
    public static final String EXPIRATION_TIME_FIELD_NAME = "_gg_expires__";

    /** seperator for sort in lucene query*/
    public static final String QUERY_BOOST_PREFIX = "_BOOST_BY_";

    public static final String QUERY_SORT_ASC = "ASC";

    public static final String QUERY_SORT_DESC = "DESC";

    /** */
    private final String cacheName;

    /** */
    private final GridQueryTypeDescriptor type;

    /** */
    private final IndexWriter writer;

    private final String[] keyFields;

    private DirectoryReader reader;

    private IndexSearcher searcher;

    private Analyzer queryAnalyzer;

    /** */
    private final Map<String, Class> entityFields = new HashMap<>();

    /** */
    private final String[] idxdFields;

    /** */
    private final String[] queryingFields;

    /** */
    private final AtomicLong updateCntr = new GridAtomicLong();

    /** */
    private final BaseDirectory dir;

    /** */
    private final GridKernalContext ctx;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param cacheName Cache name.
     * @param type Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public GridLuceneIndex(GridKernalContext ctx, @Nullable String cacheName, GridQueryTypeDescriptor type)
        throws IgniteCheckedException {
        this.ctx = ctx;
        this.cacheName = cacheName;
        this.type = type;

        boolean isPersist = ctx.cache().internalCache(cacheName).context().group().persistenceEnabled();
        CacheConfiguration<?,?>  cacheConfig = ctx.cache().internalCache(cacheName).configuration();

        List<String> keyFields = new ArrayList<>();

        for(QueryEntity qe : cacheConfig.getQueryEntities()) {

            Set<String> qeKeyFields = qe.getKeyFields();
            if(qeKeyFields != null)
            for(String k : qeKeyFields){
                keyFields.add(k.toUpperCase());
            }

            for(Map.Entry<String,String> abc  : qe.getFields().entrySet()){
                try {
                    entityFields.put(abc.getKey().toUpperCase(), Class.forName(abc.getValue()));
                } catch (ClassNotFoundException e) {
                    entityFields.put(abc.getKey().toUpperCase(), Object.class);
                }
            }
        }

        this.keyFields = keyFields.toArray(new String[0]);

        try {

            final PdsFolderSettings folderSettings = ctx.pdsFolderResolver().resolveFolders();

            Path path =Paths.get(folderSettings.persistentStoreRootPath().getPath(), folderSettings.folderName(), "LuceneIndex-"+cacheName);

            dir =
                    isPersist
                            ? FSDirectory.open(path)
                            : new GridLuceneDirectory(new GridUnsafeMemory(0));

            Analyzer analyzer = queryAnalyzer =new Analyzer() {
                @Override
                protected TokenStreamComponents createComponents(String fieldName) {
                    Tokenizer tokenizer = new IKTokenizer();
                    TokenStream stream;
                    stream = new EdgeNGramTokenFilter(tokenizer,1, 20, true);
                    return new TokenStreamComponents(tokenizer, stream);
                }
            };

            IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);

            if(isNewCache(ctx, cacheName))
                writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE); //if is new cache we need to create new index. this opt will erase old data and create new one

            queryAnalyzer =  new Analyzer() {
                @Override
                protected TokenStreamComponents createComponents(String fieldName) {
                    Tokenizer tokenizer = new IKTokenizer();
                    return new TokenStreamComponents(tokenizer);
                }
            };

            writer = new IndexWriter(dir, writerConfig);

            if(DirectoryReader.indexExists(dir)) {
                reader = DirectoryReader.open(dir);
                createSearcher();
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        GridQueryIndexDescriptor idx = type.textIndex();

        if (idx != null) {

            Collection<String> fields = idx.fields();

            idxdFields = new String[fields.size() + 1];

            fields.toArray(idxdFields);
        }
        else {
            assert type.valueTextIndex() || type.valueClass() == String.class;

            idxdFields = new String[1];
        }

        idxdFields[idxdFields.length - 1] = VAL_STR_FIELD_NAME;
        queryingFields = getQueryingFields(idxdFields);

        luceneIndexes.put(cacheName, this);
    }

    private String[] getQueryingFields(String[] fields) {
        ArrayList<String> arrayList = new ArrayList<>();

        for(String indexField : fields){
            if(entityFields.containsKey(indexField)){
                Class cls = entityFields.get(indexField);
                arrayList.addAll(GridLuceneFieldFactory.Instance.getGeneratedIndexableName(indexField, cls));
            }
        }

        return arrayList.toArray(new String[0]);
    }

    private boolean isNewCache(GridKernalContext ctx, @Nullable String cacheName){
        int size = ctx.cache().internalCache(cacheName).metrics0().getEntriesStat().size();
        return size == 0;
    }


    private void createSearcher() {
        searcher = new IndexSearcher(reader);

//        final ClassicSimilarity similarity = new ClassicSimilarity()
//        {
//            @Override
//            public float tf(float freq) {
//                return 1;
//            }
//
//            @Override
//            public float idf(long docFreq, long docCount) {
//                return 1;
//            }
//        };
//
//        searcher.setSimilarity(similarity);
    }

    /**
     * @return Cache object context.
     */
    private CacheObjectContext objectContext() {
        if (ctx == null)
            return null;

        return ctx.cache().internalCache(cacheName).context().cacheObjectContext();
    }

    /**
     * Stores given data in this fulltext index.
     *
     * @param k Key.
     * @param v Value.
     * @param ver Version.
     * @param expires Expiration time.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void store(CacheObject k, CacheObject v, GridCacheVersion ver, long expires) throws IgniteCheckedException {
        CacheObjectContext coctx = objectContext();

        Object key = k.isPlatformType() ? k.value(coctx, false) : k;
        Object val = v.isPlatformType() ? v.value(coctx, false) : v;

        Document doc = new Document();

        boolean stringsFound = false;

        if (type.valueTextIndex() || type.valueClass() == String.class) {
            doc.add(new TextField(VAL_STR_FIELD_NAME, val.toString(), Field.Store.YES));

            stringsFound = true;
        }

        for (int i = 0, last = idxdFields.length - 1; i < last; i++) {
            Object fieldVal = type.value(idxdFields[i], key, val);
            if(fieldVal == null) continue;

            Collection<IndexableField> fields = GridLuceneFieldFactory.Instance.createFields(idxdFields[i], fieldVal);

            if(fields != null){
                for(IndexableField field : fields){
                    doc.add(field);
                    stringsFound = true;
                }
            }
        }

        BytesRef keyByteRef = new BytesRef(k.valueBytes(coctx));

        try {
            final Term term = new Term(KEY_FIELD_NAME, keyByteRef);

            if (!stringsFound) {
                writer.deleteDocuments(term);
                return; // We did not find any strings to be indexed, will not store data at all.
            }

            doc.add(new StringField(KEY_FIELD_NAME, keyByteRef, Field.Store.YES));

//            if (type.valueClass() != String.class)
//                doc.add(new StoredField(VAL_FIELD_NAME, v.valueBytes(coctx)));

            doc.add(new StoredField(VER_FIELD_NAME, ver.toString().getBytes()));

            doc.add(new LongPoint(EXPIRATION_TIME_FIELD_NAME, expires));

            // Next implies remove than add atomically operation.
            writer.updateDocument(term, doc);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            updateCntr.incrementAndGet();
        }
    }

    /**
     * Removes entry for given key from this index.
     *
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(CacheObject key) throws IgniteCheckedException {
        try {
            writer.deleteDocuments(new Term(KEY_FIELD_NAME,
                new BytesRef(key.valueBytes(objectContext()))));
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            updateCntr.incrementAndGet();
        }
    }

    /**
     * Runs lucene fulltext query over this index.
     *
     * @param qry Query.
     * @param filters Filters over result.
     * @param pageSize Size of batch
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> query(String qry, IndexingQueryFilter filters, int pageSize) throws IgniteCheckedException {
        if (!prepareQuery()) return new GridEmptyCloseableIterator<>();

        Query query = parseQuery(qry);

        IndexingQueryCacheFilter fltr = null;

        if (filters != null)
            fltr = filters.forCache(cacheName);

        IgniteCache<K,V> rawCache =  ctx.grid().cache(cacheName).withKeepBinary();

        return new It<>(searcher, query, fltr, rawCache::get, pageSize, 0, pageSize);
    }

    protected Query parseBoostClause(Query query, String boostClause) {
        if(boostClause == null){
            return query;
        }

        String[] boostClauses = boostClause.split(",");
        Pair<String,Float>[] boostedProperties = new Pair[boostClauses.length];

        for(int i =0;i <boostClauses.length;i++){
            boostedProperties[i] = ParseBoostProperty(boostClauses[i]);
        }

        return new BoostedScoreQuery(query, boostedProperties);
    }

    private static Pair<String, Float> ParseBoostProperty(String boostProperty){
        int i = boostProperty.indexOf('^');
        String property = boostProperty.substring(0, i).trim();
        String rateS = boostProperty.substring(i+1).trim();
        Float rate = Float.parseFloat(rateS);

        return new Pair<>(property, rate);
    }


    protected Query parseQuery(String qry) throws IgniteCheckedException {
        Query query;

        try {

            MultiFieldQueryParser parser = new LongPointRangeQueryParser(queryingFields, queryAnalyzer);

//            parser.setAllowLeadingWildcard(true);

            // Filter expired items.
            Query filter = LongPoint.newRangeQuery(EXPIRATION_TIME_FIELD_NAME, U.currentTimeMillis(), Long.MAX_VALUE);

            query = new BooleanQuery.Builder()
                .add(parser.parse(GridLuceneFieldFactory.TextUtils.normalize(qry)), BooleanClause.Occur.MUST)
                .add(filter, BooleanClause.Occur.FILTER)
                .build();
        }
        catch (Exception e) {
            //U.closeQuiet(reader);

            throw new IgniteCheckedException(e);
        }

        return query;
    }

    protected boolean prepareQuery() throws IgniteCheckedException {
        try {
            long updates = updateCntr.get();

            if (updates != 0) {
                writer.commit();

                updateCntr.addAndGet(-updates);
            }

            //We can cache reader\searcher and change this to 'openIfChanged'
            if(reader == null) {
                if(DirectoryReader.indexExists(dir)){
                    reader = DirectoryReader.open(writer);
                    createSearcher();
                }
                else
                    return false;
            }else {
                DirectoryReader newReader = DirectoryReader.openIfChanged(reader);

                if(newReader != null){
                    reader= newReader;
                    createSearcher();
                }
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(writer);
        U.close(dir, ctx.log(GridLuceneIndex.class));
        luceneIndexes.remove(cacheName);
    }

    protected interface ItValueGetter<K,V>{
        V getValue(K key);
    }

    /**
     * Key-value iterator over fulltext search result.
     */
    private class It<K, V> extends GridCloseableIteratorAdapter<IgniteBiTuple<K, V>> {
        private final int BatchPosBeforeHead = -1;

        /** */
        private static final long serialVersionUID = 0L;

        private final ItValueGetter<K, V> valueGetter;
        /** */
        private final int pageSize;

        private int remains;

        /** */
        private final Query query;

        /** */
        private final IndexSearcher searcher;

        /** current batch docs*/
        private ScoreDoc[] batch;

        /** current position in batch*/
        private int batchPos = BatchPosBeforeHead;

        /** */
        private final IndexingQueryCacheFilter filters;

        /** */
        private IgniteBiTuple<K, V> curr;

        /** */
        private CacheObjectContext coctx;

        /**
         * Constructor.
         *
         * @param searcher Searcher.
         * @param filters Filters over result.
         * @throws IgniteCheckedException if failed.
         */
        private It(IndexSearcher searcher, Query query, IndexingQueryCacheFilter filters,ItValueGetter<K,V> valueGetter ,int pageSize, int offset, int limit)
            throws IgniteCheckedException {
            this.searcher = searcher;
            this.filters = filters;
            this.query = query;
            this.valueGetter = valueGetter;
            this.pageSize = pageSize;
            this.remains = limit;
            coctx = objectContext();
            findNext();
            skip(offset);
        }

        /**
         * @param bytes Bytes.
         * @param ldr Class loader.
         * @return Object.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private <Z> Z unmarshall(byte[] bytes, ClassLoader ldr) throws IgniteCheckedException {
            if (coctx == null) // For tests.
                return (Z)JdbcUtils.deserialize(bytes, null);

            return (Z)coctx.kernalContext().cacheObjects().unmarshal(coctx, bytes, ldr);
        }

        /**
         * Finds next element.
         *
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private void findNext() throws IgniteCheckedException {
            curr = null;

            if(isClosed())
                throw new IgniteCheckedException("Iterator already closed");

            if (shouldRequestNextBatch()) {
                try {
                    requestNextBatch();
                } catch (IOException e) {
                    close();
                    throw new IgniteCheckedException(e);
                }
            }

            if(batch == null)
                return;

            while (batchPos < batch.length) {
                Document doc;
                ScoreDoc scoreDoc =batch[batchPos++];

                try {
                    doc = searcher.doc(scoreDoc.doc);
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }

                ClassLoader ldr = null;

                if (ctx != null && ctx.deploy().enabled())
                    ldr = ctx.cache().internalCache(cacheName).context().deploy().globalLoader();

                K k = unmarshall(doc.getBinaryValue(KEY_FIELD_NAME).bytes, ldr);

                if (filters != null && !filters.apply(k))
                    continue;

//                V v = type.valueClass() == String.class ?
//                    (V)doc.get(VAL_STR_FIELD_NAME) :
//                    this.<V>unmarshall(doc.getBinaryValue(VAL_FIELD_NAME).bytes, ldr);

                V v = valueGetter.getValue(k);

                assert v != null;

                curr = new IgniteBiTuple<>(k, v);

                break;
            }
        }

        private boolean shouldRequestNextBatch()  {
            if(remains <= 0) return false;

            if(batch == null){
                // should request for first batch
                return (batchPos == BatchPosBeforeHead) ;
            } else {
                // should request when reached to the end of batch
                return (batchPos  == batch.length);
            }
        }

        private void requestNextBatch() throws IOException {
            TopDocs docs;
            int remains0 = Math.min(pageSize, this.remains);

            if (batch == null) {
                docs = searcher.search(query, remains0);
            } else {
                if(remains0 > 0){
                    docs = searcher.searchAfter(batch[batch.length - 1], query, remains0);
                }
                else {
                    docs = null;
                }
            }

            if(docs == null || docs.scoreDocs.length ==0) {
                batch = null;
            }else {
                batch = docs.scoreDocs;
                if (batch.length >= remains0)
                    this.remains -= batch.length;
                else
                    this.remains = 0;
            }

            batchPos = 0;
        }

        private void skip(int count){
            while(hasNext() && count>0){
                next();
                count --;
            }
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<K, V> onNext() throws IgniteCheckedException {
            IgniteBiTuple<K, V> res = curr;

            findNext();

            return res;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            return curr != null;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            //U.closeQuiet(reader);
        }
    }

    private static class BoostedScoreQuery extends CustomScoreQuery {
        private final Pair<String, Float>[] boostedProperties;

        public BoostedScoreQuery(Query subQuery, Pair<String, Float>[] boostedProperties) {
            super(subQuery);
            this.boostedProperties = boostedProperties;
        }

        // The CustomScoreProvider is what actually alters the score
        private class MyScoreProvider extends CustomScoreProvider {

            private LeafReader reader;
            private String[] fields;
            private Set<String> fieldsSet;
            private Float[] rates;

            public MyScoreProvider(LeafReaderContext context, Pair<String, Float>[] boostedProperties) {
                super(context);
                reader = context.reader();

                fields = new String[boostedProperties.length];
                rates = new Float[boostedProperties.length];
                fieldsSet = new HashSet<>();

                for (int i = 0; i < boostedProperties.length; i++) {
                    Pair<String, Float> p = boostedProperties[i];
                    String name = GridLuceneFieldFactory.NumericStoredName(p.getKey());
                    fields[i] = name;
                    rates[i] = p.getValue();
                    fieldsSet.add(name);
                }
            }

            @Override
            public float customScore(int doc_id, float currentScore, float valSrcScore) throws IOException {
                if(currentScore<10)
                    return currentScore;

                Document doc = reader.document(doc_id, fieldsSet);

                float boost = 0;

                for (int i = 0; i < fields.length; i++) {
                    String fieldName = fields[i];
                    IndexableField field = doc.getField(fieldName);
                    if (field == null) continue;
                    float rate = rates[i];

                    boost += field.numericValue().doubleValue() * rate;
                }

                return currentScore + boost;
            }
        }

        // Make sure that our CustomScoreProvider is being used.
        @Override
        public CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) {
            return new MyScoreProvider(context, boostedProperties);
        }
    }

    private static class Pair<K,V>{
        private K key;
        private V value;

        private Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey(){
            return this.key;
        }

        public V getValue(){
            return this.value;
        }
    }

    public static class Functions {

        public static final String SCORE_FIELD_NAME = "_SCORE";


        @QuerySqlFunction(alias = "FT_SEARCH")
        public static ResultSet search(Connection conn, String table, String qry, int offset, int limit) throws SQLException
        {
            GridLuceneIndex c = luceneIndexes.getOrDefault(table, null);
            if(c == null)
                throw new SQLException("Table was not found: "+ table);

            qry  = qry.toUpperCase();
            String boostClauses = null;
            String[] queryArr = qry.split(QUERY_BOOST_PREFIX);

            if(queryArr.length>2)
                throw new SQLException("Bad full text search query");

            qry = queryArr[0];
            if(queryArr.length == 2)
                boostClauses = queryArr[1];

            Session session = (Session) ((JdbcConnection)conn).getSession();

            GridH2Table table1 = (GridH2Table) session.getDatabase().getSchema(session.getCurrentSchemaName()).findTableOrView(session, c.cacheName.toUpperCase());

            List<Column> columns = new ArrayList<>();

            for(String colName: c.keyFields){
                if(!table1.doesColumnExist(colName)) continue;

                Column col0 = table1.getColumn(colName).getClone();
                col0.setPrimaryKey(true);
                assert  col0 != null;

                columns.add(col0);
            }

            Column scoreColumn = new Column(SCORE_FIELD_NAME, Types.FLOAT);
            scoreColumn.setNullable(true);
            scoreColumn.setConvertNullToDefault(true);

            columns.add(scoreColumn);

            Iterator<Object[]> iterator = new GridEmptyIterator<>();

            try {
                if (c.prepareQuery()){
                    Query query = c.parseQuery(qry);
                    query = c.parseBoostClause(query, boostClauses);

                    ClassLoader ldr = null;
                    GridKernalContext ctx = c.ctx;

                    if (ctx != null && ctx.deploy().enabled())
                        ldr = ctx.cache().internalCache(c.cacheName).context().deploy().globalLoader();

                    iterator = new LuceneResultSet.It(c.searcher, query, null, c.objectContext(), ldr, columns.toArray(new Column[0]),20, offset, limit );
                }
            } catch (IgniteCheckedException e) {
                throw new SQLException(e);
            }

            return new LuceneResultSet(iterator,  columns.toArray(new Column[0]));
        }
    }

}