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
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;
import org.h2.util.JdbcUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;

/**
 * Lucene fulltext index.
 */
public class GridLuceneIndex implements AutoCloseable {
    /** Field name for string representation of value. */
    public static final String VAL_STR_FIELD_NAME = "_gg_val_str__";

    /** Field name for value version. */
    public static final String VER_FIELD_NAME = "_gg_ver__";

    /** Field name for value expiration time. */
    public static final String EXPIRATION_TIME_FIELD_NAME = "_gg_expires__";

    /** */
    private final String cacheName;

    /** */
    private final GridQueryTypeDescriptor type;

    /** */
    private final IndexWriter writer;

    private DirectoryReader reader;

    private IndexSearcher searcher;

    private Analyzer queryAnalyzer;

    /** */
    private final String[] idxdFields;

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

        try {

            final PdsFolderSettings folderSettings = ctx.pdsFolderResolver().resolveFolders();

            Path path =Paths.get(folderSettings.persistentStoreRootPath().getPath(), folderSettings.folderName(), "LuceneIndex-"+cacheName);

            dir =
                    isPersist
                            ? FSDirectory.open(path)
                            : new GridLuceneDirectory(new GridUnsafeMemory(0));

            IndexWriterConfig writerConfig = new IndexWriterConfig(new Analyzer() {

                @Override
                protected TokenStreamComponents createComponents(String fieldName) {
                    Tokenizer tokenizer = new StandardTokenizer();

                    TokenStream stream = tokenizer;
                    stream = new NGramTokenFilter(stream,2,20,false);
                    stream = new ASCIIFoldingFilter(stream);
                    stream = new LowerCaseFilter(stream);
                    return new TokenStreamComponents(tokenizer, stream);
                }
            });

            queryAnalyzer =  new Analyzer() {
                @Override
                protected TokenStreamComponents createComponents(String s) {
                    Tokenizer tokenizer = new StandardTokenizer();

                    TokenStream stream = tokenizer;
                    stream = new ASCIIFoldingFilter(stream);
                    stream = new LowerCaseFilter(stream);
                    stream = new LengthFilter(stream, 2,20);
                    return new TokenStreamComponents(tokenizer, stream);
                }
            };

            writer = new IndexWriter(dir, writerConfig);

            if(DirectoryReader.indexExists(dir)) {
                reader = DirectoryReader.open(dir);
                searcher = new IndexSearcher(reader);
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

            if (fieldVal != null) {
                doc.add(new TextField(idxdFields[i], fieldVal.toString(), Field.Store.YES));

                stringsFound = true;
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

            if (type.valueClass() != String.class)
                doc.add(new StoredField(VAL_FIELD_NAME, v.valueBytes(coctx)));

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
                    searcher = new IndexSearcher(reader);
                }
                else
                    return new GridEmptyCloseableIterator<>();
            }else {
                DirectoryReader newReader = DirectoryReader.openIfChanged(reader);

                if(newReader != null){
                    reader= newReader;
                    searcher = new IndexSearcher(reader);
                }
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }


        Query query;

        try {

            MultiFieldQueryParser parser = new MultiFieldQueryParser(idxdFields, queryAnalyzer);

//            parser.setAllowLeadingWildcard(true);

            // Filter expired items.
            Query filter = LongPoint.newRangeQuery(EXPIRATION_TIME_FIELD_NAME, U.currentTimeMillis(), Long.MAX_VALUE);

            query = new BooleanQuery.Builder()
                .add(parser.parse(qry), BooleanClause.Occur.MUST)
                .add(filter, BooleanClause.Occur.FILTER)
                .build();
        }
        catch (Exception e) {
            //U.closeQuiet(reader);

            throw new IgniteCheckedException(e);
        }

        IndexingQueryCacheFilter fltr = null;

        if (filters != null)
            fltr = filters.forCache(cacheName);

        return new It<>(reader, searcher, query, fltr, pageSize);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(writer);
        U.close(dir, ctx.log(GridLuceneIndex.class));
    }

    /**
     * Key-value iterator over fulltext search result.
     */
    private class It<K, V> extends GridCloseableIteratorAdapter<IgniteBiTuple<K, V>> {
        private final int BatchPosBeforeHead = -1;

        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final int pageSize;
        private final int take;

        /** */
        private final IndexReader reader;

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
         * @param reader Reader.
         * @param searcher Searcher.
         * @param filters Filters over result.
         * @throws IgniteCheckedException if failed.
         */
        private It(IndexReader reader, IndexSearcher searcher, Query query, IndexingQueryCacheFilter filters, int pageSize)
            throws IgniteCheckedException {
            this.reader = reader;
            this.searcher = searcher;
            this.filters = filters;
            this.query = query;
            this.pageSize = pageSize;
            this.take = pageSize;

            coctx = objectContext();

            findNext();
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

                V v = type.valueClass() == String.class ?
                    (V)doc.get(VAL_STR_FIELD_NAME) :
                    this.<V>unmarshall(doc.getBinaryValue(VAL_FIELD_NAME).bytes, ldr);

                assert v != null;

                curr = new IgniteBiTuple<>(k, v);

                break;
            }
        }

        private boolean shouldRequestNextBatch()  {
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

            if (batch == null) {
                docs = searcher.search(query, pageSize);
            } else {

                int read = batch.length*pageSize;

                int remains = take - read;
                remains = Math.min(pageSize, remains);

                if(remains > 0)
                    docs = searcher.searchAfter(batch[batch.length - 1], query, remains);
                else docs = null;
            }

            if(docs == null || docs.scoreDocs.length ==0) {
                batch = null;
            }else{
                batch = docs.scoreDocs;
            }

            batchPos = 0;
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
}