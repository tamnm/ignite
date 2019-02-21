package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

public class ClientCacheTextQueryRequest extends ClientCacheRequest {
    /** Query. */
    private final TextQuery qry;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheTextQueryRequest(BinaryRawReaderEx reader) {
        super(reader);

        qry = new TextQuery(reader.readString(), reader.readString())
                .setLocal(reader.readBoolean())
                .setPageSize(reader.readInt());
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteCache cache = cache(ctx);

        ctx.incrementCursors();

        try {
            QueryCursor cur = cache.query(qry);

            ClientCacheEntryQueryCursor cliCur = new ClientCacheEntryQueryCursor(
                    cur, qry.getPageSize(), ctx);

            long cursorId = ctx.resources().put(cliCur);

            cliCur.id(cursorId);

            return new ClientCacheQueryResponse(requestId(), cliCur);
        }
        catch (Exception e) {
            ctx.decrementCursors();

            throw e;
        }
    }
}