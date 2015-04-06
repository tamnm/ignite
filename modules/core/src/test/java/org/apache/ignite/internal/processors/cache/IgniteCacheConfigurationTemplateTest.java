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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;

/**
 *
 */
public class IgniteCacheConfigurationTemplateTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String TEMPLATE1 = "org.apache.ignite*";

    /** */
    private static final String TEMPLATE2 = "org.apache.ignite.test.*";

    /** */
    private static final String TEMPLATE3 = "org.apache.ignite.test2.*";

    /** */
    private boolean clientMode;

    /** */
    private boolean addTemplate;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (addTemplate) {
            CacheConfiguration dfltCfg = new CacheConfiguration();

            dfltCfg.setAtomicityMode(TRANSACTIONAL);
            dfltCfg.setBackups(2);

            CacheConfiguration templateCfg1 = new CacheConfiguration();

            templateCfg1.setName(TEMPLATE1);
            templateCfg1.setBackups(3);

            CacheConfiguration templateCfg2 = new CacheConfiguration();

            templateCfg2.setName(TEMPLATE2);
            templateCfg2.setBackups(4);

            cfg.setCacheConfiguration(dfltCfg, templateCfg1, templateCfg2);
        }

        cfg.setClientMode(clientMode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateFromTemplateConfiguration() throws Exception {
        addTemplate = true;

        Ignite ignite0 = startGrid(0);

        checkNoTemplateCaches(1);

        checkGetOrCreate(ignite0, "org.apache.ignite.test.cache1", 4);
        checkGetOrCreate(ignite0, "org.apache.ignite.test.cache1", 4);

        Ignite ignite1 = startGrid(1);

        checkGetOrCreate(ignite1, "org.apache.ignite.test.cache1", 4);
        checkGetOrCreate(ignite1, "org.apache.ignite.test.cache1", 4);

        checkGetOrCreate(ignite1, "org.apache.ignite1", 3);
        checkGetOrCreate(ignite1, "org.apache.ignite1", 3);

        checkGetOrCreate(ignite0, "org.apache.ignite1", 3);
        checkGetOrCreate(ignite0, "org.apache.ignite1", 3);

        checkGetOrCreate(ignite0, "org.apache1", 2);
        checkGetOrCreate(ignite1, "org.apache1", 2);

        checkNoTemplateCaches(2);

        addTemplate = false;
        clientMode = true;

        Ignite ignite2 = startGrid(2);

        assertNotNull(ignite2.cache("org.apache.ignite.test.cache1"));
        assertNotNull(ignite2.cache("org.apache.ignite1"));
        assertNotNull(ignite2.cache("org.apache1"));

        checkGetOrCreate(ignite2, "org.apache.ignite.test.cache1", 4);
        checkGetOrCreate(ignite2, "org.apache.ignite1", 3);
        checkGetOrCreate(ignite2, "org.apache1", 2);

        checkGetOrCreate(ignite2, "org.apache.ignite.test.cache2", 4);
        checkGetOrCreate(ignite2, "org.apache.ignite.cache2", 3);
        checkGetOrCreate(ignite2, "org.apache2", 2);

        CacheConfiguration template1 = new CacheConfiguration();

        template1.setName(TEMPLATE3);
        template1.setBackups(5);

        ignite2.addCacheConfiguration(template1);

        checkGetOrCreate(ignite0, "org.apache.ignite.test2.cache1", 5);
        checkGetOrCreate(ignite1, "org.apache.ignite.test2.cache1", 5);
        checkGetOrCreate(ignite2, "org.apache.ignite.test2.cache1", 5);

        Ignite ignite3 = startGrid(3);

        checkGetOrCreate(ignite3, "org.apache.ignite.test2.cache1", 5);

        checkNoTemplateCaches(4);

        // Template with non-wildcard name.
        CacheConfiguration template2 = new CacheConfiguration();

        template2.setName("org.apache.ignite");
        template2.setBackups(6);

        ignite0.addCacheConfiguration(template2);

        checkGetOrCreate(ignite0, "org.apache.ignite", 6);
        checkGetOrCreate(ignite1, "org.apache.ignite", 6);
        checkGetOrCreate(ignite2, "org.apache.ignite", 6);
        checkGetOrCreate(ignite3, "org.apache.ignite", 6);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartClientNodeFirst() throws Exception {
        addTemplate = true;
        clientMode = true;

        Ignite ignite0 = startGrid(0);

        checkNoTemplateCaches(0);

        addTemplate = false;
        clientMode = false;

        Ignite ignite1 = startGrid(1);

        checkGetOrCreate(ignite1, "org.apache.ignite.test.cache1", 4);
        checkGetOrCreate(ignite1, "org.apache.ignite.test.cache1", 4);

        checkGetOrCreate(ignite1, "org.apache.ignite1", 3);
        checkGetOrCreate(ignite1, "org.apache.ignite1", 3);

        checkGetOrCreate(ignite0, "org.apache.ignite1", 3);
        checkGetOrCreate(ignite0, "org.apache.ignite1", 3);
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     * @param expBackups Expected number of backups.
     */
    private void checkGetOrCreate(Ignite ignite, String name, int expBackups) {
        IgniteCache cache = ignite.getOrCreateCache(name);

        assertNotNull(cache);

        CacheConfiguration cfg = (CacheConfiguration)cache.getConfiguration(CacheConfiguration.class);

        assertEquals(name, cfg.getName());
        assertEquals(expBackups, cfg.getBackups());
    }

    /**
     * @param nodes Nodes number.
     */
    private void checkNoTemplateCaches(int nodes) {
        for (int i = 0; i < nodes; i++) {
            final Ignite ignite = grid(i);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ignite.cache(GridCacheUtils.UTILITY_CACHE_NAME);

                    return null;
                }
            }, IllegalStateException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ignite.cache(TEMPLATE1);

                    return null;
                }
            }, IllegalArgumentException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ignite.cache(TEMPLATE2);

                    return null;
                }
            }, IllegalArgumentException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ignite.cache(TEMPLATE3);

                    return null;
                }
            }, IllegalArgumentException.class, null);
        }
    }
}
