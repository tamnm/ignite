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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.sql.*;
import java.util.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;

/**
 *
 */
public class IgniteVsH2QueryTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    
    /** Partitioned cache. */
    private static IgniteCache pCache;

    /** Replicated cache. */
    private static IgniteCache rCache;
    
    /** H2 db connection. */
    private static Connection conn;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setMarshaller(new OptimizedMarshaller(true));

        c.setCacheConfiguration(createCache("partitioned", CacheMode.PARTITIONED), 
            createCache("replicated", CacheMode.REPLICATED)
        );

        return c;
    }

    /**
     * Creates new cache configuration.
     *
     * @param name Cache name.
     * @param mode Cache mode.
     * @return Cache configuration.
     */
    private static CacheConfiguration createCache(String name, CacheMode mode) {
        CacheConfiguration<?,?> cc = defaultCacheConfiguration();

        cc.setName(name);
        cc.setCacheMode(mode);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setEvictNearSynchronized(false);
        cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cc.setDistributionMode(PARTITIONED_ONLY);

        if (mode == CacheMode.PARTITIONED)
            cc.setIndexedTypes(
                Integer.class, Organization.class,
                CacheAffinityKey.class, Person.class,
                CacheAffinityKey.class, Purchase.class
            );
        else if (mode == CacheMode.REPLICATED)
            cc.setIndexedTypes(
                Integer.class, Product.class
            );
        else
            throw new IllegalStateException("mode: " + mode);

        return cc;
    }


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignite ignite = startGrids(4);

        pCache = ignite.jcache("partitioned");
        
        rCache = ignite.jcache("replicated");

        awaitPartitionMapExchange();
        
        conn = openH2Connection(false);

        initializeH2Schema();

        initCacheAndDbData();

        checkAllDataEquals();
    }
    
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        
        conn.close();
        
        stopAllGrids();
    }

    /**
     * Populate cache with test data.
     */
    @SuppressWarnings("unchecked")
    private void initCacheAndDbData() throws SQLException {
        int idGen = 0;
        
        // Organizations.
        List<Organization> orgs = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            int id = idGen++;
            
            Organization org = new Organization(id, "Org" + id);
            
            orgs.add(org);
            
            pCache.put(org.id, org);
            
            insertInDb(org);
        }

        // Persons.
        List<Person> persons = new ArrayList<>();
        
        for (int i = 0; i < 5; i++) {
            int id = idGen++;

            Person person = new Person(id, orgs.get(i % orgs.size()), "name" + id, "lastname" + id, id * 100.0);

            persons.add(person);

            pCache.put(person.key(), person);
            
            insertInDb(person);
        }

        // Products.
        List<Product> products = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            int id = idGen++;
            
            Product product = new Product(id, "Product" + id, id*1000);
            
            products.add(product);
            
            rCache.put(product.id, product);
            
            insertInDb(product);
        }

        // Purchases.
        for (int i = 0; i < products.size() * 2; i++) {
            int id = idGen++;

            Purchase purchase = new Purchase(id, products.get(i % products.size()), persons.get(i % persons.size()));

            pCache.put(purchase.key(), purchase);
            
            insertInDb(purchase);
        }
    }

    private void insertInDb(Organization org) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement("insert into ORGANIZATION (id, name) values(?, ?)")) {
            st.setInt(1, org.id);
            st.setString(2, org.name);

            st.executeUpdate();
        }
    }

    private void insertInDb(Person p) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement("insert into PERSON (id, firstName, lastName, orgId, salary) values(?, ?, ?, ?, ?)")) {
            st.setInt(1, p.id);
            st.setString(2, p.firstName);
            st.setString(3, p.lastName);
            st.setInt(4, p.orgId);
            st.setDouble(5, p.salary);

            st.executeUpdate();
        }
    }

    private void insertInDb(Product p) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement("insert into PRODUCT (id, name, price) values(?, ?, ?)")) {
            st.setInt(1, p.id);
            st.setString(2, p.name);
            st.setInt(3, p.price);

            st.executeUpdate();
        }
    }

    private void insertInDb(Purchase p) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement("insert into PURCHASE (id, personId, productId) values(?, ?, ?)")) {
            st.setInt(1, p.id);
            st.setInt(2, p.personId);
            st.setInt(3, p.productId);

            st.executeUpdate();
        }
    }

    private void initializeH2Schema() throws SQLException {
        Statement st = conn.createStatement();
        
        st.execute("create table if not exists ORGANIZATION" +
            "  (id int unique," +
            "  name varchar(255))");
        
        st.execute("create table if not exists PERSON" +
            "  (id int unique, " +
            "  firstName varchar(255), " +
            "  lastName varchar(255)," +
            "  orgId int not null," +
            "  salary double )");

        st.execute("create table if not exists PRODUCT" +
            "  (id int unique, " +
            "  name varchar(255), " +
            "  price int)");

        st.execute("create table if not exists PURCHASE" +
            "  (id int unique, " +
            "  personId int, " +
            "  productId int)");

        conn.commit();
    }


    /**
     * Gets connection from a pool.
     *
     * @param autocommit {@code true} If connection should use autocommit mode.
     * @return Pooled connection.
     * @throws SQLException In case of error.
     */
    private Connection openH2Connection(boolean autocommit) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:example;DB_CLOSE_DELAY=-1");

        conn.setAutoCommit(autocommit);

        return conn;
    }

    private void test0(String sql, Object... args) throws SQLException {
        test0(pCache, sql, args, Order.RANDOM);
    }

    private void test0(IgniteCache cache, String sql, Object... args) throws SQLException {
        test0(cache, sql, args, Order.RANDOM);
    }

    private void test0Ordered(String sql, Object... args) throws SQLException {
        test0(pCache, sql, args, Order.ORDERED);
    }

    @SuppressWarnings("unchecked")
    private void test0(IgniteCache cache, String sql, Object[] args, Order order) throws SQLException {
        log.info("Sql=" + sql + ", args=" + Arrays.toString(args));

        List<List<?>> h2Res = executeH2Query(sql, args);

        List<List<?>> cacheRes = cache.queryFields(new SqlFieldsQuery(sql).setArgs(args)).getAll();

        assertRsEquals(h2Res, cacheRes, order);
    }
    
    private List<List<?>> executeH2Query(String sql, Object[] args) throws SQLException {
        List<List<?>> res = new ArrayList<>();
        ResultSet rs = null;

        try(PreparedStatement st = conn.prepareStatement(sql)) {
            for (int idx = 0; idx < args.length; idx++) {
                Object arg = args[idx];

                fillArgByType(st, idx + 1, arg);
            }

            rs = st.executeQuery();

            int colCnt = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                List<Object> row = new ArrayList<>(colCnt);
                
                for (int i = 1; i <= colCnt; i++)
                    row.add(rs.getObject(i));
                
                res.add(row);
            }
        }
        finally {
            U.closeQuiet(rs);
        }

        return res;
    }

    private void assertRsEquals(List<List<?>> rs1, List<List<?>> rs2, Order order) {
        assertEquals("Rows count has to be equal.", rs1.size(), rs2.size());
        
        switch (order){
            case ORDERED:
                for (int rowNum = 0; rowNum < rs1.size(); rowNum++) {
                    List<?> row1 = rs1.get(rowNum);
                    List<?> row2 = rs2.get(rowNum);

                    assertEquals("Columns count have to be equal.", row1.size(), row2.size());

                    for (int colNum = 0; colNum < row1.size(); colNum++)
                        assertEquals("Row=" + rowNum + ", column=" + colNum, row1.get(colNum), row2.get(colNum));
                }

                break;
            case RANDOM:
                for (List<?> row1 : rs1)
                    assertTrue("Actual result set has to contain row.\n" + "Result set=" + rs2 + "\n" + "Row=" + row1, 
                        rs2.contains(row1));
                
                break;
            default: 
                throw new IllegalStateException();
        }
    }

    private static String currentRsRow2String(ResultSet rs) throws SQLException {
        GridStringBuilder sb = new GridStringBuilder("[");

        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            Object o = rs.getObject(i);
            sb.a(o != null ? o.toString() : "null").a(',');
        }

        sb.d(sb.length() - 1).a(']');

        return sb.toString();
    }

    private static boolean rowEqualsCurrentRsRow(ResultSet rs, List<?> row) throws SQLException {
        for (int colNum = 0; colNum < row.size(); colNum++) {
            Object o1 = row.get(colNum);

            assertNotNull("Unexpected null value. Row=" + row + ", column=" + colNum + '.', o1);

            Object o2 = extractColumn(rs, colNum + 1, o1.getClass());

            assertNotNull("Unexpected null value. Column=" + colNum + '.', o2);

            if (!(o1.equals(o2)))
                return false;
        }

        return true;
    }

    private void fillArgByType(PreparedStatement st, int idx, Object arg) throws SQLException {
        Class<?> propType = arg.getClass();

        if (propType.equals(String.class))
            st.setString(idx, (String)arg);
        else if (propType.equals(Integer.TYPE) || propType.equals(Integer.class))
            st.setInt(idx, (Integer)arg);
        else if (propType.equals(Boolean.TYPE) || propType.equals(Boolean.class))
            st.setBoolean(idx, (Boolean)arg);
        else if (propType.equals(Long.TYPE) || propType.equals(Long.class))
            st.setLong(idx, (Long)arg);
        else if (propType.equals(Double.TYPE) || propType.equals(Double.class))
            st.setDouble(idx, (Double)arg);
        else if (propType.equals(Float.TYPE) || propType.equals(Float.class))
            st.setFloat(idx, (Float)arg);
        else if (propType.equals(Short.TYPE) || propType.equals(Short.class))
            st.setShort(idx, (Short)arg);
        else if (propType.equals(Byte.TYPE) || propType.equals(Byte.class))
            st.setByte(idx, (Byte)arg);
        else if (propType.equals(Timestamp.class))
            st.setTimestamp(idx, (Timestamp)arg);
        else if (propType.equals(SQLXML.class))
            st.setSQLXML(idx, (SQLXML)arg);
        else // Object.
            st.setObject(idx, arg);
    }

    private static Object extractColumn(ResultSet rs, int idx, Class<?> propType) throws SQLException {
        if (!propType.isPrimitive() && rs.getObject(idx) == null)
            return null;

        if (propType.equals(String.class))
            return rs.getString(idx);
        else if (propType.equals(Integer.TYPE) || propType.equals(Integer.class))
            return rs.getInt(idx);
        else if (propType.equals(Boolean.TYPE) || propType.equals(Boolean.class))
            return rs.getBoolean(idx);
        else if (propType.equals(Long.TYPE) || propType.equals(Long.class))
            return rs.getLong(idx);
        else if (propType.equals(Double.TYPE) || propType.equals(Double.class))
            return rs.getDouble(idx);
        else if (propType.equals(Float.TYPE) || propType.equals(Float.class))
            return rs.getFloat(idx);
        else if (propType.equals(Short.TYPE) || propType.equals(Short.class))
            return rs.getShort(idx);
        else if (propType.equals(Byte.TYPE) || propType.equals(Byte.class))
            return rs.getByte(idx);
        else if (propType.equals(Timestamp.class))
            return rs.getTimestamp(idx);
        else if (propType.equals(SQLXML.class))
            return rs.getSQLXML(idx);
        else // Object.
            return rs.getObject(idx);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAllDataEquals() throws Exception {
        test0("select id, name from Organization");

        test0("select id, firstName, lastName, orgId, salary from Person");

        test0("select id, personId, productId from Purchase");

        test0(rCache, "select id, name, price from Product");
    }

    /**
     * @throws Exception If failed.
     */
    public void testEmptyResult() throws Exception {
        test0("select id from Person where 0 = 1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSelectWithStar() throws Exception {
        test0("select * from Person");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSelectWithStar2() throws Exception {
        test0("select Person.* from Person");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlQueryWithAggregation() throws Exception {
        test0("select avg(salary) from Person, Organization where Person.orgId = Organization.id and "
            + "lower(Organization.name) = lower(?)", "Org1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlFieldsQuery() throws Exception {
        test0("select concat(firstName, ' ', lastName) from Person");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlFieldsQueryWithJoin() throws Exception {
        test0("select concat(firstName, ' ', lastName), "
            + "Organization.name from Person, Organization where "
            + "Person.orgId = Organization.id");
    }

    /**
     * @throws Exception If failed.
     */
    public void testOrdered() throws Exception {
        test0Ordered("select firstName, lastName" +
            "  from Person" 
//                +
//            "  order by lastName, firstName"
        );
    }

    /**
     * //TODO Investigate
     *  
     * @throws Exception If failed.
     */
    public void testSimpleJoin() throws Exception {
        // Have expected results.
        test0("select id, firstName, lastName" +
            "  from Person" +
            "  where Person.id = ?", 3);

        // Ignite cache return 0 results...
        test0("select Person.firstName" +
            "  from Person, Purchase" +
            "  where Person.id = ?", 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleReplicatedSelect() throws Exception {
        test0(rCache, "select id, name from \"replicated\".Product");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCache() throws Exception {
        //TODO Investigate (should be 20 results instead of 8)
        test0("select firstName, lastName" +
            "  from Person, Purchase" +
            "  where Person.id = Purchase.personId");

        //TODO Investigate
        test0("select concat(firstName, ' ', lastName), Product.name " +
            "  from Person, Purchase, \"replicated\".Product " +
            "  where Person.id = Purchase.personId and Purchase.productId = Product.id" +
            "  group by Product.id");

        //TODO Investigate
        test0("select concat(firstName, ' ', lastName), count (Product.id) " +
            "  from Person, Purchase, \"replicated\".Product " +
            "  where Person.id = Purchase.personId and Purchase.productId = Product.id" +
            "  group by Product.id");
    }

    /**
     * Person class.
     */
    private static class Person implements Serializable {
        /** Person ID (indexed). */
        @QuerySqlField(index = true)
        private int id;

        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private int orgId;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /** Salary (indexed). */
        @QuerySqlField(index = true)
        private double salary;

        /** Custom cache key to guarantee that person is always collocated with its organization. */
        private transient CacheAffinityKey<Integer> key;

        /**
         * Constructs person record.
         *
         * @param org Organization.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        Person(int id, Organization org, String firstName, String lastName, double salary) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.salary = salary;
            orgId = org.id;
        }

        /**
         * Gets cache affinity key. Since in some examples person needs to be collocated with organization, we create
         * custom affinity key to guarantee this collocation.
         *
         * @return Custom affinity key to guarantee that person is always collocated with organization.
         */
        public CacheAffinityKey<Integer> key() {
            if (key == null)
                key = new CacheAffinityKey<>(id, orgId);

            return key;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person [firstName=" + firstName +
                ", lastName=" + lastName +
                ", id=" + id +
                ", orgId=" + orgId +
                ", salary=" + salary + ']';
        }
    }

    /**
     * Organization class.
     */
    private static class Organization implements Serializable {
        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private int id;

        /** Organization name (indexed). */
        @QuerySqlField(index = true)
        private String name;

        /**
         * Create Organization.
         *
         * @param id Organization ID.
         * @param name Organization name.
         */
        Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization [id=" + id + ", name=" + name + ']';
        }
    }

    /**
     * Product class.
     */
    private static class Product implements Serializable {
        /** Primary key. */
        @QuerySqlField(index = true)
        private int id;

        /** Product name. */
        @QuerySqlField
        private String name;

        /** Product price */
        @QuerySqlField
        private int price;

        /**
         * Create Product.
         *
         * @param id Product ID.
         * @param name Product name.
         * @param price Product price.
         */
        Product(int id, String name, int price) {
            this.id = id;
            this.name = name;
            this.price = price;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Product [id=" + id + ", name=" + name + ", price=" + price + ']';
        }
    }

    /**
     * Purchase class.
     */
    private static class Purchase implements Serializable {
        /** Primary key. */
        @QuerySqlField(index = true)
        private int id;

        /** Product ID. */
        @QuerySqlField
        private int productId;

        /** Person ID. */
        @QuerySqlField
        private int personId;

        /** Custom cache key to guarantee that purchase is always collocated with its person. */
        private transient CacheAffinityKey<Integer> key;

        /**
         * Create Purchase.
         *
         * @param id Purchase ID.
         * @param product Purchase product.
         * @param person Purchase person.
         */
        Purchase(int id, Product product, Person person) {
            this.id = id;
            productId = product.id;
            personId = person.id;
        }

        /**
         * Gets cache affinity key. Since in some examples purchase needs to be collocated with person, we create
         * custom affinity key to guarantee this collocation.
         *
         * @return Custom affinity key to guarantee that purchase is always collocated with person.
         */
        public CacheAffinityKey<Integer> key() {
            if (key == null)
                key = new CacheAffinityKey<>(id, personId);

            return key;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Purchase [id=" + id + ", productId=" + productId + ", personId=" + personId + ']';
        }
    }
    
    private enum Order {
        /** Random. */
        RANDOM, 
        /** Ordered. */
        ORDERED
    }
}
