package es.haritzmedina.examples.hbase;

import es.haritzmedina.examples.domain.Person;
import es.haritzmedina.examples.io.Serializer;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HBaseClientTest {

    static String TABLE_NAME = "testTable";
    static String FAMILY_NAME = "testFamily";
    static String ROW = "testRow";
    static String QUALIFIER_NAME = "testQuialifier";
    static int NUMBER_OF_PUTS = 10000;

    static HBaseClient hBaseClient;
    static Table table;

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseClientTest.class);

    @Before
    public void setUp(){
        hBaseClient = new HBaseClient();
        hBaseClient.openConnection();
    }

    @After
    public void tearDown(){
        hBaseClient.closeConnection();
    }

    @Test
    public void putGetTest(){
        // Create table
        hBaseClient.createTable(TABLE_NAME, FAMILY_NAME);

        // Get table
        table = hBaseClient.getTable(TABLE_NAME);

        // Put elements on table
        List<Person> putPeople = putElements();

        // Get elements from table
        List<Person> getPeople = getElements();

        // Assert put elements are the same to get elements
        for(int i=0;i<NUMBER_OF_PUTS;i++){
            assertEquals(putPeople.get(i), getPeople.get(i));
        }

        // Delete table
        hBaseClient.deleteTable(TABLE_NAME);
    }

    @Test
    public void putScanTest(){
        // Create table
        hBaseClient.createNewTable(TABLE_NAME, FAMILY_NAME);

        // Get table
        table = hBaseClient.getTable(TABLE_NAME);

        // Put elements on table
        List<Person> putPeople = putElements();

        // Scan elements
        List<Person> scannedPeople = scanElements();

        // Check if putted elements are in scanned list
        for(int i=0;i<putPeople.size();i++){
            assertTrue(scannedPeople.contains(putPeople.get(i)));
        }
    }

    private List<Person> scanElements() {
        ResultScanner resultScanner = hBaseClient.scan(table, FAMILY_NAME, QUALIFIER_NAME);
        List<Person> people = new ArrayList<Person>();
        try {
            for (Result rr = resultScanner.next(); rr != null; rr = resultScanner.next()) {
                // print out the row we found and the columns we were looking for
                byte[] bytearray = rr.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(QUALIFIER_NAME));
                Person person = (Person) Serializer.deserialize(bytearray);
                people.add(person);
                logger.debug("Scanned value: " + person);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            resultScanner.close();
        }
        return people;
    }

    public List<Person> putElements(){
        // Put elements on table
        List<Person> people = new ArrayList<Person>();
        List<Put> puts = new ArrayList<Put>();
        for(int i=0; i<NUMBER_OF_PUTS;i++){
            Put put = new Put(Bytes.toBytes(ROW + i));
            put.setDurability(Durability.USE_DEFAULT);
            Person person = new Person("Haritz", "Medina" + i);
            people.add(person);
            logger.debug("Added to put list: " + person.toString());
            byte[] personSerialized = new byte[0];
            try {
                personSerialized = Serializer.serialize(person);
            } catch (IOException e) {
                logger.error("Cannot serialize person");
            }
            put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(QUALIFIER_NAME), (new Date()).getTime(), personSerialized);
            puts.add(put);
        }
        hBaseClient.put(table, puts);
        return people;
    }

    public List<Person> getElements(){
        // Get list
        List<Get> gets = new ArrayList<Get>();
        for(int i=0; i<NUMBER_OF_PUTS;i++){
            Get get = new Get(Bytes.toBytes(ROW + i));
            gets.add(get);
        }
        Result[] results = hBaseClient.get(table, gets);
        List<Result> resultList = Arrays.asList(results);
        List<Person> people = new ArrayList<Person>();
        // Print list
        for(Result result : resultList){
            byte[] byteResult = result.getValue(
                    Bytes.toBytes(FAMILY_NAME),
                    Bytes.toBytes(QUALIFIER_NAME)
            );
            try {
                Person person = (Person) Serializer.deserialize(byteResult);
                people.add(person);
                logger.debug("Getted value: " + person.toString());
            } catch (IOException e) {
                logger.error("Cannot deserialize person");
            } catch (ClassNotFoundException e) {
                logger.error("Cannot deserialize person");
            }
        }
        return people;
    }
}