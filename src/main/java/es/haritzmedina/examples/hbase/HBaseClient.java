package es.haritzmedina.examples.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by Haritz Medina on 10/10/2014.
 */
public class HBaseClient {

    private Configuration hBaseConfiguration;
    private Connection hBaseConnection;

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseClient.class);

    public HBaseClient(){
        this.hBaseConfiguration = HBaseConfiguration.create();
    }

    public void openConnection(){
        try {
            this.hBaseConnection = ConnectionFactory.createConnection(this.hBaseConfiguration);
            logger.debug("Opened connection to hbase zookeeper: " + this.hBaseConfiguration.get("hbase.zookeeper.quorum"));
        } catch (IOException e) {
            // TODO handle exception
            e.printStackTrace();
        }
    }

    public void closeConnection(){
        try {
            this.hBaseConnection.close();
        } catch (IOException e) {
            // TODO handle exception
            e.printStackTrace();
        }
    }

    public void createTable(String name, String family){
        try {
            Admin admin = this.hBaseConnection.getAdmin();
            TableName tableName = TableName.valueOf(name);
            // If table don't exists, create a new one
            if(!admin.tableExists(tableName)){
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                hTableDescriptor.addFamily(new HColumnDescriptor(family));
                admin.createTable(hTableDescriptor);
                logger.debug("Created new table: " + name + ", " + family);
            }
        } catch (IOException e) {
            // TODO handle exception
            e.printStackTrace();
        }
    }

    public void createNewTable(String name, String family){
        try {
            Admin admin = this.hBaseConnection.getAdmin();
            TableName tableName = TableName.valueOf(name);
            // If table exists, delete it first
            if(admin.tableExists(tableName)){
                this.deleteTable(name);
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor(family));
            admin.createTable(hTableDescriptor);
            logger.debug("Created new table: " + name + ", " + family);
        } catch (IOException e) {
            // TODO handle exception
            e.printStackTrace();
        }
    }

    public void deleteTable(String name){
        try {
            Admin admin = this.hBaseConnection.getAdmin();
            TableName tableName = TableName.valueOf(name);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            logger.debug("Deleted table: " + name);
        } catch (IOException e) {
            //TODO handle exception
            e.printStackTrace();
        }

    }

    public Table getTable(String name){
        // Create connection
        TableName tableName = TableName.valueOf(name);
        Table table = null;
        try {
             table = this.hBaseConnection.getTable(tableName);
        } catch (IOException e) {
            // TODO handle exception
            e.printStackTrace();
        }
        return table;
    }

    public void put(Table table, Put put){
        try {
            table.put(put);
            logger.debug("Putted element on table: " + table.getName());
        } catch (IOException e) {
            //TODO handle exception
            e.printStackTrace();
        }
    }

    public void put(Table table, List<Put> puts){
        try {
            table.put(puts);
            logger.debug("Putted elements on table: " + table.getName());
        } catch (IOException e) {
            // TODO handle exception
            e.printStackTrace();
        }
    }

    public Result get(Table table, Get get){
        Result result = null;
        try {
            result = table.get(get);
        } catch (IOException e) {
            //TODO handle exception
            e.printStackTrace();
        }
        return result;
    }

    public Result[] get(Table table, List<Get> gets){
        Result[] results = null;
        try {
             results = table.get(gets);
        } catch (IOException e) {
            //TODO handle exception
            e.printStackTrace();
        }
        return results;
    }

    public ResultScanner scan(Table table, String family, String qualifier){
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            logger.debug("Getted scanner from table: " + table.getName());
        } catch (IOException e) {
            //TODO handle exception
            e.printStackTrace();
        }
        return scanner;
    }
}
