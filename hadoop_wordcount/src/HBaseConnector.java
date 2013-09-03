import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
 
public class HBaseConnector {
 
    private static Configuration conf = null;
    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
    }
 
    /**
     * Create a table
     */
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                HColumnDescriptor hCd = new HColumnDescriptor(familys[i]);
                //hCd.setMaxVersions(1);
                tableDesc.addFamily(hCd);
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }
 
    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }
 
    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
            String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }
 
    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
             HTable table = new HTable(conf, tableName);
             Scan s = new Scan();
             ResultScanner ss = table.getScanner(s);
             for(Result r:ss){
                 for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                 }
             }
        } catch (IOException e){
            e.printStackTrace();
        }
    }
 
    private static void insertTestData(String tableName, String rowKey) throws Exception {
        HBaseConnector.addRecord(tableName, rowKey, "grade", "", "5");
        HBaseConnector.addRecord(tableName, rowKey, "course", "", "90");
        HBaseConnector.addRecord(tableName, rowKey, "course", "math", "97");
        HBaseConnector.addRecord(tableName, rowKey, "course", "art", "87");        
    }
    
    private static void filterRecord(String tableName) {
        try{
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            
            Filter filter = new RowFilter(org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator(".*AUG-12.*"));
            s.setFilter(filter);
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                   System.out.print(new String(kv.getRow()) + " ");
                   System.out.print(new String(kv.getFamily()) + ":");
                   System.out.print(new String(kv.getQualifier()) + " ");
                   System.out.print(kv.getTimestamp() + " ");
                   System.out.println(new String(kv.getValue()));
                }
            }
       } catch (IOException e){
           e.printStackTrace();
       }
    }
    
    public static void main(String[] agrs) {
        try {
            String tablename = "scores";
            String[] familys = { "grade", "course" };
            HBaseConnector.creatTable(tablename, familys);
 
            // add record zkb
            HBaseConnector.insertTestData(tablename, "PEMEL3~|4~|09-AUG-13 05.42.40.750000 AM~|T1U382.23");
            HBaseConnector.insertTestData(tablename, "PEMEL3~|4~|09-AUG-13 05.42.42.750000 AM~|T1U382.23");
            HBaseConnector.insertTestData(tablename, "PEMEL3~|4~|09-AUG-13 05.23.40.750000 AM~|T1U382.23");
            HBaseConnector.insertTestData(tablename, "PEMEL3~|4~|09-AUG-13 05.42.55.750000 AM~|T1U382.23");
            HBaseConnector.insertTestData(tablename, "PEMEL3~|4~|09-AUG-13 07.42.40.750000 AM~|T1U382.23");
            HBaseConnector.insertTestData(tablename, "PEMEL3~|4~|09-AUG-12 09.42.40.750000 AM~|T1U382.23");
            HBaseConnector.insertTestData(tablename, "PEMEL3~|4~|09-AUG-13 00.42.40.750000 AM~|T1U382.23");
            
            // add record baoniu
            HBaseConnector.addRecord(tablename, "baoniu", "grade", "", "4");
            HBaseConnector.addRecord(tablename, "baoniu", "course", "math", "89");
 
            System.out.println("===========get one record========");
            HBaseConnector.getOneRecord(tablename, "zkb");
 
            System.out.println("===========show all record========");
            HBaseConnector.getAllRecord(tablename);
 
            System.out.println("===========del one record========");
            HBaseConnector.delRecord(tablename, "baoniu");
            HBaseConnector.getAllRecord(tablename);
 
            System.out.println("===========show all record========");
            HBaseConnector.getAllRecord(tablename);
            
            System.out.println("===========show rowkey filter record========");
            HBaseConnector.filterRecord(tablename);

            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}