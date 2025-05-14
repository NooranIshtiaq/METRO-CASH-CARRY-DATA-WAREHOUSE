package com.mesh_Join.etl;

// Nooran Ishtiaq 
// 22i-2010
// DS-B
// Metro sales Real Time Datawarehouse 

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.sql.Connection;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

public class MeshJoin 
{
	
	private static final int Stream_SIZE = 500;
	private static BlockingQueue<List<String[]>> Disk_Buffer; 
    private static BlockingQueue<StreamChunkState> streamingQueue;
    private static final Map<HashPair<String, String>, String[]> hash_table = new ConcurrentHashMap<>();
    private static final List<String[]>  transformed_data_Table = Collections.synchronizedList(new ArrayList<>());
    private static volatile boolean IS_streamComplete = false;   
    private static final String Databast_URL = "jdbc:mysql://localhost:3306/Metro_Dwh_Project";
    private static final String username = "root";
    private static final String password = "root1234";

    
    public static void main(String[] args) throws IOException {
        // transaction.csv for stream data 
    	String transactionFilePath = "E:/transactions.csv";
        // product and customer files for master data , accessing files from hard disk
    	String productFilePath = "E:/products_data.csv";
        String customerFilePath = "E:/customers_data.csv";
        final int customer_partition_Size=50;
        final int Product_partition_Size=50;
        
        // Customer and product list is used for partitions in one disk buffer 
        List<List<String[]>> productPartitions = partitionMasterData(productFilePath,Product_partition_Size);
        List<List<String[]>> customerPartitions = partitionMasterData(customerFilePath,customer_partition_Size);
        
        int sumPartitions = productPartitions.size() + customerPartitions.size();
        // 30248 are the number of rows in the transaction.csv
        int totalTransactionChunks = 30248 / 50; 
        // setting the size of stream queue 
        streamingQueue = new ArrayBlockingQueue<>(totalTransactionChunks * 2); 
        // the disk buffer size is equal to the sum of partitions of product and customer
        Disk_Buffer= new ArrayBlockingQueue<>(sumPartitions); // Set size dynamically


        Thread Insertion_Thread = new Thread(new StreamDataHandling (transactionFilePath, Stream_SIZE));
        Insertion_Thread.start();
        Thread Loading_Thread = new Thread(new MasterDataHandling(productPartitions, customerPartitions));
        Loading_Thread.start();

        // Monitor transformed table
        new Thread(() -> {
            while (true) {
                synchronized ( transformed_data_Table) {
                    if (! transformed_data_Table.isEmpty()) {
                        System.out.println("Inserting transformed data into database...");
                        load_into_DWH(new ArrayList<>( transformed_data_Table));
                        transformed_data_Table.clear();
                    }
                }
                try {
                    Thread.sleep(5000);  // Wait before checking again
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
    
    // connection for metro datawarehouse in dbms 
    private static Connection getConnection() throws SQLException 
    {
        return DriverManager.getConnection(Databast_URL, username, password);
    }
    
    // Function to load data into data warehouse 
    private static void load_into_DWH(List<String[]> transformedData) {
        // loading transformed product data after transformation
        String product_dim = "INSERT INTO PRODUCT_DIMENSION (PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME) " +
                                     "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                                     "ON DUPLICATE KEY UPDATE " + "PRODUCT_NAME = VALUES(PRODUCT_NAME), " +
                                     "PRODUCT_PRICE = VALUES(PRODUCT_PRICE), " +"SUPPLIER_ID = VALUES(SUPPLIER_ID), " 
                                     +"SUPPLIER_NAME = VALUES(SUPPLIER_NAME), " +
                                     "STORE_ID = VALUES(STORE_ID), " + "STORE_NAME = VALUES(STORE_NAME)";
 
        // loading transformed customer data after transformation
        String customer_dim= "INSERT INTO CUSTOMER_DIMENSION (CUSTOMER_ID, CUSTOMER_NAME, GENDER) " +
                                      "VALUES (?, ?, ?) " +
                                      "ON DUPLICATE KEY UPDATE " +
                                      "CUSTOMER_NAME = VALUES(CUSTOMER_NAME), " +
                                      "GENDER = VALUES(GENDER)";

        // loading transformed time data after transformation
        String time_dim = "INSERT INTO TIME_DIMENSION (TIME_ID, Orderdate, day, month, quarter, year, week, day_of_week) " +
        "VALUES (?, ?, DAY(?), MONTH(?), QUARTER(?), YEAR(?), WEEK(?, 1), DAYOFWEEK(?)) " +
                                  "ON DUPLICATE KEY UPDATE " +
                                  "Orderdate = VALUES(Orderdate), " +
                                  "day = VALUES(day), " +
                                  "month = VALUES(month), " +
                                  "quarter = VALUES(quarter), " +
                                  "year = VALUES(year), " +
                                  "week = VALUES(week), " +
                                  "day_of_week = VALUES(day_of_week)";

        // SQL for inserting into Metro_Sales_Fact
        String metroSalesFactSql = "INSERT INTO Metro_Sales_Fact (ORDER_ID, ORDER_DATE, TIME_ID, PRODUCT_ID, CUSTOMER_ID, QUANTITY, SALE) " +
                                   "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection getconnect = getConnection();
           
        		// this is used to add connect the dimension to the database 
        	 PreparedStatement product_statement = getconnect.prepareStatement(product_dim );
             PreparedStatement customer_statement = getconnect.prepareStatement(customer_dim);
             PreparedStatement time_statement = getconnect.prepareStatement(time_dim);
             PreparedStatement fact_statement = getconnect.prepareStatement(metroSalesFactSql)) {
        	
        	for (String[] array : transformedData) {
                if (array != null && array.length > 0) 
                {
                    try {
                        // extracting the rows from transformed data array 
                    	//Loading data into product dimension 
                    	product_statement.setInt(1, Integer.parseInt(array[6])); //ProductID 
                    	product_statement.setString(2, array[7]);  // Product_Name 
                        // removing the alpha numeric character form product price to remove dollar sign
                    	String product_price = array[8].replaceAll("[^\\d.]",""); 
                    	product_statement.setBigDecimal(3, new BigDecimal(product_price)); // PRODUCT_PRICE
                        product_statement.setInt(4, Integer.parseInt(array[9]));  // SUPPLIER_ID
                        product_statement.setString(5, array[10]); // SUPPLIER_NAME
                        product_statement.setInt(6, Integer.parseInt(array[11])); // STORE_ID
                        product_statement.setString(7, array[12]); // STORE_NAME
                        product_statement.executeUpdate();
                        
                        // loading data into CUSTOMER_DIMENSION
                        customer_statement.setInt(1, Integer.parseInt(array[13])); // CUSTOMER_ID
                        customer_statement.setString(2, array[14]);               // CUSTOMER_NAME
                        customer_statement.setString(3, array[15]);               // Customer_GENDER
                        customer_statement.executeUpdate();
                        
                        // correct the format of order data by reading removing AM/PM 
                        String order_date = array[1];  
                        order_date =order_date.trim();
                        if (order_date.toUpperCase().contains("PM")|| order_date.toUpperCase().contains("AM")) {
                        	order_date= order_date.replaceAll("\\s(AM|PM)$", ""); 
                        }
                        // Converting it into universal date format 
                        SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        date_format.setTimeZone(TimeZone.getTimeZone("UTC")); 

                        // create the object for 
                        Timestamp orderDate = null;
                        try {
                            long timeInMillis = date_format.parse(order_date).getTime();
                            orderDate = new Timestamp(timeInMillis);  
                        } catch (ParseException e) {
                            System.err.println("Error in parsing of Orderdate: " +order_date);
                            e.printStackTrace();
                            continue;  
                        }
                        
                        // Loading data into TIME_DIMENSION
                        time_statement.setInt(1, Integer.parseInt(array[5])); // TIME_ID
                        time_statement.setTimestamp(2, orderDate);  // TimeStamp
                        time_statement.setString(3, array[1]); // day
                        time_statement.setString(4, array[1]); // month
                        time_statement.setString(5, array[1]); // quarter
                        time_statement.setString(6, array[1]); // year
                        time_statement.setString(7, array[1]); // week
                        time_statement.setString(8, array[1]); // day_of_week
                        time_statement.executeUpdate();
                        
                        int quantity = Integer.parseInt(array[3]); 
                      // extracting the product sales price and convert it into correct format
                        String Productsale = array[8].replaceAll("[^\\d.]", ""); 
                        BigDecimal salePrice = new BigDecimal(Productsale); 
                        BigDecimal Total_Sales = salePrice.multiply(new BigDecimal(quantity)); 
                        
                        // loading data into Metro_Sales_Fact
                        fact_statement.setInt(1, Integer.parseInt(array[0]));  // ORDER_ID
                        fact_statement.setTimestamp(2, orderDate);           // ORDER_DATE
                        fact_statement.setInt(3, Integer.parseInt(array[5]));  // TIME_ID 
                        fact_statement.setInt(4, Integer.parseInt(array[2]));  // PRODUCT_ID
                        fact_statement.setInt(5, Integer.parseInt(array[4])); // CUSTOMER_ID
                        fact_statement.setInt(6, quantity);                  // QUANTITY
                        fact_statement.setBigDecimal(7,Total_Sales);          // SALE (TOTAL_SALE)
                        fact_statement.executeUpdate();
                    }
                    catch (NumberFormatException e) {
                        System.err.println("Error parsing row: " + String.join(",", array));
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("Data Loaded into Warehouse successfully!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    static class StreamChunkState {
        private final List<String[]> chunk;
        public StreamChunkState(List<String[]> chunk) {
            this.chunk = chunk;}
        public List<String[]> getChunk() {
            return chunk;
        }
    }
    static class StreamDataHandling implements Runnable {
    
    	private final String transact_filePath;
        private final int sizeof_chunk;
        private final List<String[]> streambuffer = new ArrayList<>(Stream_SIZE);
       
        // constructor 
        public StreamDataHandling(String filePath, int chunkSize) {
            this.transact_filePath= filePath;
            this.sizeof_chunk = chunkSize;
        }
        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new FileReader(transact_filePath))) {
            	String read_line;
            	// reading from csv line by line splitting by comma and then storing it in the list 
            	while ((read_line = reader.readLine()) != null) 
                {
                    String[] tuple = read_line.split(",");
                    String[] fKs = extractFks(tuple); 
                   
                    synchronized ( streambuffer) {
                    	 streambuffer.add(fKs);
                         // this code if adding the foreign keys of product and customer into the stream queue  when the line in the buffer reaches the size of stream chunk  
                    	 if ( streambuffer.size() == sizeof_chunk) 
                    	 {
                    		 appendToStreamQueue();
                         }
                    }
                    ADD_Keys_to_HashTable(fKs, tuple);
                }
                synchronized ( streambuffer ) {
                    if (! streambuffer.isEmpty()) {
                        appendToStreamQueue(); 
                    }
                }
                IS_streamComplete= true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
       // This is used to add keys to the hash table 
        private void ADD_Keys_to_HashTable(String[] foreignKeys, String[] streamData) {
            if (foreignKeys != null && foreignKeys.length == 2) 
            {
            	String product_fk = foreignKeys[0]; 
                String customer_fk = foreignKeys[1]; 
                HashPair<String,String> composite_key = new HashPair<>(product_fk,customer_fk);
                hash_table.put(composite_key,streamData); 
            
            }
        }

        // Adding data to the stream queue 
        private void appendToStreamQueue() throws InterruptedException {
            List<String[]> streamchunk;
            synchronized ( streambuffer ) {
            	streamchunk= new ArrayList<>(streambuffer);
                streambuffer.clear();
            }
            StreamChunkState state_of_chunk = new StreamChunkState(streamchunk);
            // adding to stream queue 
            streamingQueue.put(state_of_chunk); 
            System.out.println("Added chunk with size: " + streamchunk.size() + " to stream Queue.");
        }
        
       // Extract only the foreign keys 
       private String[] extractFks(String[] row) {
    	     // row[2] --> product_id row[4]--> customerID
                return new String[]{row[2], row[4]}; 
        }
    }

    private static List<List<String[]>> partitionMasterData(String filePath, int partitionSize) throws IOException {
        List<List<String[]>> partitions = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            List<String[]> currentPartition = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                currentPartition.add(line.split(","));
                if (currentPartition.size() == partitionSize) {
                    partitions.add(new ArrayList<>(currentPartition));
                    currentPartition.clear();
                }
            }
            if (!currentPartition.isEmpty()) {
                partitions.add(currentPartition);
            }
        }
        return partitions;
    }    

    static class MasterDataHandling implements Runnable {
        private final Load_Partitions  product_partiton_loader;
        private final Load_Partitions  customer_partition_loader;

        public MasterDataHandling(List<List<String[]>> productPartitions, List<List<String[]>> customerPartitions) {
            this.product_partiton_loader = new Load_Partitions (productPartitions);
            this.customer_partition_loader= new Load_Partitions (customerPartitions);
        }

        @Override
        public void run() {
            try {
                while (true) 
                {
                   // Fetched the partitions of data and loaded into the partitions of disk buffer 
                	List<String[]> productPartition = product_partiton_loader.load_nextPartition();
                    List<String[]> customerPartition = customer_partition_loader.load_nextPartition();
                    Disk_Buffer.put(productPartition);
                    Disk_Buffer.put(customerPartition);
                  
                    //Extract the partition to perform mesh join
                    List<String[]> extract_productPartition =  Disk_Buffer.take();
                    List<String[]> extract_customerPartition =  Disk_Buffer.take();
                    perform_mesh_Join(extract_productPartition,extract_customerPartition);
                    
                    // Loop end when the is completed and no more chunk is left in the queue 
                    if (IS_streamComplete && streamingQueue.isEmpty()) {
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

       private void perform_mesh_Join(List<String[]> productPartition, List<String[]> customerPartition) {
           try 
           {
               while (!streamingQueue.isEmpty()) {
                    // taking chunk of stream from the queue 
            	   StreamChunkState chunkState = streamingQueue.take(); 
                    List<String[]> fKsChunk = chunkState.getChunk();
                    // For each foreign key pair in the chunk, try to find matching records
                    Iterator<String[]> iterator = fKsChunk.iterator();
                    while (iterator.hasNext()) {
                        String[] fk = iterator.next();
                        String productId = fk[0]; // ProductID
                        String customerId = fk[1]; // CustomerID
                        HashPair<String,String> hash_key = new HashPair<>(productId, customerId);
                        // getting the value of hash table against a certain composite key 
                        String[] transaction_tuple = hash_table.get(hash_key);    
                        if (transaction_tuple != null) 
                        {
                            // NESTED LOOP JOIN 
                            for (String[] product_tuple : productPartition)
                            {
                                for (String[] customer_tuple : customerPartition) 
                                {
                                    // matching if the product rows and customer rows of hash table are equal , it will append it to the enrcihed row 	
                                	if (product_tuple [0].equals(productId) &&  customer_tuple[0].equals(customerId)) 
                                	{
                                        // adding the rows into the transformed data table if the join is successful
                                		 int row_length=transaction_tuple.length + product_tuple.length + customer_tuple.length;
                                		 String[] enriched_row = new String[row_length];
                                         System.arraycopy( transaction_tuple, 0, enriched_row , 0,  transaction_tuple.length);
                                         System.arraycopy(product_tuple, 0, enriched_row, transaction_tuple.length, product_tuple.length);
                                         System.arraycopy(customer_tuple, 0, enriched_row, transaction_tuple.length + product_tuple.length, customer_tuple.length);
                            
                                        synchronized ( transformed_data_Table) {
                                            transformed_data_Table.add(enriched_row); 
                                        }
                                    }
                                }
                            }
                            // removing key from the hash table and the iterate when the chunk is processed 
                            hash_table.remove(hash_key);
                            iterator.remove(); 
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    static class Load_Partitions {
    	private final List<List<String[]>> data_partition;
        private int currentIdx;
        public Load_Partitions (List<List<String[]>> partitions) {
            this.data_partition = partitions;
            this.currentIdx = 0;
        }
        public List<String[]> load_nextPartition() {
            List<String[]> partition = data_partition.get(currentIdx);
            // this is used to load cyclic partitions
            currentIdx = (currentIdx + 1) % data_partition.size();
            return partition;
        }
    }

    // Pair Class for Composite Key
    static class HashPair<Key, Value> {
        private final Key key1;
        private final Value key2;

        public HashPair(Key k1, Value k2) {
            this.key1 = k1;
            this.key2 = k2;
        }

        public Key get_Key1() {
            return key1;
        }

        public Value get_Key2() {
            return key2;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key1, key2);
        }
        @Override
        public boolean equals(Object obj) 
        {
            if (this == obj) 
            	{return true;}
            else if (obj == null || getClass() != obj.getClass()) 
            	{return false;}
            
            HashPair<?, ?> pair = (HashPair<?, ?>) obj;
            return Objects.equals(key1, pair.key1) && Objects.equals(key2, pair.key2);
        }
    }

}
