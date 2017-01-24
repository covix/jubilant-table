package master;

import java.io.BufferedReader;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Collator;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


public class hbaseApp {
    private static final String HTABLE_NAME = "twitterStats";
    private static final String ID = "03";
    private static final byte N_WORDS = 3;

    private static HTable table;
    private static String[] all_languages;

    private static byte[] generateKey(long timestamp) {
        System.out.println(System.currentTimeMillis());

        System.out.println(timestamp + " :key");
        return Bytes.toBytes(timestamp);
    }

    // TODO plz
    private static List<Entry<String, Long>> arrangeMap(Map<String, Long> map) {
        Set<Entry<String, Long>> set = map.entrySet();
        List<Entry<String, Long>> list = new ArrayList<>(set);

        // lexicographically order
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            Collator c = Collator.getInstance();

            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                if (c.compare(o2.getKey(), o1.getKey()) == -1)
                    return 1;
                else if (c.compare(o2.getKey(), o1.getKey()) == 1)
                    return -1;
                else
                    return 0;
            }
        });

        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });
        return list;
    }

    private static void arrangeAndPrint(Map<String, HashMap<String, Long>> intervalTopTopicLangs, String query, String[] languages, long start_timestamp, long end_timestamp, String output_folder, int N) throws IOException {
        for (String language : languages) {
            Map<String, Long> intervalTopTopic = intervalTopTopicLangs.get(language);
            // Process the results and print them
            List<Entry<String, Long>> intervalTopTopicList = arrangeMap(intervalTopTopic);
            int position = 1;
            //System.out.println("The length is : " + intervalTopTopicList.size());
            for (Map.Entry<String, Long> entry : intervalTopTopicList) {
                //System.out.println("The result for the first query is:" + "TOPIC: " + entry.getKey() +  " Position: " + position + "Count" + entry.getValue());
                writeInOutputFile(query, language, position, entry.getKey(), start_timestamp, end_timestamp, output_folder, entry.getValue().toString());
                if (position == N)
                    break;
                else
                    position++;
            }
        }
    }

    // private static void executeQuery(String query, long start_timestamp, long end_timestamp, int N, String lang, String outputFolderPath) {
    //     System.out.println("Executing the " + query);
    //     Scan scan = new Scan(generateKey(start_timestamp), generateKey(end_timestamp));
    //     scan.addFamily(Bytes.toBytes(lang));
    //     System.out.println("Get the results");
    //     ResultScanner rs;
    //     try {
    //         rs = table.getScanner(scan);
    //         Result res = rs.next();
    //         if (!query.equals("query3"))
    //             intervalTopTopic = new HashMap<String, Long>();
    //         while (res != null && !res.isEmpty()) {
    //             byte[] topic_bytes = res.getValue(Bytes.toBytes(lang), Bytes.toBytes("TOPIC"));
    //             byte[] count_bytes = res.getValue(Bytes.toBytes(lang), Bytes.toBytes("COUNTS"));
    //             String topic = Bytes.toString(topic_bytes).toString();
    //             String count = Bytes.toString(count_bytes).toString();
    //             intervalTopTopic.put(topic, (long) Integer.parseInt(count));
    //             res = rs.next();
    //         }
    //         if (!query.equals("query3"))
    //             arrangeAndPrint(intervalTopTopic, query, lang, start_timestamp, end_timestamp, outputFolderPath, N);
    //
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    // }

    private static HashMap<String, HashMap<String, Long>> bla(long start_timestamp, long end_timestamp, int N, String[] languages, String outputFolderPath) throws IOException {
        // TODO exclusive?
        Scan scan = new Scan(generateKey(start_timestamp), generateKey(end_timestamp));
        HashMap<String, HashMap<String, Long>> results = new HashMap<>(languages.length);

        for (String language : languages) {
            scan.addFamily(Bytes.toBytes(language));
            results.put(language, new HashMap<String, Long>());
        }

        ResultScanner rs = table.getScanner(scan);
        Result res = rs.next();

        System.out.println("isEmpty: " + res.isEmpty());

        while (res != null && !res.isEmpty()) {
            for (String language : languages) {
                System.out.println(language);
                for (int i = 0; i < N_WORDS; i++) {
                    byte[] word_bytes = res.getValue(Bytes.toBytes(language), Bytes.toBytes("word" + i));
                    byte[] count_bytes = res.getValue(Bytes.toBytes(language), Bytes.toBytes("freq" + i));
                    String word = Bytes.toString(word_bytes);
                    Long count = Long.parseLong(Bytes.toString(count_bytes));

                    HashMap<String, Long> map = results.get(language);
                    if (map.containsKey(word)) {
                        map.put(word, map.get(word) + count);
                    } else {
                        map.put(word, count);
                    }
                }
            }
            res = rs.next();
        }
        return results;
    }

    private static void languageQuery(String query, String start_timestamp, String end_timestamp, int N, String languages, String outputFolderPath) throws IOException {
        System.out.println("execute query");

        long sts = Long.parseLong(start_timestamp);
        long ets = Long.parseLong(end_timestamp);

        String[] langs = languages.split(",");

        HashMap<String, HashMap<String, Long>> results = bla(sts, ets, N, langs, outputFolderPath);
        arrangeAndPrint(results, query, langs, sts, ets, outputFolderPath, N);
    }

    // private static void secondQuery(String start_timestamp, String end_timestamp, int N, String languages, String outputFolderPath) throws IOException {
    //     System.out.println("execute query");
    //     // executeQuery("query1", start_timestamp, end_timestamp, N, language, outputFolderPath);
    //     long sts = Long.parseLong(start_timestamp);
    //     long ets = Long.parseLong(end_timestamp);
    //     String[] languages = {language};
    //     HashMap<String, HashMap<String, Long>> results = bla(sts, ets, N, languages, outputFolderPath);
    //     arrangeAndPrint(results, "query1", languages, sts, ets, outputFolderPath, N);
    // }

    // private static void thirdQuery(String start_timestamp, String end_timestamp, int N, String outputFolderPath) {
    //     //System.out.println("Executing the query3");
    //     intervalTopTopic = new HashMap<String, Long>();
    //     try {
    //         // TODO what if no column family?
    //         String[] query_languages = new String[table.getTableDescriptor().getColumnFamilies().length];
    //         for (int i = 0; i <= table.getTableDescriptor().getColumnFamilies().length - 1; i++) {
    //             query_languages[i] = table.getTableDescriptor().getColumnFamilies()[i].getNameAsString();
    //             executeQuery("query3", start_timestamp, end_timestamp, N, query_languages[i], outputFolderPath);
    //         }
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    //     arrangeAndPrint(intervalTopTopic, "query3", null, start_timestamp, end_timestamp, outputFolderPath, N);
    // }

    private static String[] extractLangsSource(String dataFolder) {
        File folder = new File(dataFolder);
        File[] listOfFiles = folder.listFiles();
        assert listOfFiles != null;

        String[] languages = new String[listOfFiles.length];
        for (int i = 0; i < listOfFiles.length; i++) {
            File file = listOfFiles[i];
            if (file.isFile() && file.getName().endsWith(".out")) {
                languages[i] = file.getName().split(".out")[0];
            }
        }
        return languages;
    }

    private static void getTable() throws IOException {
        // TODO uncomment
        // System.setProperty("hadoop.home.dir", "/");
        Configuration conf = HBaseConfiguration.create(); // Instantiating configuration class

        // TODO what's this?
        // conf.set("hbase.zookeeper.quorum", "node4");


        //conf.addResource(new Path("/home/masteruser1/hbase-0.98.16.1-hadoop2/conf/hbase-site.xml"));

        HBaseAdmin admin = new HBaseAdmin(conf);
        System.out.println("Table exist: " + admin.tableExists(HTABLE_NAME));
        if (!admin.tableExists(HTABLE_NAME)) {
            System.out.println("Creating table in hbase");
            // Instantiating table descriptor class
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(HTABLE_NAME));
            System.out.println("table descriptor");

            System.out.print("languages: ");
            for (String all_language : all_languages) {
                System.out.print(all_language + ", ");
            }
            System.out.println();

            // Adding column families to table descriptor
            for (String language : all_languages) {
                tableDescriptor.addFamily(new HColumnDescriptor(language));
            }
            System.out.println("added families");

            admin.createTable(tableDescriptor);

            System.out.println("created table");

            HConnection conn = HConnectionManager.createConnection(conf);
            System.out.println("cretae connection");
            table = new HTable(TableName.valueOf(HTABLE_NAME), conn);

            System.out.println("Table created: " + table.getName());
        } else {
            HConnection conn = HConnectionManager.createConnection(conf);
            table = new HTable(TableName.valueOf(HTABLE_NAME), conn);
            System.out.println("Table opened: " + table.getName());
        }
        System.out.println("[BLA} Got the Table");
    }

    private static void insertIntoTable(long timestamp, String lang, String hashtag, String counts, int topic_pos) throws IOException {
        byte[] key = generateKey(timestamp);

        Put put = new Put(key);
        put.add(Bytes.toBytes(lang), Bytes.toBytes("word" + topic_pos), Bytes.toBytes(hashtag));
        put.add(Bytes.toBytes(lang), Bytes.toBytes("freq" + topic_pos), Bytes.toBytes(counts));

        table.put(put);
    }

    private static void load(String dataFolder) throws IOException {
        System.out.println("Loading data into hbase");
        File folder = new File(dataFolder);
        File[] listOfFiles = folder.listFiles();
        assert listOfFiles != null;
        System.out.println("Number of files: " + listOfFiles.length);
        for (File file : listOfFiles) {
            System.out.println("Reading the file: " + file.getName());
            if (file.isFile() && file.getName().endsWith(".out")) {

                BufferedReader br = new BufferedReader(new FileReader(file));
                for (String line; (line = br.readLine()) != null; ) {
                    // process line by line
                    String[] fields = line.split(",");
                    //System.out.println(line);
                    long timestamp = Long.parseLong(fields[0]);
                    String lang = fields[1];
                    int pos = 2;
                    int topic_pos = 0;

                    while (pos < fields.length) {
                        insertIntoTable(timestamp, lang, fields[pos], fields[pos + 1], topic_pos);
                        pos += 2;
                        topic_pos++;
                    }

                }
                System.out.println("Data sucessfully loaded");
            }
        }
    }

    // TODO opens the file for each line
    private static void writeInOutputFile(String query, String language, int position, String word, long startTS, long endTS, String outputFolderPath, String frequency) throws IOException {
        File file = new File(outputFolderPath + "/" + ID + "_" + query + ".out");

        if (query.equals("query3"))
            language = "none";

        String content = language + "," + position + "," + word + "," + startTS + "," + endTS;

        BufferedWriter bw = null;
        bw = new BufferedWriter(new FileWriter(file, true));
        bw.append(content);
        bw.newLine();
        bw.close();
        // System.out.println("Write done");
    }

    private static void start(String[] args, int mode) throws IOException {

        if (mode == 4)
            all_languages = extractLangsSource(args[1]);

        getTable();
        switch (mode) {
            case 1:
                // System.out.println("First query");
                languageQuery("query1", args[1], args[2], Integer.parseInt(args[3]), args[4], args[5]);
                break;
            case 2:
                languageQuery("query2", args[1], args[2], Integer.parseInt(args[3]), args[4], args[5]);
                break;
            case 3:
                // thirdQuery(args[1], args[2], Integer.parseInt(args[3]), args[4]);
                throw new NotImplementedException();
                // break;
            case 4:
                load(args[1]);
                break;
        }
    }

    public static void main(String[] args) throws Exception {
        // TODO zookeper argument
        org.apache.log4j.BasicConfigurator.configure();
        if (args.length > 0) {
            int mode = 0;
            System.out.println("Started hbaseApp with mode: " + args[0]);
            mode = Integer.parseInt(args[0]);
            if (mode == 4 && args.length < 2) {
                System.out.println("To start the App with mode 4 it is required the mode and the dataFolder");
                System.exit(1);
            }
            if ((mode == 1 || mode == 2) && args.length != 6) {
                System.out.println("To start the App with mode " + mode + " it is required the mode startTS endTS N language outputFolder");
                System.exit(1);
            }
            if (mode == 3 && args.length != 5) {
                System.out.println("To start the App with mode 1 it is required the mode startTS endTS N outputFolder");
                System.exit(1);
            }

            start(args, mode);

        } else {
            System.out.println("Arguments: mode dataFolder startTS endTS N language outputFolder");
            System.exit(1);
        }

    }

}
