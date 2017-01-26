package master;

import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

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


public class hbaseApp {
    private static final String HTABLE_NAME = "twitterStats";
    private static final String ID = "03";
    private static final byte N_WORDS = 3;

    private static HTable table;
    private static String[] all_languages;

    private static byte[] generateKey(long timestamp) {
        // System.out.println(timestamp + " :key");
        return Bytes.toBytes(timestamp);
    }

    private static String[] getTopN(Map<String, Long> map, int nResult) {
        String[] words = new String[nResult];

        for (int i = 0; i < nResult; i++) {
            String word = "";
            Long count = 0L;

            for (Map.Entry<String, Long> wordEntry : map.entrySet()) {
                if (wordEntry.getValue() > count) {
                    word = wordEntry.getKey();
                    count = wordEntry.getValue();
                }

            }
            words[i] = word;
            map.remove(word);

            if (map.isEmpty()) {
                break;
            }
        }

        return words;
    }

    private static void arrangeAndPrint(Map<String, HashMap<String, Long>> intervalTopTopicLanguages, String query, String[] languages, long startTimestamp, long endTimestamp, String outputFolder, int nResult) throws IOException {
        File file = new File(outputFolder + "/" + ID + "_" + query + ".out");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));

        for (String language : languages) {
            Map<String, Long> intervalTopTopic = intervalTopTopicLanguages.get(language);

            // Process the results and print them
            // List<Entry<String, Long>> intervalTopTopicList = arrangeMap(intervalTopTopic);
            String[] intervalTopTopicList = getTopN(intervalTopTopic, nResult);
            int position = 1;
            //System.out.println("The length is : " + intervalTopTopicList.size());
            for (String entry : intervalTopTopicList) {
                //System.out.println("The result for the first query is:" + "TOPIC: " + entry.getKey() +  " Position: " + position + "Count" + entry.getValue());
                writeInOutputFile(language, position, entry, startTimestamp, endTimestamp, bw);
                if (position == nResult)
                    break;
                else
                    position++;
            }
        }

        bw.close();
    }

    private static HashMap<String, HashMap<String, Long>> languageWiseQuery(long start_timestamp, long end_timestamp, String[] languages) throws IOException {
        Scan scan = new Scan(generateKey(start_timestamp), generateKey(end_timestamp));
        HashMap<String, HashMap<String, Long>> results = new HashMap<>(languages.length);

        for (String language : languages) {
            scan.addFamily(Bytes.toBytes(language));
            results.put(language, new HashMap<String, Long>());
        }

        ResultScanner rs = table.getScanner(scan);
        Result res = rs.next();

        // System.out.println("isEmpty: " + res.isEmpty());

        while (res != null && !res.isEmpty()) {
            for (String language : languages) {
                // System.out.println(language);
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

    private static HashMap<String, Long> languageUnwiseQuery(long start_timestamp, long end_timestamp) throws IOException {
        Scan scan = new Scan(generateKey(start_timestamp), generateKey(end_timestamp));
        HashMap<String, Long> results = new HashMap<>();

        ResultScanner rs = table.getScanner(scan);
        Result res = rs.next();

        // System.out.println("isEmpty: " + res.isEmpty());

        while (res != null && !res.isEmpty()) {
            NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap = res.getNoVersionMap();

            for (byte[] languageBytes : noVersionMap.keySet()) {
                for (int i = 0; i < N_WORDS; i++) {
                    byte[] word_bytes = noVersionMap.get(languageBytes).get(Bytes.toBytes("word" + i));
                    byte[] count_bytes = noVersionMap.get(languageBytes).get(Bytes.toBytes("freq" + i));

                    String word = Bytes.toString(word_bytes);
                    Long count = Long.parseLong(Bytes.toString(count_bytes));

                    if (results.containsKey(word)) {
                        results.put(word, results.get(word) + count);
                    } else {
                        results.put(word, count);
                    }
                }
            }
            res = rs.next();
        }
        return results;
    }

    private static void languageQuery(String query, String startTimestamp, String endTimestamp, int nResult, String languages, String outputFolderPath) throws IOException {
        // System.out.println("execute query");

        long sts = Long.parseLong(startTimestamp);
        long ets = Long.parseLong(endTimestamp);

        String[] langs = languages.split(",");

        HashMap<String, HashMap<String, Long>> results = languageWiseQuery(sts, ets, langs);
        arrangeAndPrint(results, query, langs, sts, ets, outputFolderPath, nResult);
    }

    private static void thirdQuery(String startTimestamp, String endTimestamp, int nResult, String outputFolderPath) throws IOException {
        long sts = Long.parseLong(startTimestamp);
        long ets = Long.parseLong(endTimestamp);

        HashMap<String, HashMap<String, Long>> results = new HashMap<>();
        results.put("none", languageUnwiseQuery(sts, ets));

        arrangeAndPrint(results, "query3", new String[]{"none"}, sts, ets, outputFolderPath, nResult);
    }

    private static String[] extractLanguagesFromSource(String dataFolder) {
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

    private static void getTable(String zkHost) throws IOException {
        String[] zkHostSplitted = zkHost.split(":");

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkHostSplitted[0]);
        conf.set("hbase.zookeeper.property.clientPort", zkHostSplitted[1]);

        HBaseAdmin admin = new HBaseAdmin(conf);
        // System.out.println("Table exist: " + admin.tableExists(HTABLE_NAME));
        if (!admin.tableExists(HTABLE_NAME)) {
            // System.out.println("Creating table in hbase");
            // Instantiating table descriptor class
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(HTABLE_NAME));
            // System.out.println("table descriptor");

            // System.out.print("languages: ");
            for (String all_language : all_languages) {
                System.out.print(all_language + ", ");
            }
            // System.out.println();

            // Adding column families to table descriptor
            for (String language : all_languages) {
                tableDescriptor.addFamily(new HColumnDescriptor(language));
            }
            // System.out.println("added families");

            admin.createTable(tableDescriptor);

            // System.out.println("created table");

            HConnection conn = HConnectionManager.createConnection(conf);
            // System.out.println("cretae connection");
            table = new HTable(TableName.valueOf(HTABLE_NAME), conn);

            // System.out.println("Table created: " + table.getName());
        } else {
            HConnection conn = HConnectionManager.createConnection(conf);
            table = new HTable(TableName.valueOf(HTABLE_NAME), conn);
            // System.out.println("Table opened: " + table.getName());
        }
        // System.out.println("[BLA} Got the Table");
    }

    private static void insertIntoTable(long timestamp, String lang, String hashtag, String counts, int topic_pos) throws IOException {
        byte[] key = generateKey(timestamp);

        Put put = new Put(key);
        put.add(Bytes.toBytes(lang), Bytes.toBytes("word" + topic_pos), Bytes.toBytes(hashtag));
        put.add(Bytes.toBytes(lang), Bytes.toBytes("freq" + topic_pos), Bytes.toBytes(counts));

        table.put(put);
    }

    private static void load(String dataFolder) throws IOException {
        // System.out.println("Loading data into hbase");
        File folder = new File(dataFolder);
        File[] listOfFiles = folder.listFiles();
        assert listOfFiles != null;
        // System.out.println("Number of files: " + listOfFiles.length);
        for (File file : listOfFiles) {
            // System.out.println("Reading the file: " + file.getName());
            if (file.isFile() && file.getName().endsWith(".out")) {

                BufferedReader br = new BufferedReader(new FileReader(file));
                for (String line; (line = br.readLine()) != null; ) {
                    // process line by line
                    String[] fields = line.split(",");
                    //System.out.println(line);
                    long timestamp = Long.parseLong(fields[0]);
                    String lang = fields[1];
                    int pos = 2;
                    int topicPos = 0;

                    while (pos < fields.length) {
                        insertIntoTable(timestamp, lang, fields[pos], fields[pos + 1], topicPos);
                        pos += 2;
                        topicPos++;
                    }

                }
                // System.out.println("Data sucessfully loaded");
            }
        }
    }

    private static void writeInOutputFile(String language, int position, String word, long startTS, long endTS, BufferedWriter bw) throws IOException {
        String content = language + ", " + position + ", " + word + ", " + startTS + ", " + endTS;

        bw.append(content);
        bw.newLine();
        // System.out.println("Write done");
    }

    private static void start(String[] args, int mode) throws IOException {
        if (mode == 4)
            all_languages = extractLanguagesFromSource(args[2]);

        getTable(args[1]);
        switch (mode) {
            case 1:
                languageQuery("query1", args[2], args[3], Integer.parseInt(args[4]), args[5], args[6]);
                break;
            case 2:
                languageQuery("query2", args[2], args[3], Integer.parseInt(args[4]), args[5], args[6]);
                break;
            case 3:
                thirdQuery(args[2], args[3], Integer.parseInt(args[4]), args[5]);
                break;
            case 4:
                load(args[2]);
                break;
        }
    }

    public static void main(String[] args) throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        if (args.length > 0) {
            int mode = Integer.parseInt(args[0]);
            // System.out.println("Started hbaseApp with mode: " + mode);

            if (mode == 4 && args.length != 3) {
                System.out.println("To start the App with mode 4 it is required the mode and the dataFolder");
                System.exit(1);
            }
            if ((mode == 1 || mode == 2) && args.length != 7) {
                System.out.println("To start the App with mode " + mode + " it is required the mode startTS endTS N language outputFolder");
                System.exit(1);
            }
            if (mode == 3 && args.length != 6) {
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
