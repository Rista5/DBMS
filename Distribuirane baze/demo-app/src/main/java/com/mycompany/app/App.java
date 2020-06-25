package com.mycompany.app;

import java.time.Duration;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("Expecting one parameter as argument");
            return;
        }
        int value;
        try {
            value = Integer.parseInt(args[0]);
        } catch (NumberFormatException ex) {
            System.out.println(ex.getStackTrace());
            return;
        }
        Cluster cluster = Cluster.connect("localhost", "Administrator", "Administrator");
        longRunningTransaction(cluster, value);
    }

    private static void longRunningTransaction(Cluster cluster, int value) {
        System.out.println("Starting long running transaction.");
        Bucket bucket = cluster.bucket("beer-sample");
        final Collection collection = bucket.defaultCollection();

        String query = "select meta(`beer-sample`).id " + "from `beer-sample`" + "where type='beer'"
                + "and brewery_id='21st_amendment_brewery_cafe' " + "and abv=5.5";

        TransactionConfig config = TransactionConfigBuilder.create()
                .durabilityLevel(TransactionDurabilityLevel.MAJORITY)
                .expirationTime(Duration.ofSeconds(100))
                .build();
        Transactions transactions = Transactions.create(cluster, config);
        
        try {
            transactions.run((ctx) -> {
                QueryResult result = cluster.query(query);

                for (JsonObject docId : result.rowsAs(JsonObject.class)) {
                    if (!docId.containsKey("id"))
                        continue;
                    String id = docId.getString("id");
                    TransactionGetResult res = ctx.get(collection, id);
                    JsonObject doc = res.contentAs(JsonObject.class);
                    doc.put("customer_rating", value);
                    ctx.replace(res, doc);
                }
                System.out.println("Update finished, waiting for commit!");
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                ctx.commit();
                System.out.println("Success");
            });
        } catch (TransactionFailed e) {
            System.err.println("Transaction " + e.result().transactionId() + " failed");
        
            for (LogDefer err : e.result().log().logs()) {
                System.err.println(err.toString());
            }
        }
    }

    private static void transactionTest(Cluster cluster) {
        Bucket bucket = cluster.bucket("travel-sample");
        final Collection collection = bucket.defaultCollection();

        TransactionConfig config = TransactionConfigBuilder.create()
                .durabilityLevel(TransactionDurabilityLevel.MAJORITY).build();
        Transactions transactions = Transactions.create(cluster, config);
        try {
            transactions.run((ctx) -> {
                String docId = "aDocument";
                ctx.insert(collection, docId, JsonObject.create());
                ctx.commit();
                System.out.println("Success");
            });
        } catch (TransactionFailed e) {
            System.err.println("Transaction " + e.result().transactionId() + " failed");

            for (LogDefer err : e.result().log().logs()) {
                System.err.println(err.toString());
            }
        }
    }

}
