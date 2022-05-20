package com.atex;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import org.apache.commons.cli.*;
import rx.Observable;
import rx.functions.Func1;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class ChangeAspectBeanType {

  public static final String BEAN_SOURCE_TYPE = "com.atex.onecms.app.dam.standard.aspects.OneImageBean";
  public static final String BEAN_DEST_TYPE = "com.atex.onecms.app.dam.standard.aspects.CustomImageBean";
  public static final String ATEX_ONECMS_IMAGE = "atex.onecms.image";
  // Input values
  private static String cbAddress;
  private static String cbBucket;
  private static String cbBucketPwd;
  private static String design;
  private static String view;
  private static boolean devView = false;
  private static boolean dryRun = false;
  private static int batchSize = -1;

  private static int maxConverted = 0;

  private static Logger log = Logger.getLogger("Cleanup");

  private static volatile Map<String, Long> totals = new TreeMap<>();

  private static Bucket bucket;
  private static String startKey;
  private static int numThreads = 8;

  private static volatile int processed = 0;
  private static volatile int converted = 0;
  private static volatile int removed = 0;

  private static final Set<String> convertedKeys  = Collections.synchronizedSet(new HashSet<>());
  private static int limit = -1;
  private static int skip = -1;
  private static volatile AtomicInteger lastPercentage = new AtomicInteger();
  private static volatile AtomicLong lastTime = new AtomicLong();
  private static int total = 0;
  private static long timeStarted = 0;

  private static boolean restore = false;
  private static String rescueCbAddress;
  private static String rescueCbBucket;
  private static String rescueCbBucketPwd;
  private static Bucket rescueBucket;

  private static HashMap<String,String> onecmsAspects = new HashMap<>();


  private static AtomicInteger restored = new AtomicInteger();

  private static JsonDocument getItem(String id) {
    JsonDocument response = null;
    try {
      response = bucket.get(id);
    } catch (NoSuchElementException e) {
      log.warning("No element with message: "
          + e.getMessage());
      e.printStackTrace();
    }
    return response;
  }

  private static boolean alreadyConverted(String id) {
    synchronized (convertedKeys) {
      if (convertedKeys.contains(id)) return true;
      convertedKeys.add(id);
      return false;
    }
  }

  private static void execute() throws Exception {

    String filename = "change-aspect-bean-type-" + new Date().getTime() + ".log";
    FileHandler fileHandler = new FileHandler(filename);
    SimpleFormatter simple = new SimpleFormatter();
    fileHandler.setFormatter(simple);
    log.addHandler(fileHandler);
    log.setUseParentHandlers(false);

    log.info ("Started @ " + new Date());
    log.info("Couchbase node: "+cbAddress);

    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
            .connectTimeout(TimeUnit.SECONDS.toMillis(60L))
            .kvTimeout(TimeUnit.SECONDS.toMillis(60L))
            .viewTimeout(TimeUnit.SECONDS.toMillis(1200L))
            .maxRequestLifetime(TimeUnit.SECONDS.toMillis(1200L))
            .autoreleaseAfter(5000)
            .build();

    Cluster cluster = null;
    Cluster rescueCluster = null;

    try {
      cluster = CouchbaseCluster.create(env, cbAddress);
      try {
        bucket = cluster.openBucket(cbBucket, cbBucketPwd);
      } catch (Exception e) {
        cluster.authenticate("cmuser", cbBucketPwd);
        bucket = cluster.openBucket(cbBucket);
      }

      if (rescueCbBucket != null && !rescueCbBucket.isEmpty()) {
        rescueCluster = CouchbaseCluster.create(env, rescueCbAddress);
        try {
          log.info("rescueCbBucket: " + rescueCbBucket);
          log.info("rescueCbBucketPwd: " + rescueCbBucketPwd);
          rescueBucket = rescueCluster.openBucket(rescueCbBucket, rescueCbBucketPwd);
        } catch (Exception e) {
          log.info("Exception: " + e);
          rescueCluster.authenticate("cmuser", rescueCbBucketPwd);
          rescueBucket = rescueCluster.openBucket(rescueCbBucket);
        }
      }
      process();

    } catch (InterruptedException e) {
      log.warning("Process Interrupted: "+e.getMessage());
    } finally {
      if (bucket != null) bucket.close();
      if (cluster != null) cluster.disconnect();
    }

    log.info ("Finished @ " + new Date());

    showStatistics();

  }

  private static void process() throws InterruptedException {
    ViewQuery query;
    if (devView) {
      query = ViewQuery.from(design, view).development();
    } else {
      query = ViewQuery.from(design, view);
    }
    if (startKey != null) {
      query = query.startKey(startKey);
    }
    if (limit > 0) {
      query.limit(limit);
    }
    if (skip > 0) {
      query.skip(skip);
    }
    //query.stale(Stale.FALSE);

    ViewResult result = bucket.query(query);
    total = result.totalRows();
    log.info("Number of Hangers in the view : " + total);
    log.info("limit : " + limit);
    log.info("skip : " + skip);
    if (limit > 0 && total > limit) {
      total = limit;
    }
    log.info("Number of Hangers to process : " + total);
    log.info("Number of Threads: " + numThreads);
    timeStarted = System.currentTimeMillis();

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    for (ViewRow row : result) {

      executor.submit(() -> processRow(row.id()));

      // Not ideal as we have multiple threads running, but it should help jump out early when done
      if (batchSize > 0 && (removed + converted) >= batchSize) {
        break;
      }
      if (maxConverted > 0 && converted >= maxConverted) {
        break;
      }
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

    log.info("Found "+onecmsAspects.size()+" hangers");
    for (String hangerId : onecmsAspects.keySet()) {
      String aspectId = onecmsAspects.get(hangerId);
      JsonDocument aspect = getItem(aspectId);
      JsonObject data = aspect.content().getObject("data");
      if (data != null) data.put("_type", BEAN_DEST_TYPE);
      List<JsonDocument> updates = new ArrayList<>();
      updates.add(aspect);
      log.info("hangerId = "+hangerId);
      log.info("aspectId = "+aspectId);
      if (!dryRun) sendUpdates(updates);
      if (rescueBucket != null) sendToRescue(new ArrayList<>(onecmsAspects.values()));
    }

    showStatistics();
  }

  private static void showStatistics() {

    StringBuffer buf = new StringBuffer();
    buf.append("==============================================================\n");
    buf.append("Number of Hangers processed       : " + processed + "\n");
    buf.append("Number of Hangers converted: " + converted + "\n");

    for (String key : totals.keySet()) {
      buf.append(key).append(" : ").append(totals.get(key)).append("\n");
    }
    buf.append("==============================================================");

    log.info(buf.toString());

  }

  private static boolean processRow(String itemId) {

    processed++;

    if (maxConverted > 0 && converted >= maxConverted) {
      return false;
    }

    if (itemId.startsWith("Hanger::")) {
      JsonDocument hanger = getItem(itemId);
      if (hanger != null && hanger.content() != null) {
        JsonObject aspects = hanger.content().getObject("aspectLocations");
        String aspectContentId = aspects.getString(ATEX_ONECMS_IMAGE);
        if (aspectContentId!=null && !alreadyConverted(aspectContentId)) {
          String aspectId = getAspectIdFromContentId(aspectContentId);

          JsonDocument aspect = getItem(aspectId);
          if (aspect != null && aspect.content().getString("name").equalsIgnoreCase(ATEX_ONECMS_IMAGE)) {
            JsonObject data = aspect.content().getObject("data");
            if (data != null && data.getString("_type").equals(BEAN_SOURCE_TYPE)) {

              accumlateTotals("Doc. Type " + aspect.content().getObject("data").getString("_type"));
              onecmsAspects.put(itemId, aspectId);
              converted++;
            }
          }
        }
      }
    }

    return false;
  }

  private static String getAspectIdFromContentId(String aspectContentId) {
    return "Aspect::" + aspectContentId.replace("onecms:", "").replace(":", "::");
  }

  private static void sendUpdates(List<JsonDocument> items) {
    //items.forEach(doc -> System.out.println(doc.id()));
    Observable
            .from(items)
            .flatMap((Func1<JsonDocument, Observable<JsonDocument>>) docToInsert -> bucket.async().replace(docToInsert).onErrorResumeNext(throwable -> {
              log.warning ("Error processing doc " + docToInsert.id() + " : " + throwable);
              return Observable.empty();
            }))
            .retryWhen(RetryBuilder
                    .anyOf(BackpressureException.class)
                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
                    .max(10)
                    .build())
            .toBlocking()
            .lastOrDefault(null);
  }

  private static void sendToRescue(List<String> keys) {
    Observable
        .from(keys)
        .flatMap((Func1<String, Observable<RawJsonDocument>>) key -> bucket.async().get(key, RawJsonDocument.class))
        .retryWhen(RetryBuilder
            .anyOf(BackpressureException.class)
            .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
            .max(10)
            .build())
        .toBlocking()
        .subscribe(jsonDocument ->  {
          try {
            rescueBucket.insert(jsonDocument);
          } catch (Exception ex) {
            log.warning ("Error inserting doc: " + ex);
          }
        });

  }



  private static synchronized void accumlateTotals(String type) {
    long value = 0;
    if (totals.containsKey(type)) {
      value = totals.get(type).longValue();

    }
    value++;

    totals.put(type, value);

  }



  public static void main(String[] args) throws Exception {
    Options options = new Options();
    HelpFormatter formatter = new HelpFormatter();
    options.addOption("cbAddress", true, "One Couchbase node address");
    options.addOption("cbBucket", true, "The bucket name");
    options.addOption("cbBucketPwd", true, "The bucket password");
    options.addOption("design", true, "The view design name");
    options.addOption("view", true, "The view's design view");
    options.addOption("devView", false, "the view is in development (Optional)");
    options.addOption("dryRun", false, "To just output the docs to be deleted (Optional)");
    options.addOption("batchSize", true, "Limit to a number of hanger deletions/conversions (Optional)");
    options.addOption("skip", true, "Start at position x in the results set (Optional)");
    options.addOption("limit", true, "Only process a certain number of aspects (Optional)");
    options.addOption("startKey", true, "Starting ID if re-starting the process after failure");
    options.addOption("numThreads", true, "Number of threads to use, default 10");

    options.addOption("rescueCbAddress", true, "One Rescue Couchbase node address");
    options.addOption("rescueCbBucket", true, "The Rescue bucket name");
    options.addOption("rescueCbBucketPwd", true, "The Rescue bucket password");
    options.addOption("restore", false, "Restore content from Rescue Bucket");
    options.addOption("maxConverted", true, "Max hangers to convert");

    try {
      CommandLineParser parser = new DefaultParser();
      CommandLine cmdLine = parser.parse(options, args);
      if (cmdLine.hasOption("cbAddress")) {
        cbAddress = cmdLine.getOptionValue("cbAddress");
      } else {
        throw new Exception();
      }
      if (cmdLine.hasOption("cbBucket")) {
        cbBucket = cmdLine.getOptionValue("cbBucket");
      } else {
        throw new Exception();
      }
      if (cmdLine.hasOption("cbBucketPwd")) {
        cbBucketPwd = cmdLine.getOptionValue("cbBucketPwd");
      } else {
        throw new Exception();
      }
      if (cmdLine.hasOption("design")) {
        design = cmdLine.getOptionValue("design");
      } else {
        throw new Exception();
      }
      if (cmdLine.hasOption("view")) {
        view = cmdLine.getOptionValue("view");
      } else {
        throw new Exception();
      }
      if (cmdLine.hasOption("devView")) {
        devView = true;
      }
      if (cmdLine.hasOption("dryRun")) {
        dryRun = true;
      }
      if (cmdLine.hasOption("batchSize")) {
        batchSize = Integer.parseInt(cmdLine.getOptionValue("batchSize"));
      }
      if (cmdLine.hasOption("numThreads")) {
        numThreads = Integer.parseInt(cmdLine.getOptionValue("numThreads"));
        if (numThreads > 20) {
          numThreads = 20;
        } else if (numThreads < 1) {
          numThreads = 8;
        }
      }

      if (cmdLine.hasOption("startKey")) {
        startKey = cmdLine.getOptionValue("startKey");
      }

      if (cmdLine.hasOption("skip")) {
        skip = Integer.parseInt (cmdLine.getOptionValue("skip"));
      }

      if (cmdLine.hasOption("limit")) {
        limit = Integer.parseInt (cmdLine.getOptionValue("limit"));
      }

      if (cmdLine.hasOption("restore")) {
        restore = true;
      }

      if (cmdLine.hasOption("rescueCbAddress")) {
        rescueCbAddress = cmdLine.getOptionValue("rescueCbAddress");
        if (!rescueCbAddress.isEmpty() && cmdLine.hasOption("rescueCbBucket")) {
          rescueCbBucket = cmdLine.getOptionValue("rescueCbBucket");
          if (!rescueCbBucket.isEmpty() && cmdLine.hasOption("rescueCbBucketPwd")) {
            rescueCbBucketPwd = cmdLine.getOptionValue("rescueCbBucketPwd");
          } else {
            throw new Exception();
          }
        } else {
          throw new Exception();
        }
      }

      if (cmdLine.hasOption("maxConverted")) {
        maxConverted = Integer.parseInt(cmdLine.getOptionValue("maxConverted"));
      } else {
        throw new Exception();
      }


    } catch (Exception e) {
      e.printStackTrace();
      formatter.printHelp("ChangeAspectType", options);
      System.exit(-99);
    }

    execute();
  }

}
