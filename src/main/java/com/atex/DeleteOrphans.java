package com.atex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import rx.Observable;
import rx.functions.Func1;

public class DeleteOrphans {

  protected static final String NOSQL_IMAGE_TYPE = "com.atex.nosql.image.ImageContentDataBean";
  protected static final String ATEX_ONECMS_IMAGE = "atex.onecms.image";
  protected static final String ATEX_ONECMS_ARTICLE = "atex.onecms.article";
  protected static final String NOSQL_VIDEO_BEAN = "com.atex.nosql.video.VideoContentDataBean";
  protected static final String ATEX_DAM_VIDEO = "atex.dam.standard.Video";
  protected static final String NOSQL_ARTICLE_TYPE = "com.atex.nosql.article.ArticleBean";
  protected static final String ARTCLE_BEAN_EXTENDED = "com.atex.nosql.article.ArticleBeanExtended";
  protected static final String ATEX_REMOTE_TRACKER = "com.atex.nosql.RemoteContentTrackerBean";
  protected static final String ATEX_ONECMS = "atex.OneCMS";
  protected static final String CREATED_WITH_TEMPLATE = "createdWithTemplate";
  // Input values
  protected static String cbAddress;
  protected static String cbBucket;
  protected static String cbBucketPwd;
  protected static String design;
  protected static String view;
  protected static boolean devView = false;
  protected static boolean dryRun = false;
  protected static int batchSize = -1;

  protected static String rescueCbAddress;
  protected static String rescueCbBucket;
  protected static String rescueCbBucketPwd;

  protected static final String CONTENT_ID_PREFIX = "onecms:";
  protected static final String DELETION_PREFIX = "deletion:";
  protected static final String MUTATION_PREFIX = "mutation:";

  protected static Logger log = Logger.getLogger("Cleanup");

  protected static volatile Map<String, Long> totals = new TreeMap<>();

  protected static Bucket bucket;
  protected static String startKey;
  protected static int numThreads = 8;

  protected static volatile int processed = 0;
  protected static volatile int converted = 0;
  protected static volatile int removed = 0;

  protected static final Set<String> deletedKeys = Collections.synchronizedSet(new HashSet<>());
  protected static final Set<String> convertedKeys  = Collections.synchronizedSet(new HashSet<>());
  protected static boolean fixData = false;
  protected static boolean tidyUp = false;
  protected static int limit = -1;
  protected static int skip = -1;
  protected static volatile AtomicInteger lastPercentage = new AtomicInteger();
  protected static volatile AtomicLong lastTime = new AtomicLong();
  protected static int total = 0;
  protected static long timeStarted = 0;

  protected static Bucket rescueBucket;
  protected static boolean restore = false;
  protected static AtomicInteger restored = new AtomicInteger();

  protected static RawJsonDocument getDeletedItem(String id) {
    RawJsonDocument response = null;
    try {
      response = rescueBucket.get(id, RawJsonDocument.class);
    } catch (NoSuchElementException e) {
      log.warning("No deleted element with id: " + id);
    }
    return response;
  }

  protected static void restoreRow(String id) {
    RawJsonDocument doc = getDeletedItem(id);
    try {
      bucket.insert(doc);
      //rescueBucket.async().remove(doc).subscribe(); // Don't delete the rescue docs, it's safer!
      //log.info ("<== RESTORED: " + id);
    } catch (Exception ex) {
      log.warning ("Error inserting doc: " + ex);
      return;
    }
    restored.getAndIncrement();

    float percentage =  restored.floatValue() * 100 / total;
    if (percentage >= lastPercentage.getAndSet((int) percentage) + 1) {
      String out = String.format("%f", percentage);
      long now = System.currentTimeMillis();
      float duration = now - timeStarted;
      long last = lastTime.getAndSet(now);
      if (last != 0) {
        duration = now - last;
      }
      // Duration of last 1%
      float percentRemaining = 100 - percentage;
      long timeRemaining = (long) (duration * percentRemaining);

      long endTime = now + timeRemaining;
      showStatistics();
      log.info("=== DOCS RESTORED: " + restored + ", %age = " + out + ", ETA : " + new Date(endTime));
    }

  }

  protected static void restoreDeleted() throws InterruptedException {
    ViewQuery query = ViewQuery.from("deleted", "deleted");
    if (limit > 0) {
      query.limit(limit);
    }
    if (skip > 0) {
      query.skip(skip);
    }
    query.stale(Stale.FALSE);

    ViewResult result = rescueBucket.query(query);
    total = result.totalRows();
    log.info("Number of Docs in the deleted view : " + total);
    log.info("limit : " + limit);
    log.info("skip : " + skip);
    if (limit > 0 && total > limit) {
      total = limit;
    }
    log.info("Number of Docs to restore : " + total);
    log.info("Number of Threads: " + numThreads);
    timeStarted = System.currentTimeMillis();

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    BoundedExecutor bex = new BoundedExecutor(executor, numThreads);
    for (ViewRow row : result) {
      bex.submitTask(() -> restoreRow(row.id()));
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
  }

  protected static void sendToRescue(List<String> keys) {
    Observable
        .from(keys)
        .flatMap(new Func1<String, Observable<RawJsonDocument>>() {
          @Override
          public Observable<RawJsonDocument> call(String key) {
            return bucket.async().get(key, RawJsonDocument.class);
          }
        })
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

  protected static JsonDocument getItem(String id) {
    JsonDocument response = null;
    try {
      response = bucket.get(id);
    } catch (Exception e) {
      log.warning("Failed to get " + id + " : "
          + e.getMessage());
      e.printStackTrace();
    }
    return response;
  }

  private static boolean deleteItem(String id, List<String> keys) {
    if (!alreadyDeleted(id)) {
      if (!dryRun) {
        log.info("REMOVING: " + id);
        keys.add(id);
      } else {
        log.info("(DRY-RUN) REMOVING: " + id);
      }
      return true;
    }
    return false;
  }

  protected static boolean alreadyDeleted(String id) {
    synchronized (deletedKeys) {
      if (deletedKeys.contains(id)) return true;
      deletedKeys.add(id);
      return false;
    }
  }

  protected static boolean alreadyConverted(String id) {
    synchronized (convertedKeys) {
      if (convertedKeys.contains(id)) return true;
      convertedKeys.add(id);
      return false;
    }
  }

  protected static void removeAspects(JsonDocument doc, List<String> keys) {
    if (doc.content().containsKey("aspectLocations")) {
      JsonObject obj = doc.content().getObject("aspectLocations");
      for (String s : obj.getNames()) {
        String aspectId = obj.getString(s);
        aspectId = aspectId.replace("onecms:", "Aspect:");
        aspectId = aspectId.replace(":", "::");
        deleteItem(aspectId, keys);
      }

    }
  }

  protected static boolean removeHanger(String hangerId, JsonDocument hangerInfo, List<String> keys) {

    JsonDocument doc = getItem(hangerId);
    if (doc == null && hangerInfo == null) {
      log.info("getItem returns NULL for: " + hangerId);
      return false;
    }

    if (doc != null) {
      removeAspects(doc, keys);
      deleteItem(hangerId, keys);

    }
    // Hanger Info supplied, so also delete that and all versions
    if (hangerInfo != null) {
      if (deleteItem(hangerInfo.id(), keys)) {
        JsonArray versions = hangerInfo.content().getArray("versions");
        for (int i = 0; i < versions.size(); i++) {
          JsonObject o = versions.getObject(i);
          String version = o.getString("version");
          String newhangerId = getHangerIdFromContentId(version);

          JsonDocument hangerDoc = getItem(hangerId);
          if (hangerDoc != null) {
            removeAspects(hangerDoc, keys);
            deleteItem(newhangerId, keys);
          }
        }
        // remove aliases
        if (hangerInfo.content().getObject("aliases") != null && hangerInfo.content().getObject("aliases").getArray("externalId") != null) {
          JsonArray extIDs = hangerInfo.content().getObject("aliases").getArray("externalId");
          for (int i = 0; i < extIDs.size(); i++) {
            String extId = hangerInfo.content().getObject("aliases").getArray("externalId").getString(i);
            if (extId != null && !extId.isEmpty()) {
              String hangerAlias = "HangerAlias::externalId::" + extId;
              deleteItem(hangerAlias, keys);
            }
          }
        }
      }
    }

    return true;
  }

  protected static String getHangerInfoFromHangerId(String hangerId) {
    String hangerInfoId = hangerId.replace("Hanger::", "HangerInfo::");
    hangerInfoId = hangerInfoId.substring(0, hangerInfoId.lastIndexOf("::"));
    return hangerInfoId;
  }

  protected static void execute() throws Exception {

    String filename = "delete-orphans-" + new Date().getTime() + ".log";
    FileHandler fileHandler = new FileHandler(filename);
    SimpleFormatter simple = new SimpleFormatter();
    fileHandler.setFormatter(simple);
    log.addHandler(fileHandler);
    log.setUseParentHandlers(false);

    log.info ("Started @ " + new Date());

    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
            .connectTimeout(TimeUnit.SECONDS.toMillis(60L))
            .kvTimeout(TimeUnit.SECONDS.toMillis(60L))
            .viewTimeout(TimeUnit.SECONDS.toMillis(1200L))
            .maxRequestLifetime(TimeUnit.SECONDS.toMillis(1200L))
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

      if (restore) {
        restoreDeleted();
      } else {
        delete();
      }
    } catch (InterruptedException e) {
      log.warning("Interrupted Exception: "+e.getMessage());
    } finally {
      if (bucket != null) bucket.close();
      if (cluster != null) cluster.disconnect();

      if (rescueBucket != null) {
        rescueBucket.close();
        if (rescueCluster!=null) {
          rescueCluster.disconnect();
        }
      }
    }

    log.info ("Finished @ " + new Date());

    showStatistics();

  }

  protected static void showStatistics() {

    StringBuffer buf = new StringBuffer();
    if (!restore) {
      buf.append("==============================================================\n");
      buf.append("Number of Hangers processed       : " + processed + "\n");
      buf.append("Number of Orphan Hangers removed  : " + removed + "\n");
      buf.append("Number of Legacy Hangers converted: " + converted + "\n");

      for (String key : totals.keySet()) {
        buf.append(key).append(" : ").append(totals.get(key)).append("\n");
      }
    } else {
      buf.append("==============================================================\n");
      buf.append("Number of Docs restored       : " + restored + "\n");
    }
    buf.append("==============================================================");

    log.info(buf.toString());

  }

  protected static boolean processRow(String hangerId) {

    List<JsonDocument> updates = new ArrayList<>();
    List<String> deletes = new ArrayList<>();
    processed++;

    //log.fine ("Processing row : " + hangerId + " on thread " + Thread.currentThread().getName());
    String hangerInfoId = getHangerInfoFromHangerId(hangerId);
    JsonDocument hangerInfo = getItem(hangerInfoId);
    if (hangerInfo == null) {
      if (removeHanger(hangerId, null, deletes)) {
        removed++;
      }
    } else if (fixData || tidyUp){
      boolean needsIndexing = false;
      JsonDocument hanger = getItem(hangerId);
      if (hanger != null) {
        String type = hanger.content().getString("type");
        accumlateTotals("Doc. Type " + type);
        if (fixData) {
          if (type.equalsIgnoreCase(NOSQL_IMAGE_TYPE)) {
            needsIndexing = convertAspect(hangerId, hanger, NOSQL_IMAGE_TYPE, ATEX_ONECMS_IMAGE, "com.atex.onecms.app.dam.standard.aspects.OneImageBean", false, updates, deletes);
            converted++;
          } else if (type.equalsIgnoreCase(NOSQL_ARTICLE_TYPE)) {
            needsIndexing = convertAspect(hangerId, hanger, NOSQL_ARTICLE_TYPE, ATEX_ONECMS_ARTICLE, "com.atex.onecms.app.dam.standard.aspects.CustomArticleBean", true, updates, deletes);
            converted++;
          } else if (type.equalsIgnoreCase(ATEX_ONECMS_ARTICLE)) {
            //needsIndexing = fixAspectTimeState(hanger, ATEX_ONECMS_ARTICLE, NOSQL_ARTICLE_TYPE, updates);
          } else if (type.equalsIgnoreCase(ATEX_ONECMS_IMAGE)) {
            //needsIndexing = fixAspectTimeState(hanger, ATEX_ONECMS_IMAGE, NOSQL_IMAGE_TYPE, updates);
          }  else if (type.equalsIgnoreCase(NOSQL_VIDEO_BEAN)) {
            needsIndexing = convertAspect(hangerId, hanger, NOSQL_VIDEO_BEAN, ATEX_DAM_VIDEO, "com.atex.onecms.app.dam.standard.aspects.CustomVideoBean", false, updates, deletes);
            converted++;
          }
        }
        if (tidyUp) {
          // NOTE: removing "atex.activity.Activities" we need also to remove the
          // 'HangerAlias::externalId::onecms:6c5ec61f-44a4-4c83-9794-a8c0731e230d where onecms:6c5ec61f-44a4-4c83-9794-a8c0731e230d
          // is the id of the versioned content otherwise we leaves objects that cannot be locked anymore. Seen during SNA cleanup!
          // The same seems to apply to "com.atex.onecms.content.WorkspaceBean". Not sure about "com.atex.onecms.content.DraftContent" [max]
          if (type.equalsIgnoreCase(ATEX_REMOTE_TRACKER)
              || type.equalsIgnoreCase("com.atex.onecms.content.WorkspaceBean")
              || type.equalsIgnoreCase("com.atex.onecms.content.DraftContent")
              || type.equalsIgnoreCase("atex.activity.Activities")
              ) {
            accumlateTotals("Content type " + type + " removed");
            removeHanger(hangerId, hangerInfo, deletes);
          }
        }
        if (needsIndexing) {
          sendKafkaMutation(hangerId, hangerInfo, updates);
        }
      }
    }
    if (!updates.isEmpty()) sendUpdates(updates);
    if (!deletes.isEmpty() && rescueBucket != null) sendToRescue(deletes);
    if (!deletes.isEmpty()) sendDeletes(deletes);

    float percentage = (float) processed * 100 / total;
    if (percentage >= lastPercentage.getAndSet((int) percentage) + 1) {
        String out = String.format("%f", percentage);
        long now = System.currentTimeMillis();
        float duration = now - timeStarted;
        long last = lastTime.getAndSet(now);
        if (last != 0) {
          duration = now - last;
        }
        // Duration of last 1%
        float percentRemaining = 100 - percentage;
        long timeRemaining = (long) (duration * percentRemaining);

        long endTime = now + timeRemaining;
        showStatistics();
        log.info("=== HANGERS PROCESSED: " + processed + ", %age = " + out + ", ETA : " + new Date(endTime));
    }
    return false;
  }

  private static void fixOneCMS(String aspectContentId, List<JsonDocument> updates) {

    accumlateTotals("One CMS Aspects checked");
    String aspectId = getAspectIdFromContentId(aspectContentId);

    JsonDocument aspect = getItem(aspectId);
    if (aspect != null) {
      JsonObject data = aspect.content().getObject("data");
      boolean changed = false;
      String createdWith = data.getString(CREATED_WITH_TEMPLATE);
      if (createdWith != null) {
        if (createdWith.equalsIgnoreCase("act.template.nosql.Image")) {
          data.removeKey(CREATED_WITH_TEMPLATE);
          data.put(CREATED_WITH_TEMPLATE, "atex.onecms.image");
          changed = true;
        }

        if (createdWith.equalsIgnoreCase("act.template.nosql.Article.print")) {
          data.removeKey(CREATED_WITH_TEMPLATE);
          data.put(CREATED_WITH_TEMPLATE, "atex.onecms.article.print");
          changed = true;
        }
      }
      if (changed) {
        accumlateTotals("One CMS Aspects updated");
        updates.add(aspect);
      }
    }

  }


  protected static void sendUpdates(List<JsonDocument> items) {
    //items.forEach(doc -> System.out.println(doc.id()));
    Observable
            .from(items)
            .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
              @Override
              public Observable<JsonDocument> call(final JsonDocument docToInsert) {
                return bucket.async().replace(docToInsert).onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>() {
                  @Override
                  public Observable<? extends JsonDocument> call(Throwable throwable) {
                    log.warning ("Error processing doc " + docToInsert.id() + " : " + throwable);
                    return Observable.empty();
                  }
                });
              }
            })
            .retryWhen(RetryBuilder
                    .anyOf(BackpressureException.class)
                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
                    .max(10)
                    .build())
            .toBlocking()
            .lastOrDefault(null);
  }

  protected static void sendDeletes(List<String> keys) {
    Observable
            .from(keys)
            .flatMap((Func1<String, Observable<JsonDocument>>) key -> bucket.async().remove(key).onErrorResumeNext(
                    throwable -> {
                      log.warning ("Error removing key " + key + " : " + throwable);
                      return Observable.empty();
                    }))
            .doOnError(throwable -> log.log (Level.WARNING, "Error processing batch : " + throwable))
            .retryWhen(RetryBuilder
                    .anyOf(BackpressureException.class)
                    .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
                    .max(10)
                    .build())
            .toBlocking()
            .lastOrDefault(null);

  }

  protected static void delete() throws InterruptedException {
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
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
  }

  protected static void sendKafkaMutation(String hangerId, JsonDocument hangerInfo, List<JsonDocument> updates) {
    String message = createMutationMessage (hangerInfo);

    log.info(message);

    if (!alreadyConverted(hangerInfo.id())) updates.add (hangerInfo);

  }

  private static boolean fixAspectTimeState(JsonDocument hanger, String targetType, String sourceType, List<JsonDocument> updates) {
    JsonObject aspects = hanger.content().getObject("aspectLocations");
    // Conversion went wrong , so this only happens in very specific circumstances , i.e. hanger type is onecms.article, but the aspects still contains noSQL.
    if (aspects.containsKey(sourceType)) {
      aspects.put(targetType, aspects.getString(sourceType));
      aspects.removeKey(sourceType);
      updates.add(hanger);
      accumlateTotals("Broken Aspects Fixed");
    }

    String aspectContentId = aspects.getString(targetType);
    if (aspectContentId != null) {
      String aspectId = getAspectIdFromContentId(aspectContentId);

      JsonDocument aspect = getItem(aspectId);
      if (aspect != null) {
        boolean updated = false;
        JsonObject data = aspect.content().getObject("data");

        if (data.containsKey("timeState")) {
          JsonObject tso = data.getObject("timeState");

          if (tso.getString("_type").equalsIgnoreCase("com.atex.nosql.article.TimeState")) {
            tso.put("_type", "com.atex.onecms.app.dam.types.TimeState");
            updated=true;
            accumlateTotals("TimeState date converted");

          }
        }
        if (data.containsKey("webImage")) {
          data.removeKey("webImage");
          updated = true;
        }
        if (data.containsKey("actDraft")) {
          data.removeKey("actDraft");
          updated = true;
        }
        if (updated) {
          updates.add(aspect);
        }
      }
    }
    return false;
  }

  protected static String getAspectIdFromContentId(String aspectContentId) {
    return "Aspect::" + aspectContentId.replace("onecms:", "").replace(":", "::");
  }

  protected static String getHangerIdFromContentId(String hangerContentId) {
    return "Hanger::" + hangerContentId.replace("onecms:", "").replace(":", "::");
  }

  protected static String getHangerInfoIdFromContentId(String hangerContentId) {
    return "HangerInfo::" + hangerContentId.replace("onecms:", "").replace(":", "::");
  }
  protected static synchronized void accumlateTotals(String type) {
    long value = 0;
    if (totals.containsKey(type)) {
      value = totals.get(type).longValue();

    }
    value++;

    totals.put(type, value);

  }

  protected static boolean convertAspect(String hangerId, JsonDocument hanger,
                                       String sourceType, String targetType,
                                       String targetBean, boolean convertWireArticles,
                                       List<JsonDocument> updates, List<String> deletes) {
    boolean needsIndexing = false;
    JsonObject aspects = hanger.content().getObject("aspectLocations");
    String aspectContentId = aspects.getString(sourceType);
    if (aspectContentId!=null && !alreadyConverted(aspectContentId)) {
      String aspectId = getAspectIdFromContentId(aspectContentId);

      JsonDocument aspect = getItem(aspectId);
      if (aspect != null && aspect.content().getString("name").equalsIgnoreCase(sourceType)) {
        accumlateTotals("Converted from " + sourceType + " to " + targetType);
        aspect.content().put("name", targetType);
        JsonObject data = aspect.content().getObject("data");
        data.put("_type", targetBean);

        if (data.containsKey("creationDate")) {
          long creationDate = data.getLong("creationDate");
          data.removeKey("creationDate");

          data.put("creationdate", getDateObject(creationDate));
          accumlateTotals("Creation date converted");
        }

        if (data.containsKey("timeState")) {
          JsonObject tso = data.getObject("timeState");

          tso.put ("_type", "com.atex.onecms.app.dam.types.TimeState");
          accumlateTotals("Creation date converted");
        }

        // This is no longer in OneArticleBean
        data.removeKey("editorsPickHeadline");
        data.removeKey("webImage");
        data.removeKey("actDraft");
        data.removeKey("remoteContentId");

        String name = (String) data.get("name");
        if (convertWireArticles && name != null && name.matches(".*PRESSUK_.*")) {
          data.put ("inputTemplate", "p.DamWireArticle");
          accumlateTotals("Converted to Wire Article");
          needsIndexing = true;
        }

        if(sourceType.equalsIgnoreCase(NOSQL_VIDEO_BEAN)){
          String engagementAspectContentId = aspects.getString("engagementAspect");
          String publishingAspectContentId = aspects.getString("com.atex.gong.publish.PublishingBean");

          /**
           *  Set input template based on whether video was published / has engagementAspect
           */
          if (engagementAspectContentId != null || publishingAspectContentId != null) {
            data.put ("inputTemplate", "p.StandardVideo");

          }else{
            data.put ("inputTemplate", "p.DamVideo");
          }

          accumlateTotals("Converted to Custom Video");
          needsIndexing = true;

        }

        /* Article bean extended is not used anymore, so will migrate the data into the standard bean */
        String extendedContentId = aspects.getString(ARTCLE_BEAN_EXTENDED);
        if (extendedContentId != null) {
          String extendedAspectId = getAspectIdFromContentId(extendedContentId);
          deletes.add(extendedAspectId);
          JsonDocument extended = getItem(extendedAspectId);
          if (extended != null) {
            JsonObject extendedData = extended.content().getObject("data");

            for (String key : extendedData.getNames()) {
              if (!key.equalsIgnoreCase(("_type"))) {
                data.put(key, extendedData.get(key));
              }
            }
          } else {
            log.warning("Missing aspect " + extendedAspectId);
          }

          aspects.removeKey(ARTCLE_BEAN_EXTENDED);
          accumlateTotals("Extended article bean aspect converted and removed");
        }
        if (!dryRun) {
          log.info ("Converting Aspect " + aspectContentId + " to " + targetType);
          updates.add (aspect);
        } else {
          log.info ("TEST  "+ aspectContentId + " to " + targetType);
        }
      }

    }
    String oneCMSAspectId = aspects.getString(ATEX_ONECMS);

    if (oneCMSAspectId != null && !alreadyConverted(oneCMSAspectId)) {
      fixOneCMS(oneCMSAspectId, updates);
    }
    // Need to remove and replace aspect just in case the versions have not changed.
    if (aspects.containsKey(sourceType)) {
      aspects.removeKey(sourceType);
      aspects.put(targetType, aspectContentId);
    }
    hanger.content().put("type", targetType);

    if (!dryRun) {
      log.info ("Converting Hanger " + hangerId + " to " + targetType);
      updates.add (hanger);
    } else {
      log.info ("Test Hanger " + hangerId + " to " + targetType);
    }
    return needsIndexing;
  }



  protected static JsonObject getDateObject(long creationDate) {
    Date d = new Date (creationDate);
    String dateJson = String.format("{\"_type\": \"java.util.Date\",\"time\": %d}", d.getTime());

    return JsonObject.fromJson(dateJson);
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    HelpFormatter formatter = new HelpFormatter();
    options.addOption("cbAddress", true, "One Couchbase node address");
    options.addOption("cbBucket", true, "The bucket name");
    options.addOption("cbBucketPwd", true, "The bucket password");
    options.addOption("design", true, "The hangers design name");
    options.addOption("view", true, "The hangers design view");
    options.addOption("devView", false, "the view is in development (Optional)");
    options.addOption("dryRun", false, "To just output the docs to be deleted (Optional)");
    options.addOption("fixData", false, "To also fix & convert legacy data");
    options.addOption("tidyUp", false, "Tidy up left over content");
    options.addOption("batchSize", true, "Limit to a number of hanger deletions/conversions (Optional)");
    options.addOption("skip", true, "Start at position x in the results set (Optional)");
    options.addOption("limit", true, "Only process a certain number of hangers (Optional)");
    options.addOption("startKey", true, "Starting ID if re-starting the process after failure");
    options.addOption("numThreads", true, "Number of threads to use, default 10");

    options.addOption("rescueCbAddress", true, "One Rescue Couchbase node address");
    options.addOption("rescueCbBucket", true, "The Rescue bucket name");
    options.addOption("rescueCbBucketPwd", true, "The Rescue bucket password");
    options.addOption("restore", false, "Restore content from Rescue Bucket");

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
      if (cmdLine.hasOption("restore")) {
        restore = true;
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

      if (cmdLine.hasOption("fixData")) {
        fixData = true;
      }
      if (cmdLine.hasOption("tidyUp")) {
        tidyUp = true;
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

    } catch (Exception e) {
      e.printStackTrace();
      formatter.printHelp("DeleteOrphans", options);
      System.exit(-99);
    }

    execute();
  }

  protected static String createMutationMessage(final JsonDocument hangerInfo)
  {

    String cvid = getCvid(hangerInfo);
    if (cvid != null) {
      return MUTATION_PREFIX + cvid;
    }
    return null;
  }

  /**
   * Using "raw" json since we do not want dependencies on HangerInfo et. al.
   */
  protected static String getCvid(JsonDocument doc)
  {
    JsonObject top = doc.content();
    if (top.containsKey("versions")) {
      JsonArray versions = top.getArray("versions");
      if (versions.size() > 0) {
        JsonObject latestVersion = (JsonObject) versions.get(versions.size() - 1);
        return latestVersion.getString("version");
      }
    }
    return null;
  }

    public static class BoundedExecutor {
        private final Executor exec;
        private final Semaphore semaphore;
        public BoundedExecutor(Executor exec, int bound) {
            this.exec = exec;
            this.semaphore = new Semaphore(bound);
        }
        public void submitTask(final Runnable command)
                throws InterruptedException {
            semaphore.acquire();
            try {
                exec.execute(new Runnable() {
                    public void run() {
                        try {
                            command.run();
                        } finally {
                            semaphore.release();
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                semaphore.release();
            }
        }
    }

}
