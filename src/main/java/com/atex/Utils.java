package com.atex;

import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.util.retry.RetryBuilder;
import rx.Observable;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

public class Utils {

  private static Bucket bucket;
  private static boolean dryRun = false;

  public static void init(Bucket bucket, boolean dryRun) {
    Utils.bucket = bucket;
    Utils.dryRun = dryRun;
  }

  public static JsonDocument getItem(String id) {
    JsonDocument response = null;
    try {
      response = bucket.get(id);
    } catch (NoSuchElementException e) {
      System.out.println("ERROR: No element with message: "
          + e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      System.out.println("ERROR: getItem exception: " + e);
    }
    return response;
  }

  public static Observable<JsonDocument> getItemEx(String id) {
    return bucket.async().get(id, JsonDocument.class)
        .retryWhen(RetryBuilder.anyOf(TemporaryFailureException.class)
            .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
            .max(5)
            .build()
        )
        .first()
        .onErrorResumeNext(throwable -> Observable.just(null));
  }

  public static void deleteItem(String id) {
    try {
      if (!dryRun) {
        System.out.println("REMOVING: " + id);
        bucket.remove(id, PersistTo.MASTER);
      } else {
        System.out.println("(DRY-RUN) REMOVING: " + id);
      }
    } catch (DocumentDoesNotExistException e) {
      System.out.println("ERROR: DocumentDoesNotExistException: " + id);
    } catch (Exception e) {
      System.out.println("ERROR: deleteItem exception: " + e);
    }
  }

  public static Observable<JsonDocument> deleteItemEx(String id) {
    if (dryRun) {
      System.out.println("(DRY-RUN) REMOVING: " + id);
      return Observable.empty();
    }
    System.out.println("REMOVING: " + id);
    return bucket.async().remove(id)
        .retryWhen(RetryBuilder.anyOf(TemporaryFailureException.class)
            .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
            .max(5)
            .build()
        )
        .onErrorResumeNext(throwable -> {
          if (throwable instanceof DocumentDoesNotExistException) {
            System.out.println("ERROR: DocumentDoesNotExistException: " + id);
          } else {
            System.out.println("ERROR: deleteItem exception: " + throwable);
          }
          return Observable.empty();
        });
  }

  private static void removeAspects(JsonDocument doc) {
    if (doc.content().containsKey("aspectLocations")) {
      JsonObject obj = doc.content().getObject("aspectLocations");
      for (String s : obj.getNames()) {
        String aspectId = obj.getString(s);
        aspectId = aspectId.replace("onecms:", "Aspect:");
        aspectId = aspectId.replace(":", "::");
        deleteItem(aspectId);
      }

    }
  }

  private static void removeAspectsEx(JsonDocument doc) {
    if (doc.content().containsKey("aspectLocations")) {
      JsonObject obj = doc.content().getObject("aspectLocations");
      for (String s : obj.getNames()) {
        String aspectId = obj.getString(s);
        aspectId = aspectId.replace("onecms:", "Aspect:");
        aspectId = aspectId.replace(":", "::");
        deleteItemEx(aspectId);
      }
    }
  }

  static boolean removeHanger(String hangerId) {
    JsonDocument doc = getItem(hangerId);
    if (doc != null) {
      removeAspects(doc);
      deleteItem(hangerId);
      return true;
    }
    return false;
  }

  static Observable<JsonDocument> removeH(String hangerId) {
    return bucket.async().get(hangerId, JsonDocument.class)
        .retryWhen(RetryBuilder.anyOf(TemporaryFailureException.class)
            .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
            .max(5)
            .build()
        )
        .first()
        .onErrorResumeNext(throwable -> Observable.just(null))
        .map(doc -> {
          if (doc != null) {
            removeAspectsEx(doc);
            deleteItem(hangerId);
          }
          return doc;
        });

  }

  static boolean removeHangerInfo(String hangerInfoId) {
    JsonDocument doc = getItem(hangerInfoId);
    if (doc != null) {
      JsonArray versions = doc.content().getArray("versions");
      for (Object version : versions) {
        JsonObject obj = (JsonObject) version;
        String _version = obj.getString("version");
        _version = _version.replace("onecms:", "Hanger:");
        _version = _version.replace(":", "::");
        removeHanger(_version);
      }
      removeAspects(doc);
      deleteItem(hangerInfoId);
      return true;
    }
    return false;
  }

  static String getHangerInfoFromHangerId(String hangerId) {
    String hangerInfoId = hangerId.replace("Hanger::", "HangerInfo::");
    hangerInfoId = hangerInfoId.substring(0, hangerInfoId.lastIndexOf("::"));
    return hangerInfoId;
  }

}
