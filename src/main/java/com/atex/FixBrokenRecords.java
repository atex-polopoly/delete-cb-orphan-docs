package com.atex;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static java.time.temporal.ChronoField.*;


public class FixBrokenRecords extends DeleteOrphans {


    private static Logger log = Logger.getLogger("Cleanup");
    private static String file;


    protected static void execute() throws Exception {

        String filename = "fix-broken-" + new Date().getTime() + ".log";
        FileHandler fileHandler = new FileHandler(filename);
        SimpleFormatter simple = new SimpleFormatter();
        fileHandler.setFormatter(simple);
        log.addHandler(fileHandler);
        log.setUseParentHandlers(false);

        log.info("Started @ " + new Date());

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .connectTimeout(TimeUnit.SECONDS.toMillis(60L))
                .kvTimeout(TimeUnit.SECONDS.toMillis(60L))
                .viewTimeout(TimeUnit.SECONDS.toMillis(1200L))
                .maxRequestLifetime(TimeUnit.SECONDS.toMillis(1200L))
                .build();

        Cluster cluster = CouchbaseCluster.create(env, cbAddress);

        Cluster rescueCluster = null;

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
            fixData();
        }

        bucket.close();
        cluster.disconnect();

        if (rescueBucket != null) {
            rescueBucket.close();
            if (rescueCluster != null) {
                rescueCluster.disconnect();
            }
        }

        log.info("Finished @ " + new Date());

        showStatistics();

    }

    private static void fixData() throws InterruptedException, IOException {


        List<String> input = Files.readAllLines(new File(file).toPath());

        total = input.size();


        log.info("Number of Hangers to process : " + total);
        log.info("Number of Threads: " + numThreads);
        timeStarted = System.currentTimeMillis();


        for (String id : input) {
            processRow(id);

        }


    }


    protected static boolean processRow(String contentId) {


        List<JsonDocument> updates = new ArrayList<>();
        List<String> deletes = new ArrayList<>();
        processed++;


        String hangerId = null, hangerInfoId = null, versionContentId = null;
        JsonDocument hangerInfo = null;

        boolean needsFixing = false;

        if (contentId.length() > 45) {
            versionContentId = contentId;
            hangerId = getHangerIdFromContentId(contentId);
            hangerInfoId = getHangerInfoFromHangerId(hangerId);
            hangerInfo = getItem(hangerInfoId);
        } else {
            hangerInfoId = getHangerInfoIdFromContentId(contentId);
            hangerInfo = getItem(hangerInfoId);
            if (hangerInfo != null)  {
                JsonArray versions = hangerInfo.content().getArray("versions");

                JsonObject last = versions.getObject(versions.size() - 1);
                versionContentId = last.getString("version");
                hangerId = getHangerIdFromContentId(versionContentId);
            }
        }


        if (hangerInfo == null) {
            needsFixing = true;
            if (versionContentId != null) {
                JsonObject hangerObj = JsonObject.fromJson("{\n" +
                        "  \"versions\": [\n" +
                        "    {\n" +
                        "      \"version\": \"" + versionContentId + "\",\n" +
                        "      \"creationInfo\": {\n" +
                        "        \"timestamp\": " + new Date().getTime() + ",\n" +
                        "        \"principalId\": \"nobody\"\n" +
                        "      }}\n" +
                        "  ],\n" +
                        "  \"views\": {},\n" +
                        "  \"aliases\": {}\n" +
                        "}");
                hangerInfo = JsonDocument.create(hangerInfoId, hangerObj);

                if (!alreadyConverted(hangerInfoId)) {
                    accumlateTotals("Upsert new HangerInfo");
                    updates.add(hangerInfo);
                }
            } else {
                log.info ("Cannot recreate hanger for " + contentId);
                accumlateTotals("Missing hanger");
                accumlateTotals("Check : " + contentId);
            }

        } else if (fixData) {

            JsonDocument hanger = getItem(hangerId);
            if (hanger != null) {
                String type = hanger.content().getString("type");
                accumlateTotals("Doc. Type " + type);
                long lastModified = hanger.content().getObject("systemData").getLong("modificationTime");
                Instant modified = Instant.ofEpochMilli(lastModified);
                DateTimeFormatter dft = new DateTimeFormatterBuilder()
                        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                        .appendLiteral('-')
                        .appendValue(MONTH_OF_YEAR, 2)
                        .appendLiteral('-')
                        .appendValue(DAY_OF_MONTH, 2)
                        .appendLiteral('@')
                        .appendValue(HOUR_OF_DAY, 2).toFormatter();

                String d = dft.format(modified.atZone(ZoneId.of("GMT")));

                accumlateTotals("Date " + d);

                if (fixData ) {
                    needsFixing = needsFixing | checkAspects(hangerId, hanger, hangerInfo, updates, deletes);

                }

            }
        }

        if (dryRun) {
            if (needsFixing) {
                log.info("Content ID " + contentId + " was broken with " + updates.size() + " fixes");
            } else {
                log.info("Content ID " + contentId + " is OK");
            }
        } else {
            updates.forEach(doc -> {
                log.info("Upsert : " + doc);
                bucket.upsert(doc);
            });
        }

        return needsFixing;
    }


    private static boolean checkAspects(String hangerId, JsonDocument hanger,
                                        JsonDocument hangerInfo, List<JsonDocument> updates, List<String> deletes) {
        JsonObject aspects = hanger.content().getObject("aspectLocations");

        final boolean[] needsFixing = {false};

        aspects.toMap().keySet().forEach(s -> {
            needsFixing[0] = needsFixing[0] | checkAspect(hangerId, hanger, s, aspects.getString(s), hangerInfo, updates, deletes);
            }
        );


        return needsFixing[0];
    }

    private static boolean checkAspect(String hangerId, JsonDocument hanger, String type, String aspectContentId, JsonDocument hangerInfo, List<JsonDocument> updates, List<String> deletes) {

        boolean needsFixing = false;

        if (aspectContentId != null && !alreadyConverted(aspectContentId)) {
            String aspectId = getAspectIdFromContentId(aspectContentId);

            JsonDocument aspect = getItem(aspectId);
            if (aspect == null) {
                needsFixing = true;
                accumlateTotals("Missing aspect type " + type);
                log.info(hangerId + " missing aspect " + type + " : " + aspectContentId );

                JsonArray versions = hangerInfo.content().getArray("versions");
                boolean foundOldVersion = false;
                String otherAspectContentId = null;
                for (int i = versions.size()-1; i >= 0; i--) {
                    JsonObject o = versions.getObject(i);
                    String version = o.getString("version");
                    String newhangerId = getHangerIdFromContentId(version);

                    JsonDocument otherVersionhanger = getItem(newhangerId);
                    if (otherVersionhanger != null) {
                        JsonObject aspects = otherVersionhanger.content().getObject("aspectLocations");
                        if (aspects != null) {
                            otherAspectContentId = aspects.getString(type);
                            if (otherAspectContentId != null) {
                                String otherAspectId = getAspectIdFromContentId(aspects.getString(type));
                                if (otherAspectId != null && !otherAspectId.equals(aspectId)) {
                                    aspect = getItem(otherAspectId);
                                    if (aspect != null) {
                                        accumlateTotals("New Aspect found for " + type);
                                        foundOldVersion = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                JsonObject aspects = hanger.content().getObject("aspectLocations");
                if (!foundOldVersion) {
                    accumlateTotals("No replacement Aspect found for " + type);
                    aspects.removeKey(type);
                    updates.add(hanger);
                }  else {
                    aspects.put(type, otherAspectContentId);
                    updates.add(hanger);
                }
            }
            if (aspect != null && type.equals("atex.onecms.article")) {

                JsonArray images = aspect.content().getObject("data").getArray("images");
                if (images != null ) {
                    images.forEach(
                            img -> {
                                String contentId = ((String) img).replace("contentid/","");
                                if (!alreadyConverted(contentId)) {
                                    processRow(contentId);
                                }
                            }
                    );
                }

            }

        }
        return needsFixing;

    }


    public static void main(String[] args) throws Exception {
        Options options = new Options();
        HelpFormatter formatter = new HelpFormatter();
        options.addOption("cbAddress", true, "One Couchbase node address");
        options.addOption("cbBucket", true, "The bucket name");
        options.addOption("cbBucketPwd", true, "The bucket password");
        options.addOption("file", true, "A file of oncms content ID's");
        options.addOption("dryRun", false, "To just output the docs to be deleted (Optional)");
        options.addOption("fixData", false, "To also fix  data");

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

            if (cmdLine.hasOption("file")) {
                file = cmdLine.getOptionValue("file");
            } else {
                throw new Exception();
            }

            if (cmdLine.hasOption("dryRun")) {
                dryRun = true;
            }

            if (cmdLine.hasOption("fixData")) {
                fixData = true;
            }

            if (cmdLine.hasOption("batchSize")) {
                batchSize = Integer.parseInt(cmdLine.getOptionValue("batchSize"));
            }
            numThreads = 1;
            if (cmdLine.hasOption("numThreads")) {
                numThreads = Integer.parseInt(cmdLine.getOptionValue("numThreads"));
                if (numThreads > 20) {
                    numThreads = 20;
                } else if (numThreads < 1) {
                    numThreads = 8;
                }
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


}


