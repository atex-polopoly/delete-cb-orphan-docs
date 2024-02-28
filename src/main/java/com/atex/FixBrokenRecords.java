package com.atex;

import com.couchbase.client.core.tracing.ThresholdLogReporter;
import com.couchbase.client.core.tracing.ThresholdLogTracer;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.opentracing.Tracer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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

    private static String apiHost;
    private static String apiUser;

    private static String apiPwd;

    private static String authToken;

    private static JsonParser jsonParser = new JsonParser();

    private static DateTimeFormatter dft = new DateTimeFormatterBuilder()
        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 2)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 2)
        .appendLiteral('@')
        .appendValue(HOUR_OF_DAY, 2).toFormatter();
    private static boolean fixEmbeddedImages = false;


    protected static void execute() throws Exception {

        String filename = "fix-broken-" + new Date().getTime() + ".log";
        FileHandler fileHandler = new FileHandler(filename);
        SimpleFormatter simple = new SimpleFormatter();
        fileHandler.setFormatter(simple);
        log.addHandler(fileHandler);
        log.setUseParentHandlers(false);
        System.setProperty("com.couchbase.sentRequestQueueLimit", "20000");

        log.info("Started @ " + new Date());

        Tracer tracer = ThresholdLogTracer.create(ThresholdLogReporter.builder()
                                                                      .kvThreshold(5, TimeUnit.SECONDS) // 1 micros
                                                                      .logInterval(1, TimeUnit.MINUTES) // log every second
                                                                      .sampleSize(10)
                                                                      .pretty(true) // pretty print the json output in the logs
                                                                      .build());

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .connectTimeout(TimeUnit.SECONDS.toMillis(60L))
                .kvTimeout(TimeUnit.SECONDS.toMillis(10))

                //.viewTimeout(TimeUnit.SECONDS.toMillis(1200L))
                //.maxRequestLifetime(TimeUnit.SECONDS.toMillis(10))
                //.keepAliveTimeout(TimeUnit.SECONDS.toMillis(5))
                .ioPoolSize(numThreads)
                .tracer(tracer)
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

        if (apiHost != null) {
            HttpURLConnection urlConnection = null;
            URL contentApi = new URL(apiHost + "/onecms/security/token");
            try {
                urlConnection = (HttpURLConnection) contentApi.openConnection();

                String postData = String.format("{\"username\":\"%s\",\"password\":\"%s\"}", apiUser, apiPwd);
                urlConnection.setRequestMethod("POST");
                urlConnection.setRequestProperty("Content-Type", "application/json");
                urlConnection.setRequestProperty("Content-Length", "" + postData.getBytes().length);
                urlConnection.setUseCaches(false);
                urlConnection.setDoInput(true);
                urlConnection.setDoOutput(true);


                try(OutputStream os = urlConnection.getOutputStream()) {
                    byte[] input = postData.getBytes("utf-8");
                    os.write(input, 0, input.length);
                }

                urlConnection.connect();
                int status = urlConnection.getResponseCode();

                if (status == 200) {
                    String result = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()))
                        .lines().collect(Collectors.joining("\n"));

                    final JsonElement parse = jsonParser.parse(result);
                    authToken = parse.getAsJsonObject().get("token").getAsString();
                } else {
                    String error = "Status : " + status + " - " + new BufferedReader(new InputStreamReader(urlConnection.getErrorStream()))
                        .lines().collect(Collectors.joining("\n"));
                    throw new RuntimeException("Failed to authenticate : " + error + " : Posting " + postData + " to " + contentApi);
                }
            } finally {
                if (urlConnection != null) {
                    urlConnection.disconnect();
                }
            }

        }
        BufferedReader reader = new BufferedReader(new FileReader(file));
        while (reader.readLine() != null) total++;
        reader.close();

        reader = new BufferedReader(new FileReader(file));

        log.info("Number of Threads: " + numThreads);
        timeStarted = System.currentTimeMillis();

        List<String> input = getBatch(reader, batchSize);
        boolean done = false;

        while (input.size() > 0 && !done) {

            BoundedExecutor executor = new BoundedExecutor(numThreads);

            for (String id : input) {

                executor.submitButBlockIfFull(() -> processRow(id));

            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            if (!done) {
                input = getBatch(reader, 50000);
                convertedKeys.clear();
                deletedKeys.clear();
            }
        }
        reader.close();



    }

    private static List<String> getBatch(final BufferedReader reader,
                                         final int batchSize)
        throws IOException
    {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            String line = reader.readLine();
            if (line != null) {
                result.add(line);
            } else {
                return result;
            }
        }
        return result;
    }

    public static class BoundedExecutor extends ThreadPoolExecutor
    {

        private final Semaphore semaphore;

        public BoundedExecutor(int bound) {
            super(bound, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
            semaphore = new Semaphore(bound);
        }

        /**Submits task to execution pool, but blocks while number of running threads
         * has reached the bound limit
         */
        public <T> Future<T> submitButBlockIfFull(final Callable<T> task) throws InterruptedException{

            semaphore.acquire();
            return submit(task);
        }


        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);

            semaphore.release();
        }
    }


    protected static boolean processRow(String contentId) {


        List<JsonDocument> updates = new ArrayList<>();
        List<String> deletes = new ArrayList<>();

        if ((++processed % 1000) == 0) {
            float percent = (float) (processed * 100.0 / total);
            String msg = String.format("Processed %d rows of %d, %.2f", processed, total, percent);
            log.info(msg);
            System.out.println(new Date() + ":" + msg);
        }


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
            } else {
                accumlateTotals("No hanger info found for content");
                log.info ("No hangerInfo for " + hangerInfoId);
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
                //accumlateTotals("Check : " + contentId);
            }

        } else if (fixData) {

            JsonDocument hanger = getItem(hangerId);
            if (hanger != null) {
                String type = hanger.content().getString("type");
                accumlateTotals("Doc. Type " + type);
                //long lastModified = hanger.content().getObject("systemData").getLong("modificationTime");
                //Instant modified = Instant.ofEpochMilli(lastModified);
                //String d = dft.format(modified.atZone(ZoneId.of("GMT")));
                //accumlateTotals("Date " + d);

                if (fixData ) {
                    needsFixing = needsFixing | checkAspects(hangerId, hanger, hangerInfo, updates, deletes);
                }

            }
        }

        if (dryRun) {
            if (needsFixing) {
                log.info("Content ID " + contentId + " was broken with " + updates.size() + " fixes");
                if (apiHost != null) {
                    try {
                        URL contentApi = new URL(apiHost + "/onecms/content/contentid/" + contentId);
                        HttpURLConnection urlConnection = null;
                        try {
                            urlConnection = (HttpURLConnection) contentApi.openConnection();
                            urlConnection.setRequestProperty("X-Auth-Token", authToken);
                            urlConnection.setRequestMethod("GET");
                            urlConnection.setRequestProperty("Content-Type", "application/json");
                            urlConnection.setUseCaches(false);
                            urlConnection.setDoInput(true);
                            urlConnection.setDoOutput(true);
                            urlConnection.connect();
                            int status = urlConnection.getResponseCode();
                            if (status == 200) {
                                log.info("Content ID " + contentId + " is not broken in API");
                            } else {
                                log.info("Content ID " + contentId + " is broken in API");
                            }
                        } finally {
                            if (urlConnection != null) {
                                urlConnection.disconnect();
                            }
                        }
                    } catch (Exception e) {
                        log.info("Could not get content API");
                    }
                }
                accumlateTotals("Broken Content");
            } else {
                //log.info("Content ID " + contentId + " is OK");
                accumlateTotals("OK Content");
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
            if (aspect != null && type.equals("atex.onecms.article") && fixEmbeddedImages ) {

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

        batchSize = 50000;
        numThreads = 10;

        options.addOption("cbAddress", true, "One Couchbase node address");
        options.addOption("cbBucket", true, "The bucket name");
        options.addOption("cbBucketPwd", true, "The bucket password");
        options.addOption("file", true, "A file of oncms content ID's");
        options.addOption("dryRun", false, "To just output the docs to be deleted (Optional)");
        options.addOption("fixData", false, "To also fix data");

        options.addOption("numThreads", true, "Number of threads to use, default " + numThreads);
        options.addOption("batchSize", true, "Size of batch, default " + batchSize);

        options.addOption("rescueCbAddress", true, "One Rescue Couchbase node address");
        options.addOption("rescueCbBucket", true, "The Rescue bucket name");
        options.addOption("rescueCbBucketPwd", true, "The Rescue bucket password");
        options.addOption("restore", false, "Restore content from Rescue Bucket");
        options.addOption("apiHost", true, "API Host for cross check on content");
        options.addOption("apiUser", true, "API User for cross check on content");
        options.addOption("apiPwd", true, "API Password for cross check on content");

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

            if (cmdLine.hasOption("numThreads")) {
                numThreads = Integer.parseInt(cmdLine.getOptionValue("numThreads"));
            }

            apiHost = cmdLine.getOptionValue("apiHost");
            apiUser  = cmdLine.getOptionValue("apiUser");
            apiPwd = cmdLine.getOptionValue("apiPwd");

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


