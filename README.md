This tool will check for **Orphan documents in Couchbase** and remove them.

**Requires** a design/view to be created and published on couchbase (if the view is in development, use the flag -devView).

**The view must emit all the Hangers**.

VIEW CODE: 

```
function(doc, meta) {
 if (meta.id.indexOf('Hanger::') == 0) {
   emit(meta.id, null);
 }
}
```

BUILD:
```
mvn clean compile assembly:single
```
USAGE:
```
java -jar target/delete-cb-orphan-docs.jar -cbAddress localhost -cbBucket cmbucket -cbBucketPwd cmpasswd -design hangers -view hangers -dryRun
```
where:

-**cbAddress** is one Couchbase node address;

-**cbBucket** is the bucket name;

-**cbBucketPwd** is the bucket password;

-**design** is the design name in which the view has been created;

-**view** is the name of the view that emits all hangers;

-**devView** (Optional flag) if the view is still in development

-**dryRun** (Optional flag) is an option to run in dry mode (no real deletions will occur)

-**batchSize** (optional) is an option to limit the deletions to a specific number

-**limit** (optional) limit the result size of the view

-**skip** (optional) skip the first n records from the view

-**tidyUp** (optional) remove un-necessary data (WorkspaceBean, DraftContent, Activities)

-**fixData** (optional) Convert legacy NoSQL to new OneCMS Content types

-**numThreads** (optional) Number of threads to use for processing, default is 8

-**startKey** (optional) Key to start from in the view

## Checking Couchbase Integrity

First grab a set of content ID's to check:

```bash

curl "http://10.135.39.31:8984/solr/onecms/select?q=atex_desk_objectType%3A%5B*%20TO%20*%5D&start=0&rows=100000000&fl=id&wt=csv" > all_solr_ids.txt

```

Where 10.135.39.31 should be changed to the SOLR Master host

Now, run the command to check couchbase (probably best to put it into a bash script file)

```bash
java -cp delete-cb-orphan-docs.jar com.atex.FixBrokenRecords -cbAddress localhost -cbBucket production -cbBucketPwd prodme2024 -apiHost http://10.135.39.27:8080 -apiUser sysadmin -apiPwd AmanDikkat2021 -file all_solr_ids.txt -dryRun -fixData
```

When it has finished, it will generate a report log file, which can be grepped for data e.g.

```bash
grep "broken with " fix-broken-1708982737256.log | awk {'print "<id>"$4"</id>"'} > delete_solr.xml
```
Now edit the file and top/tail with
```xml
<delete>
  .....rest of file...
</delete>
```

Put the following into a bash file e.g. delete_by_id.sh and change the solr host to the solr master IP / Host name

```bash
solr_host=10.135.39.31

curl "http://$solr_host:8984/solr/onecms/update?commit=true" -H "Content-type: text/xml" --data-binary "@$1"
curl "http://$solr_host:8984/solr/changelist/update?commit=true" -H "Content-type: text/xml" --data-binary "@$1"
curl "http://$solr_host:8984/solr/latest/update?commit=true" -H "Content-type: text/xml" --data-binary "@$1"

```

```bash
chmod +x delete_by_id.sh
./delete_by_id.sh delete_solr.xml
```
