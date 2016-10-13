## Start kafka cluster
I recommend using confluent platform. Refer to http://docs.confluent.io/3.0.1/ 
and start ZK, Kafka and Control Center

```
{
    "fields": [
        { "name": "join_key", "type": "string" },
        { "name": "d_desc", "type": "string" },
        { "name": "ip", "type": "string" },
        { "name": "optx_hostname", "type": "string" },
    ],
    "name": "dimension",
    "type": "record"
}
```

```
import java.net.URI
def listFiles(fileDir: String): List[URI] = {
    var files = List[URI]()
    val remoteFiles = FileSystem
        .get(sc.hadoopConfiguration)
        .listFiles(new Path(fileDir), true)
        
    while(remoteFiles.hasNext) {
        files = remoteFiles.next.getPath.toUri :: files
    }
    files
}

def readAvroFile(file: URI, conf: org.apache.hadoop.conf.Configuration): TraversableOnce[GenericRecord] = {
    val in = new FsInput(new Path(file), conf)
    DataFileReader.openReader(in, new GenericDatumReader[GenericRecord]()).iterator
}

sc.parallelize(listFiles(s"$outputDir/dimension")).flatMap { file => readAvroFile(file, sc.hadoopConfiguration) }
