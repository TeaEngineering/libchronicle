
### Command line examples - Java

InputMain.java and OutputMain.java are provided from:
https://github.com/OpenHFT/Chronicle-Queue-Sample/tree/master/simple-input/src/main/java/net/openhft/chronicle/queue/simple/input

To run an example appender:

    (mac)    $ brew install maven
    (ubuntu) $ sudo apt-get install maven

Then

    $ mvn dependency:copy-dependencies
    $ mvn package
    $ java -cp target/java-1.0-SNAPSHOT.jar:target/dependency/chronicle-queue-5.20.102.jar mains.InputMain cqv5 < sample.input
    type something
    [81346680586240] one
    [81346680586241] two
    [81346680586242] three
    Input stream finished
    $

InputMain will create a directory called 'q1' in the current directory containing a queuefile (`*.cq4`) and a `metadata.cq4t` file. The file will contain three messages in the current cycle.

For the tailer:

    $ java -cp target/java-1.0-SNAPSHOT.jar:target/dependency/chronicle-queue-5.20.102.jar mains.OutputMain q1 2>/dev/null
    [81346680586240] one
    [81346680586241] two
    [81346680586242] three
    $

### Interoperability

Java write, libchronicle read:

    $ make -C ../native
    $ ../native/obj/shmmain :q1/ -d 2>/dev/null
    [81346680586240] one
    [81346680586241] two
    [81346680586242] three
    $

libchronicle write, Java read:

    TODO: support wire encoding for text round trip to work!
    $ ../native/obj/shmmain :q1/ -d -a HELLO
    $ ../native/obj/shmmain :q1/ -d
    $ java -cp target/java-1.0-SNAPSHOT.jar:target/dependency/chronicle-queue-5.20.102.jar mains.OutputMain q1

## Adding test data for unit tests

gzip doesn't compress megabytes of zeros efficiently - see Mark Adlers reply on
https://stackoverflow.com/questions/16792189/gzip-compression-ratio-for-zeros

bzip2 works well, and can be unpacked by `libarchive` in unit tests:

## Generating CQV5 test data

Build as above then:

    $ java -cp target/java-1.0-SNAPSHOT.jar:target/dependency/chronicle-queue-5.20.102.jar mains.InputMain qv5 < sample.input
    $ tar -cvf - qv5 | bzip2 --best > ../native/test/cqv5-sample-input.tar.bz2


### Generating CQV4 test data

CQV4 needs quite an old JVM to run, JDK 10 is the maximum. Under JDK11, or without the extra `--add-exports` options it throws `java.lang.NoSuchFieldException: reservedMemory` due to the reflection calls failing. I've included a known good POM file for this combination:

    $ export JAVA_HOME=~/java/jdk-10.0.2/
    $ mvn -B clean dependency:copy-dependencies package --file pom.cqv4.xml
    $ $JAVA_HOME/bin/java -cp target/java-1.0-SNAPSHOT.jar:target/dependency/chronicle-queue-4.6.109.jar --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED mains.InputMain cqv4 < sample.input
    $ tar -cvf - cqv4 | bzip2 --best > ../native/test/cqv4-sample-input.tar.bz2

