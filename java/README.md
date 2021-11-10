
### Command line examples - Java

InputMain.java and OutputMain.java are provided from:
https://github.com/OpenHFT/Chronicle-Queue-Sample/tree/master/simple-input/src/main/java/net/openhft/chronicle/queue/simple/input

To run an example appender:

    (mac)    $ brew install maven
    (ubuntu) $ sudo apt-get install maven

Then

    $ mvn dependency:copy-dependencies
    $ mvn package
    $ java -cp target/java-1.0-SNAPSHOT.jar:target/dependency/chronicle-queue-5.20.102.jar mains.InputMain < sample.input
    type something
    [81346680586240] one
    [81346680586241] two
    [81346680586242] three
    Input stream finished
    $

InputMain will create a directory called 'queue' in the current directory containing a queuefile (`*.cq4`) and a `metadata.cq4t` file. The file will contain three messages in the current cycle.

For the tailer:

    $ java -cp target/java-1.0-SNAPSHOT.jar:target/dependency/chronicle-queue-5.20.102.jar mains.OutputMain 2>/dev/null
    [81346680586240] one
    [81346680586241] two
    [81346680586242] three
    $

### Interoperability

Java write, libchronicle read:

    $ make -C ../native
    $ ../native/obj/shmmain :queue/ -d 2>/dev/null
    [81346680586240] one
    [81346680586241] two
    [81346680586242] three
    $

libchronicle write, Java read:

    $ ../native/obj/shmmain :queue/ -d -a HELLO
