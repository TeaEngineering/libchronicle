
### Command line examples - Java

InputMain.java and OutputMain.java are provided from:
https://github.com/OpenHFT/Chronicle-Queue-Sample/tree/master/simple-input/src/main/java/net/openhft/chronicle/queue/simple/input

To run an example appender:

    $ brew install maven
    $ mvn dependency:copy-dependencies
    $ java -cp target/java-1.0-SNAPSHOT.jar:target/dependency/chronicle-queue-4.6.109.jar mains.InputMain


For the tailer:

    $ java -cp target/java-1.0-SNAPSHOT.jar:target/dependency/chronicle-queue-4.6.109.jar mains.OutputMain


The appender will create a directory called 'queue' in the current directory containing a queuefile (`*.cq4`) and a `directory-listing.cq4t` file.

You can test basic interoperability
