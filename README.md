flume-kinesis
===========

Flume Source and sink for Kinesis

License
===========

This is released under Apache License v2


Example configuration
===========

Please refer to the kinesis_to_kinesis file under the conf directory


Building
===========

This project uses maven for building all the artefacts.
You can build it with the following command:
    mvn clean install

This will build the following artefacts:

* flume-kinesis-dist/target/flume-kinesis-1.0.0-SNAPSHOT-dist.tar.gz
  The tarball can be directly unpacked into Apache Flume plugins.d directory

* flume-kinesis-dist/target/rpm/tango-flume-redis/RPMS/noarch/tango-flume-kinesis-1.0.0-SNAPSHOT20140729050204.noarch.rpm
  This package will install itself on top of Apache Flume package and be ready for use right away.

