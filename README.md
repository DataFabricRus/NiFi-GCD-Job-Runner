# NiFi-GCD-Job-Runner
A processor for running Google Cloud Dataflow job from a template

## Prerequisites 
The bundle is builded for Apache NiFi 1.8.0

## How to use

1. Clone the project and go to the project's root folder
1. Build broject: ```mvn clean install```
1. Stop Apache NiFi service if it is working
1. Copy built nars to Apache NiFi installation:
   * ```cp ./nifi-gcd-nar/target/nifi-gcd-nar-1.0.nar <Apache NiFi path>/nifi-assembly/target/nifi-1.8.0-bin/nifi-1.8.0/lib/```
   * ```cp ./nifi-gcd-services-api-nar/target/nifi-gcd-services-api-nar-1.0.nar <Apache NiFi path>/nifi-assembly/target/nifi-1.8.0-bin/nifi-1.8.0/lib/```
1. Run Apache NiFi once again
1. You should be able to add the ```ExecuteGCDataflowJob``` processor to your data flow

The examle demonstrating the usage of the processor can be found [here](https://github.com/DataFabricRus/NiFi-GCD-Job-Runner-Example). The example datailed usage demonstration could be found [TBD].
