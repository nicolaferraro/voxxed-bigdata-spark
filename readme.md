# Voxxed BigData Spark
 
This repository contains the code of the Spark application used as demo at Voxxed Bucharest 2017.
 
## Running

1) Start [Openshift Origin Development Environment](https://github.com/openshift/origin/blob/master/docs/cluster_up_down.md). 

2) Deploy the [Kafka service](https://github.com/nicolaferraro/voxxed-bigdata-kafka).

3) Deploy the [Web application](https://github.com/nicolaferraro/voxxed-bigdata-web).

4) Install the Oshinko web console: `./install-web-console.sh`

5) Execute: `mvn clean fabric8:deploy`

## Integration tests

To run the integration tests, ensure you are logged in as admin, then execute:
```
mvn clean verify -P openshift-it
```