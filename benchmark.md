# Benchmarks

To compare the performance of regular queries vs. queries filtered via this plugin, the benchmarking tests can be executed. This is what a test does:

- Removes the index in question entirely (e.g. `student`)
- Loads in bulk a set of entries. Every entry is generated with a (short) random name, a random graduation year (uniformly distributed between 2020 and 2026), and a GPA (random floating number between 0 and 5) 
- Runs five queries that search the entries with GPAs matching the intervals [0, 1), [1, 2), etc. 
- The average query response time is computed. This does not include network latency - only server side processing

The test is run twice, one without Cedarling (regular OpenSearch query), and one with the plugin. To avoid bias due to caching or other factors, the database is restarted before executing every test.

When the test is run with plugin filtering, the average decision time is also reported. This allows to discriminate the overhead introduced by Cedarling and the plugin separately.  

Details like the server to point to, the index name, and the number of random entries to generate, among others, are parameterizable.

## Performance data

TODO

The following data were obtained in a setup using a [Digital Ocean](https://slugs.do-api.dev/) Basic VM (s-4vcpu-8gb) with Ubuntu 22, OpenSearch 3.0.0 (single node, package-based installation with default configuration), and Jans Server 1.8.0:

|Measurement|.|
|-|-|
|Average query response time (regular)|.|
|Average query response time (plugin)|.|
|Ratio|.|
|Average Cedarling authz time per entry|.|

All times measured in milliseconds. Number of entries (bulk size) per test was . 

The above shows the average overhead introduced by the plugin is %.


## How to run

For the interested, TODO