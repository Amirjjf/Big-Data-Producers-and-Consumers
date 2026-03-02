# This your assignment report

---

## **AI Usage Disclosure**:

>I declare that I have used AI tools in particular ChatGPT and Claude for several aspects:
> 1. finding articles and summarizing them and learning more abot the Big Data Platforms concepts, especially learning more about the programming ideas and gain better knowledge about implementation approaches for example how to implement MongoDB sharding. 
> 2. they have also been used to give me suggestions. for example: which dataset is best to use, which database technology would be better to use, what kind of shard key strategy would make sense etc.
> 3. for debugging the programming related issues in the code for example fixing python errors, understanding MongoDB configuration problems, and troubleshooting Docker.
> 4. to help me write the README.md file in a much more structured way so that the instructions are more clear. 
> 5. Assisting in generating and improving some technical code and configuration scripts. AI tools (CharGPT and Claude) were used to help draft or refine components such as the MongoDB sharding setup script, Docker configurations, data ingestion scripts, benchmarking tools, and performance testing utilities. All generated code was reviewed, modified, tested, and fully integrated by me.
> 6. AI tools were also used to proofread and improve the clarity, grammar, and vocabulary selection of the report that I have written. 

---

## Part 1 - Design

### 1.1.
#### *Application Domain and Data*

For this assignment, I basically chose the domain of urban transportation analytics, specifically I was focusing on the large-scale taxi trip data. between available open datasets that was in the assignment description, I selected the NYC Taxi Dataset. In today’s transportation systems, there are a lot of services that are producing continuous data streams with information regarding thousands of trips at a time. This domain has been chosen for this particular assignment since it is considered a realistic big data scenario where data volume, data updates, and data access are considered importnant requirements. The platform that has been implemented for this assignment which is mysimbdp-coredms, is designed in a way to analyze large collections of ride records.

The generic data that this platform supports are structured data that represent individual events, which in this case is specifically individual taxi rides. Each data record has various data fields, including the pickup and drop-off times, distance that is traveled, fare that is paid, number of passengers, and location data. In the code I have created, the data is represented as JSON-like documents within a MongoDB database. In addition, I have included other data fields during the data ingestion process, such as the pickup day and pickup month, for analytical purposes and also for sharding the data. It is good to point out that the data used for testing purposes is the NYC Yellow Taxi data, represented as a Parquet file, which is a common data format for analytical purposes.

For the core database technology, I have chosen to use MongoDB. The reason for choosing MongoDB for this assignment is its ability to store documents in a very flexible manner. Moreover, its ability to scale horizontally using the sharding feature, and also its ability to support various consistency models were other reasons why I decided to choose MongoDB. These features are basically key and crucial requirements of a big data multi-tenant database. To ensure the database is scalable and fault tolerant, the MongoDB database is deployed in a fully sharded configuration with two shard replica sets, three config servers, and also a router. The sharding key is based on the hashed key of the column pickup_day to ensure balanced performance because it allows data to be distributed evenly between nodes and this way we can avoid hotspots in performance. The deployment is done using Docker which ensures reproducibility and easy set up.

The data ingestion and benchmarking part is also written in Python. At first, I wanted to choose JavaScript as I am comfortable with it, but the choice of Python is based on the fact that it offers great libraries to work with the data and also it is compatible with the MongoDB database easily. The scripts are used to initialize sharding and ingest the data with various consistency options, to check the shard distribution, and to test the performance of the producer and consumer. Moreover, docker Compose is used to create a simple single-node setup and a full sharded setup. This is to test the system in different scales.

As for tenant data sources, I think that each tenant would provide some form of trip data, possibly from operational systems such as a dispatch system or mobile application. Each would export this data in some form of batch format periodically, mostly in a Parquet or CSV file. In my implementation, this is simulated by reading Parquet files, processing, and then inserting into the database in batches.

The platform is intended to support big data workloads, and I've made a number of assumptions to that end. I've assumed that the amount of data that we're working with could be quite large, potentially in the tens of millions (as my dataset was also quite large). I've also assumed that we'll be working with multiple sources that provide data all the time, as well as potentially many consumers that query this data concurrently. Finally, I've assumed that in some cases, we'll need to ensure that our data is strongly consistent, while in other cases, we'll be okay with a lower level of guarantee in favor of higher throughput.

---

### 1.2
#### *Design of the Platform*

In creating and designing mysimbdp, my main focus was on developing a simple and realistic architecture that would basically mimic and show the actual workings of a practical big data platform. My intention was not to develop a very complicated system, but rather, to create a simple combination of a few components in such way that supports scalable data ingestion and querying.

At the heart of the platform is MongoDB, which acts as the primary data management system for the entire platform (mysimbdp-coredms). The reason for selecting MongoDB as also mentioned before was the fact that it inherently supports the idea of document-based data and provides the facility for sharding and replication. All the trip data is stored as a structured document in MongoDB and producers and consumers can interact with it.

the overall architecture has three main components:

1. Data producers (mysimbdp-dataingest)

2. Core database platform (mysimbdp-coredms)

3. Data consumers ( query ,analytics)

The interactions among these components is straightforward. Data comes from external sources, then it is processed by the ingestion component, stored in MongoDB, and later it is accessed by consumer applications.


*Data Ingestion Flow*

In order to do the ingestion part, I decided to use a Python-based ingestion pipeline. I believe that the tenants will provide their data in batch files, such as Parquet files or csv files from their operational systems. I will read these files, transform them, and insert them into MongoDB in batch.

The typical flow is:

1. A tenant will exports trip data from their systems

2. The data file is placed in the platform’s directory

3. The ingestion script (ingest.py) will read the file

4. Timestamps and numeric fields are normalized

5. Additional fields like pickup_day and pickup_month are created

6. Records are inserted into MongoDB 

The ingestion component was designed to be flexible enough to hold various consistency models. The level of consistency may vary based on the requirements of the tenant. The data may be written with fast but lower guarantees (w1) or stronger guarantees (majority).


*Core Platform Architecture*

 The database has been structured as a sharded cluster, comprising a number of shard replica sets, a set of config servers, and a query router. The ingestion scripts, and also the consumer applications, interact only with query router and not directly with any of the shards. This has been done because it not only helps keep the application layer simple but also modular, while at the same time using the power of MongoDB by allowing it to manage internal data distribution. Then the data is automatically distributed in a number of shards based on a hashed shard key on a field of "pickup_day."

 *Consumer Interaction*

 For this, I added query scripts and benchmarking tools. These basically represent the analytics applications that query the platform for data. Consumers interact with MongoDB through mongos interface, conducting queries such as aggregation, count, and filtering.
 
 The consumer benchmarks mimic a real-world scenario where many users or services query the database at the same time. I used this to observe how the platform behaves with a mix of read and write operations.

 *Third Party Services*

 the platform is obviously dependent on several third-party tools and technologies that I did not create them myself. The first of these is MongoDB, which I am relying on for all database-related features that I used in the development. I am also relying on Docker and Docker Compose for managing all the various MongoDB components. and also I am relying on various tools related to the Python programming language, such as PyMongo, Pandas, and PyArrow. 

---

### 1.3

for the designing of the mysimbdp-coredms system, one of the key objectives that I was looking for was to ensure that the system does not have any point of failure. This is because the main part of the whole system is the database layer. Therefore, in case of a point of failure in our database layer, what it means is that that the whole system will not be usable for any of the tenants. To ensure that the system does not have any point of failure, I have chosen to have MongoDB run in a completely distributed and replicated manner rather than a simple single-node setup. This means that multiple replica sets and multiple shards have been used to make sure that no individual process is critical to the availability of service.

The configuration that I used for this project are composed of two shard replica sets, each with three MongoDB nodes, and also three config servers, and mongos query router. Each shard replica set is basically a three-node set that has a primary node and two secondary nodes. This provides a level of fault tolerance, which means that even if a node in a shard fails, other nodes in that same shard are still able to be used to serve data, and a new primary node can be chosen. Additionally, the config servers are also a three-node replica set, which eliminates any single point of failure for metadata about the cluster. Moreover, all clients and scripts only connect to a query router, or mongos, and this provides a level of abstraction, which hides failures from users.

There are many reasons why I have chosen this architecture, for example because it offers high availability and fault tolerance without requiring the system to become overly complicated to implement. In this configuration, the mysimbdp-coredms service will be able to tolerate failures on the hardware or network level, or during maintenance on individual nodes without affecting the ability for the system to function for the tenants who using it at that time. So basically as long as the majority of nodes in each replica set are functioning, the system will continue to function as expected.

---

### 1.4

As I have mentioned in the previous point, I have decided to use MongoDB replica sets as the main tool for providing availability and fault tolerance in my mysimbdp-coredms system. In my platform, I have selected the replication level of three nodes for each of the replica sets. This means that in the system, all of the shards will then have one primary node and two secondary nodes. I have selected this configuration because it is considered a minimal but still good configuration that can still work despite failures that can happen in the system, meaning that the system will still have majority of the nodes in the replica set to keep accepting writes in the MongoDB system even if for example one node fails.

Therefore, based on this decision, the number of data nodes that are required in my platform can be defined clearly. For instance, I am using two shards in my cluster, and each shard basically requires three nodes. Therefore, I need six nodes as shard nodes in total for the platform. However, apart from that, MongoDB sharding also requires config servers where cluster metadata is stored. Moreover, these config servers must be deployed as a replica set in order to ensure reliability for the system, which is the reason I deployed three separate config server node. Therefore, in order to operate mysimbdp-coredms appropriately with the level of replication that I have chosen, I need at least nine MongoDB nodes, six nodes for the two shard replica sets and three nodes for the config server replica set.

This configuration ensures that the data is replicated on multiple machines at all times and that the platform is able to continue functioning even if a failure happens on one or more of its nodes. If I wanted to set a lower replication level, such as two nodes per shard, then it would not be possible to ensure the availability of the system in the event of a failure on one of its nodes, since it would not be a majority in this case (I mean if a node is lost). However, using more than three replicas per shard would make the system more reliable, but it would also be more costly to implement. For this assignment, I believe that a replication factor of three is a good balance.

---

### 1.5

for mysimbdp-dataingest component, I aim to deploy it as close as possible to the tenant’s data sources, instead of always deploying it within the same data center where mysimbdp-coredms is there. My reason for this is that I assume the tenant would be producing some form of taxi-like data within their own environments, for example, the city operator’s servers or cloud storage for exporting daily parquet/csv files for data ingestion into platform. If the ingestion component is deployed within the tenant’s own environment or within the same cloud region as the tenant’s data storage, it would allow the ingestion component to read the data locally with high bandwidth, perform the cleaning etc. within the tenant’s own environment, and then push the final documents to the network into mysimbdp-coredms. In terms of the actual deployment of the mysimbdp-dataingest component, I would aim to package the application so that each tenant could deploy it within their own environment, but still allow it to connect securely to the platform’s mongos endpoint.

The benefit of this configuration is that it minimizes data movement that is required from the tenant side. It is really likely that reading a large Parquet file over a slow network is much worse than reading the file locally and only sending the processed data batches to the database. This configuration also makes it possible for tenants to scale ingestion independently. for example, running many ingest workers near the data source. and also without requiring direct access to the database. The other benefit is that this configuration avoids requiring the platform data center to be a bottleneck for data transfers. But on the other hand, the disadvantage of this configuration is that pushing data into mysimbdp-coredms will still be reliant on the network path between the tenant and the platform. This means that high latency and packet loss will reduce throughput and increase retries which I also faced while testing. Moreover, this configuration makes operations slightly harder since the tenant must be careful to configure their ingestion container correctly. also in situations where the tenant data sources are already inside the same data center or cloud region with mysimbdp-coredms, it makes more sense to deploy mysimbdp-dataingest in the platform environment. This configuration tends to offer the best write performance since latency to mongos is low and bandwidth is high. The disadvantage is that this configuration could cause increased resource utilization on the platform side.


---
## Part 2 - Implementation
### 2.1

For Part 2, I chose a basic tenant schema where the table structure is similar and it matches to the NYC Yellow Taxi trip record data, which is one MongoDB collection named as mysimbdp.yellow_trips. In this case, the atomic unit of the platform is basically one record of a trip event (one row). I kept the basic fields the same as what you can see in the original dataset (e.g., VendorID, pickup/drop-off timestamps, passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, and the different fare components like fare_amount, tip_amount, and total_amount, etc.). I would say this is a fairly natural fit for a MongoDB database because one trip is already "one document," and it’s fairly easy to query on those fields later

In addition to this, I have included a few derived fields in ingestion that make analytics and partitioning much easier later on. for example, specifically, I parse the pickup and drop off timestamps to datetime, and I include the millisecond versions of those timestamps in case I want to keep the original version of those fields available to me. I then include pickup_day, which is like YYYY-MM-DD, and pickup_month, which is like YYYY-MM, to make it simple to run daily and monthly queries and use pickup_day as the shard key in the sharded version. I normalize the numeric columns to numbers, and also setting any invalid values to null, to make the aggregation queries work well.

---
### 2.2

For the partitioning, I have decided to shard the tenant’s yellow_trips collection using a hashed shard key on the pickup_day field. The reason for this decision was mainly by the requirement for consistent write performance under high ingestion loads and avoiding the hot shard problem. If I decide to shard on a regular range-based date key, I would end up with all the newly ingested trips falling into the newest date range and overwhelming one shard in the process. Using a hashed shard key would allow the data to be more uniformly distributed across the shards, thus avoiding the situation where one shard does all the work and the others are idle.

for the Implementation, I have implemented the cluster as a sharded MongoDB deployment, and then I have enabled the sharding for database and the collection through initialization script that I wrote. In the init_sharding.py script, I have created the necessary index on the shard key, and then I have called the shardCollection method with the parameters {"pickup_day": "hashed"} to enable the MongoDB to shard the data based on the key. All the producers and consumers connect through the mongos, they do not connect to the shard nodes directly.

This complements the replication decision made in Part 1.4. Each shard is not only one node but also a replica set. So, I have scaling with the sharding because it spreads the dataset across shards, and I have availability in each shard. which means that, if one of the shard members fails, the shard will still continue to function, and the system will still have the advantage of spreading the data across many shards. 

---
### 2.3

For the tenant, I emulated data with the real NYC Yellow Taxi data stored as a .Parquet file, and I implemented mysimbdp-dataingest as a Python script (ingest.py). As mentioned earlier, the atomic unit that I used for the data source is one trip record, which basically means each row of the data set is one MongoDB document in mysimbdp.yellow_trips. For the ingestion process, the script reads the data, normalizes the timestamp and numeric fields, calculates the derived fields like pickup_day and pickup_month, and then inserts the data in batches (I used 1000 documents per batch).

for consistency, I've kept the ingestion flexible by allowing different write concern settings depending on what the tenant wants. The fast mode is w=1, where the write is acknowledged when the primary confirms it. This tends to give better throughput. The other mode that is also the safer mode is majority, where MongoDB only acknowledges the write after most of the nodes in the shard replica set confirm the write. This tends to give better durability if for example one of the nodes fails. In addition, journaling is configurable (on/off) and write timeout (wtimeout_ms) is allowed to give better performance when system is under heavy load.

---
### 2.4

I conducted a series of tests with varying levels of concurrency to basically evaluate the system, using 1, 5, and 10 parallel ingestion workers, as well as varying consistency setting. In all tests, ingestion scripts that I ran wrote data to a sharded mysimbdp-coredms cluster via the mongos router that I mentioned earliier. from the tests, it is clear that there is a significant improvement in overall performance with an increase in the number of parallel ingest workers, up to a certain limit. In one of the tests, for example with a write concern of w1, a single worker was able to ingest 4,900 documents per second, while with five workers, it was possible to ingest more than 16,000 documents per second which clearly shows the difference. However, with ten workers, although overall performance was high, individual worker performance became weak as the cluster started to get saturated. So the more workers doesn't always mean better performance.

I also tested the differences between the performance weak consistency (w1) and strong consistency (majority). As I was expecting, most of the tests that I did with majority write concern tended to have lower throughput and higher batch latency because the writes need to be confirmed by more nodes of the replica set. For example, during the tests with high concurrency, the majority writes had a much higher average batch latency and slightly lower throughput than the w1 writes. However, disabling journaling for majority writes had a pretty good effect on speed, which indicates that this setting has a significant effect on performance. Therefore as I discussed earlier, tenants can have faster ingestion with weaker consistency or slower ingestion with strong consistency.

I experimented with different ingestion velocities as well by using throttling options. When the ingestion velocity was capped to 2,000 documents per second per worker, the concurrency worked smoothly. so the system works in a predictable manner under controlled conditions. However, a very high parallel ingestion rate may lead to resource contention. From the above results, I conclude that the performance of “mysimbdp-coredms” can be scaled up by increasing the number of workers in the ingestion process, but it depends heavily on the consistency level. 


---
### 2.5

For the consumer, I wrote a Python-based consumer (bench_consumers.py) that executes queries such as count, aggregation, and search queries on the yellow_trips collection. When the consumers are running alone, the system scaled pretty well. I increased the concurrency from 1 to 5 and then to 10, and the throughput of the queries increased steadily based on that. I was able to achieve a throughput of over 30 queries per second without any errors or unreasonable latency. This basically indicates that the sharded cluster can handle reads well when it is not overwhelmed with many writes.

However, the scenario changed I did tests when the producers and consumers were running concurrently. In this situation, the latency of the queries was very high, and I was observing network timeout errors. As the number of concurrent consumers were increasing, the number of failed queries were rising, and the response time was averaging several seconds which is quite high. This suggests that the concurrent ingestion and querying are fighting for the same resources, and the system becomes overloaded.

In order to minimize these problems, I would suggest to enhance the deployment by adding more shard nodes to distribute the reads and writes more effectively between them. Using more mongos routers or using the secondary replica nodes for analytical queries would also help to distribute the load more effectively. This would ensure that the application is able to handle more concurrent operations without the latency issues that I observed in my tests.


---
## Part 3 Extension
### 3.1

The fact that the tenant can run `mysimbdp-dataingest` many times, and then create many different datasets over time, makes me believe that it is quite important to include basic lineage information along with the ingested data to the database. The metadata that I would like to support are for example the source file's name and location, the time at which the ingestion process was executed, the version of the ingestion script, the settings of the write concern that was chosen by the user, and the number of records processed. It would also be useful to also include technical metadata such as batch size, number of retries, target database etc. This allows the tenant to understand precisely where a certain dataset came from or for example how it was produced. 

As an example, a lineage record for an ingestion might include information such as source file path yellow_tripdata_2025-05.parquet, ingestion time 2026-02-05 01:30, target collection mysimbdp.yellow_trips, write concern majority, batch size 1000, total documents inserted 500000, and total processing time 29 seconds. Such information will obviously describe the full history of this dataset and this information is quite useful for later use. The lineage information may be stored either as a separate lineage collection within mysimbdp-coredms or as metadata files produced by the ingestion script.

In practice, much of this lineage information that I talked about can be easily and directly collected from the mysimbdp-dataingest component itself. As logging of each ingestion run is already implemented in the code, the script already has access to the dataset path, configuration parameters, and performance statistics and other things. By adding code to the script to write this information to the MongoDB database after each run, the platform is able to automatically maintain a structured history of ingested datasets without needing any work from the tenant. 


---
### 3.2

If I need a mysimbdp-coredms per tenant, then I would publish a small service discovery record per tenant into a registry like Consul. The goal is simple here. given a tenant user, I need to be able to quickly look up the database endpoint that is needed, and also a few important data that are required to properly connect to the database. in my opinion, what the record can have is for example tenant_id, an instance_id, the mongos_uri, the database names that are allocated to the tenant, and some basic deployment metadata such as region.

a schema that I would use in this case (as JSON, in the registry) can be { tenant_id, instance_id, service_type: "mysimbdp-coredms", mongo_uri, db_name, collections: ["yellow_trips"], sharded: true, replication_factor: 3, write_defaults: { w: "majority", journal: true }, created_at, updated_at, status, tags }. The key in the registry could be something like mysimbdp/coredms/<tenant_id>/<instance_id> so it’s easy to list or search by tenant. 

I prefer this approach because it makes it easy for ingestion and consumer services to discover the correct endpoint at runtime. It also makes it easier to do a multi-region implementation later, because the registry record can contain location tags.

---
### 3.3

in order to intgrate the service and data discovery fearure for the mysimbdp-dataingest, the change I would make is to how the script currently uses to select the database to use. Rather than using the MONGO_URI variable, the ingest tool would instead call a service registry such as Consul for example, using the tenant’s tenant_id. Then the registry would return correct mysimbdp-coredms connection details, such as the MongoDB URI, database name etc. Once this has been done, the rest of the ingest script would work the same way as I implemented it and explained.

one other thing that I would do is to add a small level of validation to this step, where the script should check to ensure the discovered instance is healthy before ingestion. The handling of the credentials should remain secure with the use of environment variables, while the registry maintains non-sensitive connection information.


---
### 3.4

if only mysimbdp-daas is allowed to read and write to mysimbdp-coredms, then I would modify the code of mysimbdp-dataingest so that it no longer talks directly to MongoDB ( for example no MongoClient in the ingest script). in this case ingest script would then only be used for reading the source file, processing and normalizing, and then sending the data to mysimbdp-daas API. For example, after doing normalization on each batch of trips, the ingest script could make a request like POST /tenants/{id}/datasets/yellow_trips/batch with a JSON payload. the consumers also would make requests to the mysimbdp-daas endpoints instead of directly querying MongoDB.

In this way, mysimbdp-daas would become a controlled gateway, and it would be possible for it to control authentication, rate limiting, default write concern settings etc. I would also put things like consistency options (w1 vs majority) at the API level so that it can be passed as a request parameter for the tenant

The architecture can be something like below:


```text
Tenant Data Sources (Parquet/CSV)
            |
            v
   mysimbdp-dataingest
            |
            v   API
        mysimbdp-daas  (auth, routing)
            |
            v
      mysimbdp-coredms (MongoDB sharded cluster)
            
   Data consumers / analytics apps ---> API calls ---> mysimbdp-daas
```


---
### 3.5

one approach to use to split hot and cold data in mysimbdp-coredms is to basically base it on how recent or frequently accessed it is. what I mean is that, I could decide that trips from the last 30 days are considered hot data because they are frequently accessed for daily analytics, while trips older than 30 days are considered cold data because they are not frequently used for queries. there are other potential constraints that could be considered for example query frequencies, business value, and size of data. Hot space is more likely to be kept on fast storage with a high number of replicas, while cold space is likely to be kept on cheaper nodes.

To support automatic movement what I would do is that I would implement a background process that periodically scans the collection for documents that no longer meet the hot criteria for example documents in the collection where the pickup_day field is more than 30 days old and move them to another collection. However, some inconsistencies may be introduced in the process. For example, if a query is run at the same time as the migration process, the results of the query may be incomplete because part of the data may be in hot space and part in cold space.