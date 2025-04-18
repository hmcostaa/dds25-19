# Distributed Data Systems Project Template

Basic project structure with Python's Flask and Redis. 
**You are free to use any web framework in any language and any database you like for this project.**

# System Architecture Summary

The system is composed of three asynchronous services -- Order Service as SAGA Orchestrator, Stock Service, and Payment Service. The Order Service acts as a SAGA Orchestrator. Client requests typically enter through a load-balanced API Gateway, which communicates with the services. High availability is also provided at the entry point by load-balancing requests across multiple API Gateway instances using Nginx. ata persistence relies on dedicated Redis master-replica sets for each service (orders, stock, payments). High availability and failover for these data stores are managed by Redis Sentinel, which handles master election for write operations. A separate Redis instance stores SAGA state and supports idempotency checks.

Consistency within each service's data operations is enforced using atomic Redis transactions (WATCH/MULTI/EXEC). Idempotency for critical operations is ensured via an @idempotent decorator backed by the shared Redis instance.

While internal communication is asynchronous, the main checkout endpoint provides a synchronous-style response. The Order Service waits for the SAGA's final outcome (notified via Redis Pub/Sub) before returning a result to the gateway, masking the internal async nature from the client.

Even though, the system is designed to recover from individual service or database node failures, the consistency test is particularly sensitive to failures in the RabbitMQ message broker during transaction coordination.
## Contributions
**[Nawmi (@NawmiNujhat) & Emiel (@emiel171)]**:
Initial Architecture Contributions
* Core Logic Refinement (Order, Payment, Stock Services)
* SAGA Orchestration Refinement & Idempotency Module
* AMQP Worker/RPC Client
* Testing (Unit, Integration, Benchmark Analysis)
* Final Debugging

**[Henrique (@hmcostaa)]**:
* Initial Contribution: Participated in early architecture discussions and service setup

**[Dimitris (@dntatsis)]:**
* Initial Contribution: Participated in early architecture discussions and service setup.

**[Dennis (@)denheijmans]:**
* Initial Contribution: Participated in early architecture discussions and service setup.

Detailed code changes and commit history can be viewed in the Git log

## Testing
* Install python 3.8 or greater (tested with 3.10 on Windows 11)
* Install the required packages in the `test` folder using: `pip install -r requirements.txt`
* Change the URLs and ports in the `urls.json` file with the one provided by ingress

> Note: For Windows users you might also need to install pywin32

### Consistency test

In the provided consistency test we first populate the databases with 1 item with 100 stock that costs 1 credit
and 1000 users that have 1 credit.

Then we concurrently send 1000 checkouts of 1 item with random user/item combinations.
If everything goes well only ~10% of the checkouts will succeed, and the expected state should be 0 stock in the item
items and 100 credits subtracted across different users.

Finally, the measurements are done in two phases:
1) Using logs to see whether the service sent the correct message to the clients
2) Querying the database to see if the actual state remained consistent

#### Running
* Run script `run_consistency_test.py`

#### Interpreting results

Wait for the script to finish and check how many inconsistencies you have in both the payment and stock services. 

### Happy Path (All Services Up and Running):
 We have observed zero inconsistency across all the logs and databases in this case.

### Service Failover:
For this case, we did observe zero inconsistency across the databases but for the logs, some sporodic inconsistencies were observed. One of the plausible reasons in this case would be the sync behavior orchestrated for an async system. However, the range for such inconsistencies lie in the range of 1-20 (depending on which the service was shutdown and restarted). 
Some of the values obtained in these cases are also relevant to the number of retry and timeout operations performed.

### Stress test

To run the stress test you have to:

1) Open a terminal and navigate to the `stress-test` folder.

2) Run the `init_orders.py` to initialize the databases with the following data:

```txt
NUMBER_0F_ITEMS = 100_000
ITEM_STARTING_STOCK = 1_000_000
ITEM_PRICE = 1
NUMBER_OF_USERS = 100_000
USER_STARTING_CREDIT = 1_000_000
NUMBER_OF_ORDERS = 100_000
```

3) Run script: `locust -f locustfile.py --host="localhost"`

> Note: you can also set the --processes flag to increase the amount of locust worker processes.

4) Go to `http://localhost:8080/` to use the Locust.io UI.


To change the weight (task frequency) of the provided scenarios you can change the weights in the `tasks` definition (line 358)
With our locust file each user will make one request between 1 and 15 seconds (you can change that in line 356).

> You can also create your own scenarios as you like (https://docs.locust.io/en/stable/writing-a-locustfile.html)



#### Using the Locust UI
Fill in an appropriate number of users that you want to test with.
The hatch rate is how many users will spawn per second
(locust suggests that you should use less than 100 in local mode).

#### In terms of stress testing,
Due to the bottleneck created by single rabbitmq or nginx load-balancer for now. 
Thus, our system starts getting slower for 1000 user ramped up in 100 user/second. 

#### Stress test with Kubernetes

If you want to scale the `stress-test` to a Kubernetes clust you can follow the guide from
Google's [Distributed load testing using Google Kubernetes Engine](https://cloud.google.com/architecture/distributed-load-testing-using-gke)
and [original repo](https://github.com/GoogleCloudPlatform/distributed-load-testing-using-kubernetes).

## Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment

* `helm-config`(not used)
   Helm chart values for Redis and ingress-nginx

* `k8s`(not used)
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.

* `order`
    Folder containing the order application logic and dockerfile.

* `global_idempotency`
   Folder containing idempotency decorator function for idempotency db and app file. It is used for maintaining track of operations performed.

* `payment`
    Folder containing the payment application logic and dockerfile.

* `stock`
    Folder containing the stock application logic and dockerfile.

* `test`
    Folder containing some basic correctness tests for the entire system.

## Run 
```txt
git clone https://github.com/hmcostaa/dds25-19
cd dds25-19
docker-compose up --build #start the project using dockers
```