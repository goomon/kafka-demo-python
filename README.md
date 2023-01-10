# Kafka demo
## Kafka Environment settings

```shell
docker-compose -f <target-file> up [-d]
```
* Target file

    You should write the commands below in `docker/` directory. Otherwise volume files are created in project root directory.
    * `docker/kafka-single.yml`
  
        This is for single kafka broker and single zookeeper environment.
        ```shell
      docker-compose -f kafka-single.yml up
        ```

    * `docker/kafka-cluster.yml`
    
        This is for kafka basic cluster environment with 3 broker ans 3 zookeepers.
      ```shell
      docker-compose -f kafka-cluster.yml up
        ```
* Bais commands
    
    This section is for basic commands to control docker based environment. You can also use them in GUI environment with [Docker Desktop](https://www.docker.com/products/docker-desktop/).
  * Stop containers
     ```shell
    docker-compose -f <target-file> stop
    ```
  * Remove containers
     ```shell
    docker-compose -f <target-file> down
    ```
  * Remove container volume
     ```shell
    docker volume prune -y
    ```

### Error Handling
If the cluster setting is not working, it might be due to those two reasons.
1. Authentication problem for volume directory.
    
    I recommend you to remove shared volume file and try it again.
    ```shell
   rm -rf docker/kafka docker/zookeeper
   docker volume prune
   docker-compose -f <traget-file> up
   ```
   Otherwise, you need to change authentication in file by using `chmod` linux commands
   ```shell
    sudo chmod u+w docker/
    ```
   
2. Docker resources problems.
        
    You should increase the CPU and memory resources that docker can use in Docker Desktop. You can modify the limitation in Docker **_Desktop > Preferences > Resources_**.

## Kafka Monitoring
You can use kafka monitoring system AKHQ. You can find out more information in [here](https://akhq.io/).
* How to access: http://localhost:8080
* (Recommended) Live tail option is really useful to track kafka records in real-time.

## Kafka Client
* python version: Python 3.9
```shell
pip install - r requirements.txt
```
### Create Topic
```shell
python create_topic.py [ --np <number of partitions> ] [ --rf <replication factors> ]
```
* Number of partitions ans replication factors is set to 1 by default.

### Publish Messages
* You need to execute Flask server first.
    ```
  python flaskserver.py
  ```
* Test API: GET http://localhost/sensordata/10
  * You shoud receive 200 status.
```shell
python publish_topic.py [--count <number of data> ] [ --delay <delay sec per publishing> ]
```
* Number of data is set to 10 and delay of per publishing is set to 0.5sec by default.

### Consume Messages

```shell
python consume_data.py
```

* Since consumer is executed under infinite loop, you need to signal Ctrl+C interrupt to exit program.