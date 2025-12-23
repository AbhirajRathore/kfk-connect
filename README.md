# JDBC connectors

### Prerequisites
- 
```shell
source delta.env
source ~/confluent-7.5.2/delta_configs/env.delta
```
- kafka-jdbc-connect install in docker with jdbc source connector plugin

### Commands

1. Create or update connector

    ```shell
    curl -i -X PUT -H "Content-Type: application/json" http://localhost:8083/connectors/nivid_inventory_dev/config -d @nivid_inventory_v3.json
    curl -i -X PUT -H "Content-Type: application/json" http://localhost:8083/connectors/nivid_inventory_live/config -d @nivid_inventory_live.json
    ```
