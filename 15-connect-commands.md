# Kafka-Connect commands

Here are some useful Kafka Connect CLI commands for managing connectors and workers on localhost:

```shell
JDBC_CONNECTOR_NAME="source_nivid_inventory_update"
JDBC_CONNECTOR_NAME="source_emp"
JDBC_CONNECTOR_NAME="source_crm_inventory"
JDBC_CONNECTOR_NAME="nivid_inventory"
JDBC_CONNECTOR_NAME="nivid_inventory_live"
JDBC_CONNECTOR_NAME="nivid_inventory_v3"
JDBC_CONNECTOR_NAME="test_config"

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8082/connectors/india_inventory/config -d @india_inventory_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8083/connectors/india_inventory/config -d @india_inventory_live.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8098/connectors/india_accounts/config -d @india_accounts_prod.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8084/connectors/dubai_inventory/config -d @dubai_inventory_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8085/connectors/dubai_inventory/config -d @dubai_inventory_live.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8086/connectors/dubai_accounts/config -d @dubai_accounts_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8087/connectors/dubai_accounts/config -d @dubai_accounts_prod.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8089/connectors/antwerp_inventory/config -d @antwerp_inventory_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8090/connectors/antwerp_inventory/config -d @antwerp_inventory_live.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8091/connectors/antwerp_accounts/config -d @antwerp_accounts_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8092/connectors/antwerp_accounts/config -d @antwerp_accounts_prod.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8093/connectors/ny_accounts/config -d @ny_accounts_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8094/connectors/ny_accounts/config -d @ny_accounts_prod.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8095/connectors/ny_inventory/config -d @ny_inventory_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8096/connectors/ny_inventory/config -d @ny_inventory_live.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8097/connectors/india_inventory_19c/config -d @india_inventory_19c.json


#8099 hk inventory dev
#8100 hk inventory prod
#8101 hk accounts dev
#8102 hk accounts prod

### Serrated under kafka-connect/jdbc_connectors/configs - for starting all at once

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8082/connectors/india_inventory/config -d @india_inventory_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8084/connectors/dubai_inventory/config -d @dubai_inventory_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8086/connectors/dubai_accounts/config -d @dubai_accounts_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8089/connectors/antwerp_inventory/config -d @antwerp_inventory_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8091/connectors/antwerp_accounts/config -d @antwerp_accounts_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8093/connectors/ny_accounts/config -d @ny_accounts_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8095/connectors/ny_inventory/config -d @ny_inventory_dev.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8099/connectors/hk_inventory/config -d @hk_inventory_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8100/connectors/hk_inventory/config -d @hk_inventory_live.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8101/connectors/hk_accounts/config -d @hk_accounts_dev.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8102/connectors/hk_accounts/config -d @hk_accounts_prod.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8083/connectors/india_inventory/config -d @india_inventory_live.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8098/connectors/dubai_inventory/config -d @india_accounts_prod.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8082/connectors/india_inventory/config -d @india_inventory_dev.json

curl -i -X PUT -H "Content-Type: application/json" http://localhost:8087/connectors/dubai_accounts/config -d @dubai_accounts_prod.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8090/connectors/antwerp_inventory/config -d @antwerp_inventory_live.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8092/connectors/antwerp_accounts/config -d @antwerp_accounts_prod.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8094/connectors/ny_accounts/config -d @ny_accounts_prod.json
curl -i -X PUT -H "Content-Type: application/json" http://localhost:8096/connectors/ny_inventory/config -d @ny_inventory_live.json

curl -s http://localhost:8082/connectors
curl -s http://localhost:8083/connectors
curl -s http://localhost:8084/connectors
curl -s http://localhost:8085/connectors
curl -s http://localhost:8086/connectors
curl -s http://localhost:8087/connectors

curl -s http://localhost:8082/connectors/india_inventory/status
curl -s http://localhost:8083/connectors/india_inventory/status
curl -s http://localhost:8084/connectors/nivid_inventory/status

curl -s http://localhost:8084/connectors/dubai_inventory/status
curl -s http://localhost:8085/connectors/dubai_inventory/status

curl -s http://localhost:8086/connectors/dubai_accounts/status
curl -s http://localhost:8087/connectors/dubai_accounts/status
curl -s http://localhost:8096/connectors/ny_inventory/status

curl -s http://localhost:8084/connectors/surat_inventory_test/status
    

curl -X DELETE http://localhost:8083/connectors/india_inventory
curl -X DELETE http://localhost:8084/connectors/dubai_inventory
curl -X DELETE http://localhost:8085/connectors/dubai_inventory
curl -X DELETE http://localhost:8084/connectors/surat_inventory_test
curl -X DELETE http://localhost:8085/connectors/dubai_accounts
curl -X DELETE http://localhost:8087/connectors/dubai_accounts

curl -X PUT -H "Content-Type: application/json" --data '{"action":"pause"}' http://localhost:8083/connectors/$JDBC_CONNECTOR_NAME/pause
curl -X PUT -H "Content-Type: application/json" http://localhost:8084/connectors/dubai_inventory/resume

curl -X POST http://localhost:8083/connectors/india_inventory/restart
curl -X POST http://localhost:8085/connectors/dubai_inventory/restart
```

1. **List all connectors:**
   ```bash
   curl -s http://localhost:8084/connectors
   curl -s http://localhost:8083/connectors
   ```

2. **Get information about a specific connector:**
   ```bash
   curl -s http://localhost:8083/connectors/$JDBC_CONNECTOR_NAME
   ```

3. **Get the status of a connector:**
   ```bash
   curl -s http://localhost:8083/connectors/$JDBC_CONNECTOR_NAME/status
   ```

4. **Pause a connector:**
   ```bash
   curl -X PUT -H "Content-Type: application/json" --data '{"action":"pause"}' http://localhost:8083/connectors/$JDBC_CONNECTOR_NAME/pause
   ```

5. **Resume a connector:**
   ```bash
   curl -X PUT -H "Content-Type: application/json" --data '{"action":"resume"}' http://localhost:8083/connectors/$JDBC_CONNECTOR_NAME/resume
   ```

6. **Restart a connector:**
   ```bash
   curl -X POST http://localhost:8083/connectors/$JDBC_CONNECTOR_NAME/restart
   ```

7. **Delete a connector:**
   ```bash
   curl -X DELETE http://localhost:8083/connectors/$JDBC_CONNECTOR_NAME
   ```

8. **List all workers:**
   ```bash
   curl -s http://localhost:8083/connector-plugins
   ```

9. **Get worker information:**
   ```bash
   curl -s http://localhost:8083/
   ```

10. **Stop a specific worker:**
    ```bash
    curl -X POST http://localhost:8083/admin/workers/<worker-id>/shutdown
    ```

11. **Stop all workers:**
    ```bash
    curl -X POST http://localhost:8083/admin/shutdown
    ```

12. **Start a worker:**
    ```bash
    curl -X POST http://localhost:8083/admin/start
    ```

13. **Restart a worker:**
    ```bash
    curl -X POST http://localhost:8083/admin/restart
    ```

### Nivid APIs fixes
- Pack/Unpack stone
<!-- BOX_NO_ -->
<!-- CREATE_DATE_ -->
<!-- PARCELNAME_ -->
CERTIFICATE_ID_
<!-- ROUGH_NAME_ -->
CP
BACK
NET_AMOUNT_DOLLAR
NET_AMOUNT_LOCAL

return Invoice Number needs to be same
return invoice number - in memo out - wrong data

- Memo out
GROSS AMOUNT DOLLAR

RATE_DOLLAR_
<!-- PARCEL_NAME -->
CERTIFICATE_ID
RATE_LOCAL
AMOUNT_LOCAL
<!-- ROUGH_NAME -->
--- Getting multiple entries with same trad_memo_detail


- Local sale
<!-- Add slash between ref invoice number - LSTKMCR1597396 -->
