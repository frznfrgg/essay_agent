0) Make sure that there are no old volumes and networks.
1) Run milvus via:  
```bash
docker compose -f docker-compose.milvus.yaml up -d
```
2) Verify that milvus is running and healthy:  
```bash
watch docker ps -a
```
3) Run airflow, postgres and redis via:  
```bash
docker compose -f docker-compose.data.yaml up -d
```
4) Optionally, you can add data running *create_tables.py* and *fill_tables.py* from */postgres_db_setup*