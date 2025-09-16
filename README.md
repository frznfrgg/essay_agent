## Installation
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
4) Database would init automatically



## Usage
0) Tables would be created and filled with initial automatically after airflow service gets up
1) To add a group of students to the database firstly place a folder named by a group index into the data/incoming. Folder should contain essay files named like Name_Surname.docx
2) To begin addition process you should trigger the process_students_docs dag using config like the following one:  
```json
{
  "subfolder": "cohort0",
  "graduation_date": "2025-08-01",
  "courses": ["Стратегия", "Маркетинг", "Лидерство"]
}
```