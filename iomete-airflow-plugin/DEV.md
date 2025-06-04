## Local development
If you want to run this project locally just open the terminal 
and execute the following command:  
```bash
docker-compose up --build
```
Custom Airflow DAGs should be added to `dags` folder.  
After each change you need to restart docker container.


```shell
docker buildx build --platform linux/amd64,linux/arm64 --push -f Dockerfile_old -t iomete/iom-airflow:2.10.3 .
```