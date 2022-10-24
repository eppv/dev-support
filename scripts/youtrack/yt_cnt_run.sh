
docker run -it --name youtrack_server \
-v ./youtrack_data:/opt/data/youtrack/data \
-v ./youtrack_data/conf:/opt/youtrack/conf \
-v ./youtrack_data/logs:/opt/youtrack/logs \
-v ./youtrack_data/backups:/opt/youtrack/backups \
-p 80:8080 \
jetbrains/youtrack: 2022.1.42950