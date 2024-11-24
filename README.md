# Manga Text Extraction Automation

## Project Overview

This project consists of two Python scripts, **Dispatcher** and **Worker**, working together seamlessly to automate the extraction of text from manga chapters stored in an online database. The **Dispatcher** is responsible for assigning tasks to the **Worker**, while the **Worker** is dedicated to extracting text from each manga chapter. Together, these components create a fully automated, scalable solution for extracting valuable text data from manga.

The system is designed to run indefinitely until every chapter in the database has been processed, ensuring all manga content is analyzed with minimal manual intervention.

### Components

- **Dispatcher**: This script manages task distribution by assigning available chapters to the worker for processing. It monitors the database to track completed tasks and continuously assigns new ones. It also monitors the worker status, to ensure it is still online.

- **Worker**: The Worker script extracts text data from manga chapters assigned by the Dispatcher. It accesses the online database, performs text extraction on the manga pages, and stores the extracted text back into the database for further analysis. The worker is in charge to update its heartbeat every 60 seconds, on a table that keeps track of all workers and their status.

### Features
- **Automated Workflow**: Once initiated, the system runs without manual supervision, continuously extracting text from new chapters until the entire collection is processed.
- **Scalable Design**: Multiple Worker instances can be deployed in parallel to enhance performance, allowing for faster extraction as the workload grows.
- **Database Integration**: The scripts interact directly with an online database, ensuring that text extraction progress is constantly updated and easy to monitor.
- **Long-Term Operation**: Both Dispatcher and Worker are designed to operate indefinitely, making them suitable for ongoing projects that require continuous data processing.
- **offline workers fix**: The dispatcher is able to reassign tasks if a worker goes offline, ensuring that every chapter is extracted.

### How It Works
1. **Dispatcher assigns tasks**: The Dispatcher scans the database for manga chapters that require text extraction and assigns these chapters to the available Worker instances, while simultaneously ensuring that every worker is online.
2. **Worker processes the chapter**: The Worker receives the assignment, extracts the text using, and stores the extracted text back in the database, while updating its heartbeat, so that the dispatcher can recognise that it is still online.
3. **Repeat until completion**: The Dispatcher continues assigning tasks, and Workers continue processing until all chapters in the database have been fully extracted. If one or more workers go offline, an email will be sent informing the supervisor of the project.

### Docker Containerization
These scripts are ready to be containerized with Docker, which further enhances scalability and simplifies deployment. By containerizing both the Dispatcher and Worker, you can leverage Docker to manage multiple Worker instances under the same Dispatcher, all within isolated, reproducible environments. This setup allows for:
- **Efficient Scaling**: Easily scale the number of Workers by deploying multiple containers, which can be managed effortlessly with Docker Compose or Kubernetes.
- **Consistent Environments**: Ensure that both Dispatcher and Worker scripts run in identical environments, reducing compatibility issues and simplifying maintenance.
- **Load Distribution**: Docker allows you to quickly deploy more Worker containers to distribute the workload evenly, making it an ideal solution for handling large databases and optimizing resource utilization.

