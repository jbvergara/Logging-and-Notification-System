AWS Logging and Notification System
=================
Utilizes an API Gateway and Lambda to receive logs from multiple applications and save the data to a DynamoDB table and publish the logs to an SNS topic. The application also calls for a 3rd party API for critical logs.

1. bulk_renamer.py - renames multiple files at once to a specified format
2. deploy_function.py - deploys lambda code
3. lambda_function.py - lambda function code
4. products_csv_parse.py - parses a csv file and removes the row if the cell of a specific column is empty