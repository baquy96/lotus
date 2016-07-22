from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

def createTable(service):
    project_id = "598330041668"
    dataset_id = 'recommendation_001'
    table_id = 'product_flat_index'

    tables = service.tables()
    table_ref = {'tableId': table_id,
                 'datasetId': dataset_id,
                 'projectId': project_id}
    # [Delete table]
    tables.delete(**table_ref).execute()

    # [START create new table]
    request_body = {
        "schema": {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "type": "STRING",
                    "name": "customer_id",
                },
                {
                    "mode": "NULLABLE",
                    "type": "STRING",
                    "name": "sku",
                },
                {
                    "mode": "NULLABLE",
                    "type": "INTEGER",
                    "name": "sales",
                },
                {
                    "mode": "NULLABLE",
                    "type": "INTEGER",
                    "name": "views",
                },
                {
                    "mode": "NULLABLE",
                    "type": "INTEGER",
                    "name": "carts",
                },
                {
                    "mode": "NULLABLE",
                    "type": "FLOAT",
                    "name": "sales_effective_rate",
                },
                {
                    "mode": "NULLABLE",
                    "type": "FLOAT",
                    "name": "rating",
                },
                {
                    "mode": "NULLABLE",
                    "type": "INTEGER",
                    "name": "comments",
                },
                """
                {
                    "mode": "NULLABLE",
                    "type": "TIMESTAMP",
                    "name": "from_date",
                },
                {
                    "mode": "NULLABLE",
                    "type": "TIMESTAMP",
                    "name": "to_date",
                }
                """
            ],
        },
        "tableReference": {
            "projectId": project_id,
            "tableId": table_id,
            "datasetId": dataset_id
        }
    }

    response = tables.insert(projectId=project_id,
                             datasetId=dataset_id,
                             body=request_body).execute()
    # [END create new table]
    # print out the response

def insertValues(service):
    project_id = "598330041668"
    dataset_id = "recommendation_001"
    table_id = "product_flat_index"


    # [START run_query]

    query = ('SELECT customer_id, sku, SUM(sales) AS sales, SUM(views) AS views,'
          'SUM(carts) AS carts,  SUM(sales) / SUM(carts) AS sales_effective_rate,AVG(rating) AS rating,'
          'SUM(comments) AS comments '
          'FROM recommendation_001.user_input_product '
          'GROUP BY customer_id, sku '
             )

    configuration = {
        "query": query,
        "destinationTable": {
            "projectId": project_id,
            "datasetId": dataset_id,
            "tableId": table_id
        },
        "createDisposition": "CREATE_IF_NEEDED",
        "writeDisposition": "WRITE_APPEND"
    }
    body = {
        "configuration":
            {
                "query": configuration
            }
    }

    response= service.jobs().insert(
        projectId=project_id,
        body=body
    ).execute()
    # [END run_query]

def result(service):
    project_id = "598330041668"
    # [START run_query]
    query_request = service.jobs()
    query = ('select * from recommendation_001.product_flat_index')
    query_data = {
        'query': query
    }

    query_response = query_request.query(
        projectId=project_id,
        body=query_data).execute()
    # [END run_query]

    # [START print_results]
    return query_response


credentials = GoogleCredentials.get_application_default()
bigquery_service = build('bigquery', 'v2', credentials=credentials)
createTable(bigquery_service)
insertValues(bigquery_service)
result(bigquery_service)