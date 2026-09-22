{
  "Comment": "Olist DWH load: Glue -> Redshift staging -> transform -> notify",
  "StartAt": "RunGlueJob",
  "States": {
    "RunGlueJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "${glue_job_name}" },
      "Retry": [{ "ErrorEquals": ["Glue.AWSGlueException"], "IntervalSeconds": 30, "MaxAttempts": 2, "BackoffRate": 2.0 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "NotifyFailure" }],
      "Next": "CopyToStaging"
    },
    "CopyToStaging": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement.sync",
      "Parameters": {
        "WorkgroupName": "${workgroup_name}",
        "Database": "${database_name}",
        "SecretArn": "${secret_arn}",
        "Sql": "TRUNCATE staging.orders; COPY staging.orders FROM 's3://${bucket_name}/processed/olist/orders/' IAM_ROLE '${redshift_copy_role_arn}' FORMAT AS PARQUET; TRUNCATE staging.order_items; COPY staging.order_items FROM 's3://${bucket_name}/processed/olist/order_items/' IAM_ROLE '${redshift_copy_role_arn}' FORMAT AS PARQUET; TRUNCATE staging.customers; COPY staging.customers FROM 's3://${bucket_name}/processed/olist/customers/' IAM_ROLE '${redshift_copy_role_arn}' FORMAT AS PARQUET; TRUNCATE staging.sellers; COPY staging.sellers FROM 's3://${bucket_name}/processed/olist/sellers/' IAM_ROLE '${redshift_copy_role_arn}' FORMAT AS PARQUET; TRUNCATE staging.products; COPY staging.products FROM 's3://${bucket_name}/processed/olist/products/' IAM_ROLE '${redshift_copy_role_arn}' FORMAT AS PARQUET; TRUNCATE staging.payments; COPY staging.payments FROM 's3://${bucket_name}/processed/olist/payments/' IAM_ROLE '${redshift_copy_role_arn}' FORMAT AS PARQUET;"
      },
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "NotifyFailure" }],
      "Next": "RunTransform"
    },
    "RunTransform": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement.sync",
      "Parameters": {
        "WorkgroupName": "${workgroup_name}",
        "Database": "${database_name}",
        "SecretArn": "${secret_arn}",
        "Sql": "CALL dw.sp_transform_orders();"
      },
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "NotifyFailure" }],
      "Next": "NotifySuccess"
    },
    "NotifySuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": { "TopicArn": "${sns_topic_arn}", "Message": "Olist DWH load succeeded" },
      "End": true
    },
    "NotifyFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": { "TopicArn": "${sns_topic_arn}", "Message": "Olist DWH load FAILED" },
      "End": true
    }
  }
}
