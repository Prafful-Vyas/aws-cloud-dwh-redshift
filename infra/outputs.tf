output "bucket_name" {
  value = module.data_lake.bucket_name
}

output "redshift_workgroup" {
  value = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "redshift_database" {
  value = "dev"
}

output "redshift_admin_secret_arn" {
  value = aws_redshiftserverless_namespace.this.admin_password_secret_arn
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.olist_dwh_load.arn
}

output "glue_job_name" {
  value = aws_glue_job.csv_to_parquet.name
}

output "sns_topic_arn" {
  value = module.alerts.topic_arn
}

output "run_sql_command" {
  description = "Run this once after apply to create schemas/tables/the transform procedure."
  value       = "./scripts/run_sql.sh ${aws_redshiftserverless_workgroup.this.workgroup_name} dev ${aws_redshiftserverless_namespace.this.admin_password_secret_arn}"
}
