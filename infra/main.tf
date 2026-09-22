module "kms" {
  source      = "./modules/kms_key"
  name        = "${var.project_name}-key"
  description = "CMK for the Olist cloud data warehouse pipeline (Redshift + Glue + Step Functions)."
}

module "data_lake" {
  source      = "./modules/s3_data_lake_bucket"
  bucket_name = var.bucket_name
  kms_key_arn = module.kms.key_arn
}

module "alerts" {
  source      = "./modules/alerting"
  name        = "${var.project_name}-alerts"
  kms_key_arn = module.kms.key_arn
  alert_email = var.alert_email
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# ---------------------------------------------------------------------------
# Glue: CSV -> Parquet job
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "olist_processed" {
  name = "olist_processed"
}

resource "aws_iam_role" "glue_etl" {
  name = "${var.project_name}-glue-etl-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_etl" {
  name = "${var.project_name}-glue-etl-policy"
  role = aws_iam_role.glue_etl.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadRawWriteProcessed"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [module.data_lake.bucket_arn, "${module.data_lake.bucket_arn}/raw/*", "${module.data_lake.bucket_arn}/processed/*", "${module.data_lake.bucket_arn}/tmp/*", "${module.data_lake.bucket_arn}/scripts/*"]
      },
      {
        Sid      = "KmsForData"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = [module.kms.key_arn]
      },
      {
        Sid      = "Logging"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:${var.aws_region}:*:log-group:/aws-glue/*"]
      }
    ]
  })
}

resource "aws_s3_object" "etl_script" {
  bucket = module.data_lake.bucket_name
  key    = "scripts/csv_to_parquet.py"
  source = "${path.module}/../glue_jobs/csv_to_parquet.py"
  etag   = filemd5("${path.module}/../glue_jobs/csv_to_parquet.py")
}

resource "aws_glue_job" "csv_to_parquet" {
  name              = "${var.project_name}-csv-to-parquet"
  role_arn          = aws_iam_role.glue_etl.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${module.data_lake.bucket_name}/${aws_s3_object.etl_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${module.data_lake.bucket_name}/tmp/glue/"
    "--raw_path"                         = "s3://${module.data_lake.bucket_name}/raw/olist"
    "--processed_path"                   = "s3://${module.data_lake.bucket_name}/processed/olist"
  }
}

# ---------------------------------------------------------------------------
# Redshift Serverless
# ---------------------------------------------------------------------------

resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-redshift-sg"
  description = "Redshift Serverless workgroup security group"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "redshift_self_ingress" {
  type              = "ingress"
  from_port         = 5439
  to_port           = 5439
  protocol          = "tcp"
  self              = true
  security_group_id = aws_security_group.redshift.id
}

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name        = "${var.project_name}-ns"
  admin_username         = var.redshift_admin_username
  manage_admin_password  = true # AWS creates and rotates the admin password in Secrets Manager
  db_name                = "dev"
  kms_key_id              = module.kms.key_arn
  default_iam_role_arn   = aws_iam_role.redshift_copy.arn
  iam_roles              = [aws_iam_role.redshift_copy.arn]
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name      = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name       = "${var.project_name}-wg"
  base_capacity        = var.redshift_base_capacity
  publicly_accessible = false
  subnet_ids           = data.aws_subnets.default.ids
  security_group_ids  = [aws_security_group.redshift.id]
}

resource "aws_iam_role" "redshift_copy" {
  name = "${var.project_name}-redshift-copy-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_copy" {
  name = "${var.project_name}-redshift-copy-policy"
  role = aws_iam_role.redshift_copy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [module.data_lake.bucket_arn, "${module.data_lake.bucket_arn}/processed/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = [module.kms.key_arn]
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Step Functions
# ---------------------------------------------------------------------------

resource "aws_iam_role" "step_functions" {
  name = "${var.project_name}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${var.project_name}-sfn-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "GlueJobControl"
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = [aws_glue_job.csv_to_parquet.arn]
      },
      {
        Sid      = "RedshiftDataApi"
        Effect   = "Allow"
        Action   = ["redshift-data:ExecuteStatement", "redshift-data:DescribeStatement", "redshift-data:GetStatementResult", "redshift-data:BatchExecuteStatement"]
        Resource = ["*"]
      },
      {
        Sid      = "GetAdminSecret"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [aws_redshiftserverless_namespace.this.admin_password_secret_arn]
      },
      {
        Sid      = "Notify"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [module.alerts.topic_arn]
      },
      {
        Sid      = "EventsForSyncTasks"
        Effect   = "Allow"
        Action   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
        Resource = ["arn:aws:events:${var.aws_region}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsFor*"]
      }
    ]
  })
}

resource "aws_sfn_state_machine" "olist_dwh_load" {
  name     = "${var.project_name}-load"
  role_arn = aws_iam_role.step_functions.arn

  definition = templatefile("${path.module}/../stepfunctions/state_machine.asl.json.tpl", {
    glue_job_name          = aws_glue_job.csv_to_parquet.name
    workgroup_name           = aws_redshiftserverless_workgroup.this.workgroup_name
    database_name             = "dev"
    secret_arn                 = aws_redshiftserverless_namespace.this.admin_password_secret_arn
    redshift_copy_role_arn    = aws_iam_role.redshift_copy.arn
    bucket_name                = module.data_lake.bucket_name
    sns_topic_arn               = module.alerts.topic_arn
  })
}

# ---------------------------------------------------------------------------
# EventBridge trigger: new object under processed/olist/orders/ starts the state machine
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_notification" "eventbridge" {
  bucket      = module.data_lake.bucket_name
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "new_orders_processed" {
  name = "${var.project_name}-new-orders-processed"

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [module.data_lake.bucket_name] }
      object = { key = [{ prefix = "processed/olist/orders/" }] }
    }
  })

  depends_on = [aws_s3_bucket_notification.eventbridge]
}

resource "aws_iam_role" "eventbridge_sfn" {
  name = "${var.project_name}-eventbridge-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn" {
  name = "${var.project_name}-eventbridge-sfn-policy"
  role = aws_iam_role.eventbridge_sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = [aws_sfn_state_machine.olist_dwh_load.arn]
    }]
  })
}

resource "aws_cloudwatch_event_target" "start_state_machine" {
  rule     = aws_cloudwatch_event_rule.new_orders_processed.name
  arn      = aws_sfn_state_machine.olist_dwh_load.arn
  role_arn = aws_iam_role.eventbridge_sfn.arn
}

data "aws_iam_policy_document" "sns_eventbridge" {
  statement {
    sid    = "AllowEventBridgePublish"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    actions   = ["SNS:Publish"]
    resources = [module.alerts.topic_arn]
  }
}

resource "aws_sns_topic_policy" "allow_eventbridge" {
  arn    = module.alerts.topic_arn
  policy = data.aws_iam_policy_document.sns_eventbridge.json
}

# ---------------------------------------------------------------------------
# Failure alerting: Step Functions execution failures -> SNS (belt-and-braces
# alongside the state machine's own NotifyFailure branch)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "sfn_execution_failed" {
  name = "${var.project_name}-sfn-execution-failed"

  event_pattern = jsonencode({
    source        = ["aws.states"]
    "detail-type" = ["Step Functions Execution Status Change"]
    detail = {
      status          = ["FAILED", "TIMED_OUT", "ABORTED"]
      stateMachineArn = [aws_sfn_state_machine.olist_dwh_load.arn]
    }
  })
}

resource "aws_cloudwatch_event_target" "sfn_execution_failed_sns" {
  rule = aws_cloudwatch_event_rule.sfn_execution_failed.name
  arn  = module.alerts.topic_arn
}
