# Lambda function
resource "aws_lambda_function" "aws_lambda_partition_submit" {
  image_uri     = "${data.aws_ecr_repository.partition_submit.repository_url}:latest"
  function_name = "${var.prefix}-partition-submit"
  role          = aws_iam_role.aws_lambda_execution_role.arn
  package_type  = "Image"
  memory_size   = 256
  timeout       = 300
  ephemeral_storage {
    size = 1024
  }
  vpc_config {
    subnet_ids         = data.aws_subnets.private_application_subnets.ids
    security_group_ids = data.aws_security_groups.vpc_default_sg.ids
  }
  file_system_config {
    arn              = data.aws_efs_access_points.aws_efs_generate_ap.arns[1]
    local_mount_path = "/mnt/data"
  }
}

# Lambda resource-based policy
resource "aws_lambda_permission" "aws_lambda_partition_submit_sqs" {
  statement_id  = "AllowExecutionFromSQS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.aws_lambda_partition_submit.function_name
  principal     = "sqs.amazonaws.com"
  source_arn    = data.aws_sqs_queue.download_lists.arn
}

# Lambda event source mapping
resource "aws_lambda_event_source_mapping" "aws_lambda_partition_submit_event_mapping" {
  event_source_arn = data.aws_sqs_queue.download_lists.arn
  function_name    = aws_lambda_function.aws_lambda_partition_submit.arn
  enabled          = true
  batch_size       = 1
  depends_on = [
    aws_lambda_function.aws_lambda_partition_submit
  ]
}

# Lambda role and policy
resource "aws_iam_role" "aws_lambda_execution_role" {
  name = "${var.prefix}-lambda-partition-submit-execution-role"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "lambda.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
  permissions_boundary = "arn:aws:iam::${local.account_id}:policy/NGAPShRoleBoundary"
}

resource "aws_iam_role_policy_attachment" "aws_lambda_execution_role_policy_attach" {
  role       = aws_iam_role.aws_lambda_execution_role.name
  policy_arn = aws_iam_policy.aws_lambda_execution_policy.arn
}

resource "aws_iam_policy" "aws_lambda_execution_policy" {
  name        = "${var.prefix}-lambda-partition-submit-execution-policy"
  description = ""
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Sid" : "AllowCreatePutLogs",
        "Effect" : "Allow",
        "Action" : [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource" : "arn:aws:logs:*:*:*"
      },
      {
        "Sid" : "AllowVPCAccess",
        "Effect" : "Allow",
        "Action" : [
          "ec2:CreateNetworkInterface"
        ],
        "Resource" : concat([for subnet in data.aws_subnet.private_application_subnet : subnet.arn], ["arn:aws:ec2:${var.aws_region}:${local.account_id}:*/*"])
      },
      {
        "Sid" : "AllowVPCDelete",
        "Effect" : "Allow",
        "Action" : [
          "ec2:DeleteNetworkInterface"
        ],
        "Resource" : "arn:aws:ec2:${var.aws_region}:${local.account_id}:*/*"
      },
      {
        "Sid" : "AllowVPCDescribe",
        "Effect" : "Allow",
        "Action" : [
          "ec2:DescribeNetworkInterfaces",
        ],
        "Resource" : "*"
      },
      {
        "Sid" : "AllowSQSAccessDL",
        "Effect" : "Allow",
        "Action" : [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ],
        "Resource" : "${data.aws_sqs_queue.download_lists.arn}"
      },
      {
        "Sid" : "AllowEFSAccess",
        "Effect" : "Allow",
        "Action" : [
          "elasticfilesystem:ClientMount",
          "elasticfilesystem:ClientWrite"
        ],
        "Resource" : "${data.aws_efs_access_points.aws_efs_generate_ap.arns[1]}"
      },
      {
        "Sid" : "AllowListBucket",
        "Effect" : "Allow",
        "Action" : [
          "s3:ListBucket"
        ],
        "Resource" : "${data.aws_s3_bucket.s3_download_lists.arn}"
      },
      {
        "Sid" : "AllowGetDeleteObject",
        "Effect" : "Allow",
        "Action" : [
          "s3:GetObject",
          "s3:DeleteObject"
        ],
        "Resource" : "${data.aws_s3_bucket.s3_download_lists.arn}/*"
      },
      {
        "Sid" : "AllowKMSKeyAccess",
        "Effect" : "Allow",
        "Action" : [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ],
        "Resource" : "${data.aws_kms_key.aws_s3.arn}"
      },
      {
        "Sid" : "AllowSQSAccessPJ",
        "Effect" : "Allow",
        "Action" : [
          "sqs:SendMessage"
        ],
        "Resource" : "${data.aws_sqs_queue.pending_jobs.arn}"
      },
      {
        "Sid" : "AllowSSMGetPut",
        "Effect" : "Allow",
        "Action" : [
          "ssm:GetParameter",
          "ssm:PutParameter"
        ],
        "Resource" : "arn:aws:ssm:${var.aws_region}:${local.account_id}:parameter/${var.prefix}*"
      },
      {
        "Sid" : "AllowBatchSubmitJob",
        "Effect" : "Allow",
        "Action" : [
          "batch:SubmitJob"
        ],
        "Resource" : [
          "arn:aws:batch:${var.aws_region}:${local.account_id}:job-definition/${var.prefix}*",
          "arn:aws:batch:${var.aws_region}:${local.account_id}:job-queue/${var.prefix}*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "sns:ListTopics"
        ],
        "Resource" : "*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "sns:Publish"
        ],
        "Resource" : "${data.aws_sns_topic.batch_failure_topic.arn}"
      }
    ]
  })
}