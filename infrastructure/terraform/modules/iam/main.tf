data "aws_iam_policy_document" "assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [var.TRUSTED_PRINCIPAL_ARN]
    }
  }
}

resource "aws_iam_role" "etl_role" {
  name               = "${var.PROJECT_NAME}-etl-role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

resource "aws_iam_policy" "etl_policy" {
  name        = "${var.PROJECT_NAME}-etl-policy"
  description = "ETL access to S3 buckets and RDS connect"
  policy      = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["s3:GetObject","s3:ListBucket","s3:PutObject"],
        Resource = [
          "arn:aws:s3:::${var.RAW_BUCKET}",
          "arn:aws:s3:::${var.RAW_BUCKET}/*",
          "arn:aws:s3:::${var.PROCESSED_BUCKET}",
          "arn:aws:s3:::${var.PROCESSED_BUCKET}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = ["rds-db:connect"],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "etl_attach" {
  role       = aws_iam_role.etl_role.name
  policy_arn = aws_iam_policy.etl_policy.arn
}

# Attach ETL policy to data-pipeline-user for Bronze layer operations
resource "aws_iam_user_policy_attachment" "data_pipeline_user_etl" {
  user       = "data-pipeline-user"
  policy_arn = aws_iam_policy.etl_policy.arn
}

# IAM Role for Kafka Connect to Bronze S3 Layer
data "aws_iam_policy_document" "kafka_connect_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "kafka_connect_role" {
  name               = "${var.PROJECT_NAME}-kafka-connect-role"
  assume_role_policy = data.aws_iam_policy_document.kafka_connect_assume_role.json

  tags = {
    Name    = "${var.PROJECT_NAME}-kafka-connect-role"
    Project = var.PROJECT_NAME
  }
}

# Policy for Kafka Connect to access S3 Bronze layer
resource "aws_iam_policy" "kafka_connect_policy" {
  name        = "${var.PROJECT_NAME}-kafka-connect-policy"
  description = "Policy for Kafka Connect to access S3 and other AWS services"
  policy      = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::${var.RAW_BUCKET}",
          "arn:aws:s3:::${var.RAW_BUCKET}/*",
          "arn:aws:s3:::${var.PROCESSED_BUCKET}",
          "arn:aws:s3:::${var.PROCESSED_BUCKET}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ],
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "kafka_connect_attach" {
  role       = aws_iam_role.kafka_connect_role.name
  policy_arn = aws_iam_policy.kafka_connect_policy.arn
}

# IAM Role for Databricks/Spark execution
data "aws_iam_policy_document" "spark_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type = "AWS"
      identifiers = [
        var.TRUSTED_PRINCIPAL_ARN
      ]
    }

    # Add Databricks service principal if needed
    dynamic "principals" {
      for_each = var.TRUSTED_PRINCIPAL_ARN != "" ? [] : [1]
      content {
        type        = "Service"
        identifiers = ["ec2.amazonaws.com"]
      }
    }
  }
}

resource "aws_iam_role" "spark_execution_role" {
  name               = "${var.PROJECT_NAME}-spark-execution-role"
  assume_role_policy = data.aws_iam_policy_document.spark_assume_role.json

  tags = {
    Name    = "${var.PROJECT_NAME}-spark-execution-role"
    Project = var.PROJECT_NAME
  }
}

# Enhanced policy for Spark jobs
resource "aws_iam_policy" "spark_execution_policy" {
  name        = "${var.PROJECT_NAME}-spark-execution-policy"
  description = "Policy for Spark/Databricks to access S3 and other AWS services"
  policy      = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        Resource = [
          "arn:aws:s3:::${var.RAW_BUCKET}",
          "arn:aws:s3:::${var.RAW_BUCKET}/*",
          "arn:aws:s3:::${var.PROCESSED_BUCKET}",
          "arn:aws:s3:::${var.PROCESSED_BUCKET}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
          "logs:DescribeLogGroups"
        ],
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow",
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ],
        Resource = "arn:aws:secretsmanager:*:*:secret:snowflake/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "spark_execution_attach" {
  role       = aws_iam_role.spark_execution_role.name
  policy_arn = aws_iam_policy.spark_execution_policy.arn
}
