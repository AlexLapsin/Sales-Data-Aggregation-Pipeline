# IAM role for Snowflake to access S3 Silver layer
resource "aws_iam_role" "snowflake_access_role" {
  name        = "${var.PROJECT_NAME}-SnowflakeAccessRole-${var.ENVIRONMENT}"
  description = "Allows Snowflake to read Delta Lake Silver layer from S3"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = var.SNOWFLAKE_IAM_USER_ARN
      }
      Action = "sts:AssumeRole"
      Condition = {
        StringEquals = {
          "sts:ExternalId" = var.SNOWFLAKE_EXTERNAL_ID
        }
      }
    }]
  })

  tags = {
    Project     = var.PROJECT_NAME
    Environment = var.ENVIRONMENT
    ManagedBy   = "Terraform"
    Purpose     = "Snowflake S3 Silver layer access"
  }
}

# S3 read policy for Silver layer
resource "aws_iam_policy" "snowflake_s3_silver_read" {
  name        = "${var.PROJECT_NAME}-SnowflakeS3SilverReadPolicy-${var.ENVIRONMENT}"
  description = "Read access to S3 Silver layer for Snowflake external tables"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.PROCESSED_BUCKET}",
          "arn:aws:s3:::${var.PROCESSED_BUCKET}/silver/*"
        ]
      }
    ]
  })

  tags = {
    Project     = var.PROJECT_NAME
    Environment = var.ENVIRONMENT
    ManagedBy   = "Terraform"
  }
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "snowflake_s3_access" {
  role       = aws_iam_role.snowflake_access_role.name
  policy_arn = aws_iam_policy.snowflake_s3_silver_read.arn
}
