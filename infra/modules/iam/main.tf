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
