# parition & submit

Partitions download list creator text files in AWS Batch jobs (which contain available OBPG downloads) and then submits all Generate worfklow jobs to AWS Batch (downloader -> combiner -> processor -> upload).

Top-level Generate repo: https://github.com/podaac/generate

## pre-requisites to building

None.

## build command

`docker build --tag partition_submit:0.1 . `

## execute command

Arguments:
- Taken from event that triggers AWS Lambda (download list creator publishes to SQS queue).

MODIS A: 
`docker run --rm --name ps`

MODIS T: 
`docker run --rm --name ps`

VIIRS: 
`docker run --rm --name ps`

**NOTES**
- In order for the commands to execute the `/?/` directories will need to point to actual directories on the system.

## aws infrastructure

The partition and submit component includes the following AWS services:
- AWS Lambda function.
- AWS S3 bucket to hold input text files.
- AWS SQS queue to allow the publication of the list of text files.
- AWS SQS queue to track pending AWS jobs from previous runs.
- AWS SSM parameters to track the number of available IDL licenses.

## terraform 

Deploys AWS infrastructure and stores state in an S3 backend using a DynamoDB table for locking.

To deploy:
1. Edit `terraform.tfvars` for environment to deploy to.
2. Edit `terraform_conf/backed-{prefix}.conf` for environment deploy.
3. Initialize terraform: `terraform init -backend-config=terraform_conf/backend-{prefix}.conf`
4. Plan terraform modifications: `terraform plan -out=tfplan`
5. Apply terraform modifications: `terraform apply tfplan`

`{prefix}` is the account or environment name.