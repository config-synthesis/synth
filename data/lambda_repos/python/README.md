# Python Serverless Repos

This dataset is a list of repos that contain serverless configuration files.
Files must either be in the AWS SAM (`template.yaml`), Serverless 
(`serverless.yml`), or Dockerfile formats. The dataset was generated from
Google BigQuery using the following query.

```bigquery
SELECT
    file.repo_name, file.ref, file.path, file.symlink_target
FROM
    `bigquery-public-data.github_repos.files` file
        JOIN
    `bigquery-public-data.github_repos.contents` file_content
    ON
        file.id = file_content.id
WHERE
    (
        # Severless serverless.yml files containing an AWS Lambda definition.
        #
        # Format 1:
        #
        # ```
        # provider:
        #   name: aws
        #   runtime: python3.8
        # ```
        #
        # This misses other basic yaml formats such as quoted strings or `{}` syntax for objects,
        # but should catch the most common case.
            file.path LIKE '%serverless.yml'
            AND REGEXP_CONTAINS(file_content.content, r'(?m:^provider:\n(( +.*)?\n)*? +name: aws)')
            AND REGEXP_CONTAINS(file_content.content, r'(?m:^provider:\n(( +.*)?\n)*? +runtime: python)')
        )
   OR (
    # Cloud Formation template.yaml files containing a serverless definition.
    #
    # Format 1:
    # ```
    # Transform: AWS::Serverless-2016-10-31
    # Resources: 
    #   <function name>:
    #     Properties:
    #       Runtime: python3.8
    # ```
        file.path LIKE '%template.yaml'
        AND REGEXP_CONTAINS(file_content.content, r'(?m:^Transform: *(\n( *- .*?\n)* *- )?AWS::Serverless)')
        AND REGEXP_CONTAINS(file_content.content, r'(?m:^ +Runtime: python)')
    )
   OR (
    # Single stage Dockerfiles from one of the following emulatoin images.
    #
    # 1. sls-docker-python (may be followed by a version number)
    # 2. aws-sam-cli-emulation-image-python (may be followed by a version number)
    # 3. aws-lambda-python
    # 4. lambci/lambda
        file.path LIKE '%Dockerfile'
        AND ARRAY_LENGTH(REGEXP_EXTRACT_ALL(file_content.content, r'(?m:^FROM)')) = 1
        AND (
                REGEXP_CONTAINS(file_content.content, r'(?m:^FROM sls-docker-python)')
                OR REGEXP_CONTAINS(file_content.content, r'(?m:^FROM amazon/aws-sam-cli-emulation-image-python)')
                OR REGEXP_CONTAINS(file_content.content, r'(?m:^FROM amazon/aws-lambda-python)')
                OR REGEXP_CONTAINS(file_content.content, r'(?m:^FROM lambci/lambda:(build-)?python)')
            )
    )
;

```
