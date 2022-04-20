# Simple Debian Dockerfiles

This dataset is a list of repos that contain Dockerfiles for Debian 11 
(bullseye). The Dockerfiles must be a single stage and cannot contain 
nonstandard syntax, environment variables or build arguments without defaults,
`ADD/COPY --chown`, `RUN --mount`, or some other complicating commands.

The dataset was generated from Google BigQuery using the following query.

```bigquery
-- Constraints for Dockerfiles:
-- 
--  1. Single stage.
--  2. Based on debian:11(\.\d+)*(-.+)? or debian:bullseye(-.+)?.
--  3. Standard syntax (I.E., no `# syntax=`).
--  4. No `COPY --from` or `COPY --chown`
--  5. No `ADD http` or `ADD --chown`. 
--  6. No `RUN --mount`, `RUN var=value command`, `RUN wget`, or `RUN curl`.
--  7. No `RUN command1 | command2`.
--  8. No `ENV` or `ARG` without defaults.
-- 
-- See Also:
-- 1. String Functions: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
-- 2. Regex Syntax: https://github.com/google/re2/wiki/Syntax



SELECT
    file.repo_name, file.ref, file.path, file.symlink_target
FROM
    `bigquery-public-data.github_repos.files` file
    JOIN 
        `bigquery-public-data.github_repos.contents` file_content
    ON 
        file.id = file_content.id
WHERE
    file.path LIKE '%Dockerfile%'
    AND ARRAY_LENGTH(REGEXP_EXTRACT_ALL(file_content.content, r'(?m:^FROM)')) = 1
    AND REGEXP_CONTAINS(file_content.content, r'(?m:^FROM debian:(?:(?:11(?:\.\d+)*)|(?:bullseye))(?:-.+)?$)')
    AND NOT REGEXP_CONTAINS(file_content.content, r'(?m:^# syntax=)')
    AND NOT REGEXP_CONTAINS(file_content.content, r'(?m:^COPY (?:(?:--from)|(?:--chown)))')
    AND NOT REGEXP_CONTAINS(file_content.content, r'(?m:^ADD (?:(?:http)|(?:--chown)))')
    AND NOT REGEXP_CONTAINS(file_content.content, r'(?m:^RUN (?:(?:--mount)|(?:[^ ]+?=[^ ]+)|(?:wget)|(?:curl)))')
    AND NOT REGEXP_CONTAINS(file_content.content, r'(?m:^RUN .*?\|)')
    AND NOT REGEXP_CONTAINS(file_content.content, r'(?m:^(?:(?:ENV)|(?:ARG)) [^=]+$)')
;
```
