# Debian and Ubuntu Dockerfiles

This dataset is a list of repos that contain Dockerfiles for:

- Debian 9 (stretch)
- Debian 10 (buster)
- Debian 11 (bullseye)
- Ubuntu 14 (trusty)
- Ubuntu 16 (xenial)
- Ubuntu 18 (bionic)
- Ubuntu 20 (focal)
- Ubuntu 21 (impish)

The Dockerfiles must be a single stage and cannot contain nonstandard syntax,
`RUN --mount`, or environment variables or build arguments without defaults.

The dataset was generated from Google BigQuery using the following query.

```bigquery
-- Constraints for Dockerfiles:
-- 
--  1. Single stage.
--  2. Based on debian:(9|10|11)(\.\d+)*(-.+)?, 
--     debian:(stretch|buster|bullseye)(-.+)?, 
--     ubuntu:(14|16|18|20|21)(\.\d+)*(-.+)?,
--     or ubuntu:(trusty|xenial|bionic|focal|impish)(-.+)?.
--  3. Standard syntax (I.E., no `# syntax=`).
--  4. No `COPY --from`.
--  5. No `RUN --mount`.
--  6. No `ENV` or `ARG` without defaults.
-- 
-- See Also:
-- 1. String Functions: https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions
-- 2. Regex Syntax: https://github.com/google/re2/wiki/Syntax



SELECT
    file.repo_name,
    file.ref,
    file.path,
    file.symlink_target,
    REGEXP_EXTRACT(file_content.content, r'(?m:^FROM ((?:debian:(?:(?:(?:9|10|11)(?:\.\d+)*)|(?:stretch|buster|bullseye))(?:-.+)?)|(?:ubuntu:(?:(?:(?:14|16|18|20|21)(?:\.\d+)*)|(?:trusty|xenial|bionic|focal|impish))(?:-.+)?))$)') as docker_base_image
FROM
    `bigquery-public-data.github_repos.files` file
        JOIN
    `bigquery-public-data.github_repos.contents` file_content
    ON
        file.id = file_content.id
WHERE
    file.path LIKE '%Dockerfile%'
    AND ARRAY_LENGTH(REGEXP_EXTRACT_ALL(file_content.content, r'(?m:^FROM)')) = 1
    AND REGEXP_CONTAINS(file_content.content, r'(?m:^FROM (?:(?:debian:(?:(?:(?:9|10|11)(?:\.\d+)*)|(?:stretch|buster|bullseye))(?:-.+)?)|(?:ubuntu:(?:(?:(?:14|16|18|20|21)(?:\.\d+)*)|(?:trusty|xenial|bionic|focal|impish))(?:-.+)?))$)')
    AND NOT REGEXP_CONTAINS(file_content.content, r'(?m:^# syntax=)')
    AND NOT REGEXP_CONTAINS(file_content.content, r'(?m:^COPY --from)')
    AND NOT REGEXP_CONTAINS(file_content.content, r'(?m:^RUN --mount)')
    AND NOT REGEXP_CONTAINS(file_content.content, r'(?m:^(?:(?:ENV)|(?:ARG)) [^=]+$)')
;
```

The deduplicated dataset uses a simple heuristic to exclude duplicated 
Dockerfiles caused by forks from the dataset. Repo names (excluding the owner)
and Dockerfile paths must be unique. In the event that there is a collision,
we pick one to preserve in the dataset.

The sampled dataset is a random sample of 50% of the deduplicated Dockerfiles.
