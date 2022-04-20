# Simple Debian Ansible Playbooks

This dataset is a list of repos that contain Ansible playbooks for Debian 11
(bullseye). The playbooks must include tasks or roles and must have a reference
to the apt module, which is assumed to imply Debian support.

The dataset was generated from Google BigQuery using the following query.

```bigquery
-- Constraints for Playbooks:
-- 
--  1. Meant for Debian based systems (has `ansible.builtin.apt:` or `apt:`).
--  2. File contains at least one play (has keys `hosts:` and (`tasks:` or `roles:`)). Cannot be entirely composed of import_playbook.
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
    REGEXP_CONTAINS(file.path, r'^.*\.ya?ml$')
    AND REGEXP_CONTAINS(file_content.content, r'(?m:^ +(?: - )?(?:ansible\.builtin\.)?apt:)')
    AND (
        REGEXP_CONTAINS(file_content.content, r'(?m:^ +(?: - )?hosts:)')
        AND (
            REGEXP_CONTAINS(file_content.content, r'(?m:^ +(?: - )?tasks:)')
            OR REGEXP_CONTAINS(file_content.content, r'(?m:^ +(?: - )?roles:)')
        )
    )
;
```
