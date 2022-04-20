#!/bin/bash


# Set Terraform environment variables corresponding to the DigitalOcean
# authentication variables.
# Usage: `source ./setauth.sh`.


# Because ZSH supports prompts with ? instead of -p.
if [[ "$(ps -c -o command= -p $$)" == 'zsh' ]]
then

  read -s 'TF_VAR_do_token?DigitalOcean Access Token:  '
  export TF_VAR_do_token

else

  read -p "DigitalOcean Access Token: " -s TF_VAR_do_token
  export TF_VAR_do_token

fi

echo
