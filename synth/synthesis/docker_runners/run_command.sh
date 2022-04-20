#!/usr/bin/env bash


# Run a command.
#
# Before and after environment metadata will be recorded unless the environment
# variable SYNTH_NO_METADATA is set.
#
# If the environment variable PLAYBOOK is defined, its value will be written
# to the file playbook.yml before running the command. The playbook file is
# removed by the cleanup script.
#
# Usage ./run_command.sh <cmd>


# Exit if no command was provided.
if [[ -z "$@" ]]; then
  echo 'A command must be provided.'
  exit 1
fi


# Define script variables.
VALIDATION_DIR="${VALIDATION_DIR:+"$(realpath $VALIDATION_DIR)"}"
VALIDATION_DIR="${VALIDATION_DIR:-/validation}"
PRE_DIR="$VALIDATION_DIR/pre"
POST_DIR="$VALIDATION_DIR/post"


# Define function for getting metadata about the environment.
get_metadata() {

  if [[ -z "$1" ]]; then
    echo "Must provide an output directory."
    exit 1
  fi

  out_dir="$1"

  # Running processes.
  ps -e -o args | tail -n +2 | sort > "$out_dir/proc"

  # Working directory.
  pwd > "$out_dir/cwd"

  # Environment variables.
  env | sort > "$out_dir/env"

  # Systemd services.
  if [[ $(command -v systemctl) && $(systemctl is-system-running) != 'offline' ]]; then
    systemctl list-units --type=service | head -n -6 | \
        awk '{for(i=1; i<=4; i++) printf $i ","; for(i=5; i<=NF; i++) printf "%s%s", $i, (i<NF ? OFS : ORS)}' \
        > "$out_dir/services"
  fi

}


# If a playbook is provided, write the playbook file.
# This must happen before recording pre-execution metadata so that PLAYBOOK
# is properly unset.
if [[ -n "$PLAYBOOK" ]]; then
  echo "$PLAYBOOK" > playbook.yml
  unset PLAYBOOK
fi


# Pre-execution metadata.
if [[ ! -v SYNTH_NO_METADATA ]]; then
  # Make all validation directories.
  mkdir -p "$PRE_DIR"
  mkdir -p "$POST_DIR"

  # Get pre-execution environment metadata.
  get_metadata "$PRE_DIR"
fi


# Execute the command and save the exit code.
# If the command does not exist, provide a standardized output. Different
# shells often have different syntax.
command_parts=($1)
if command -v "${command_parts[0]}" > /dev/null; then
  eval "$@"
  exit="$?"
else
  >&2 echo "${command_parts[0]}: command not found"
  exit=127
fi

# Perform cleanup for configuration systems.
/scripts/cleanup.sh


# Get post-execution environment metadata.
if [[ ! -v SYNTH_NO_METADATA ]]; then
  get_metadata "$POST_DIR"
fi


# Exit with the same exit code as the command being validated.
exit "$exit"
