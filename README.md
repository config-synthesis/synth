# Synth - Configuration Harmony

Synth is a tool for automatically synthesizing environment configurations.

## Dependencies

Synth is tested and known to work with the following dependencies.

| Dependency | Version  | Description                 |
|------------|----------|-----------------------------|
| Python     | 3.9.7    | Language runtime.           |
| Pipenv     | 2022.1.8 | Dependency management.      |
| Docker     | 20.10.13 | Container management.       |
| Vagrant    | 2.2.19   | Virtual machine management. |
| VirtualBox | 6.1      | Hypervisor.                 |

## Installing Synth

### Installing Locally (Recommended)

**1. Install Python Dependencies and Docker**

Install Python, Pipenv, and Docker. Verify that Docker is running and that
containers can be created by the current user without `sudo`.

**2. Clone this repository**

```text
$ git clone <repo url>
$ cd synth
```

**3. Create a Python Virtualenv and Install Python Dependencies**

```text
$ pipenv sync --dev --keep-outdated
```

**4. Run Docker Services**

```text
$ docker-compose up --detach
```

**5. Verify Docker and Synth**

```text
$ docker ps

CONTAINER ID   IMAGE                    COMMAND                  CREATED          STATUS          PORTS                                                  NAMES
74349d89e792   muccg/devpi              "/docker-entrypoint.…"   37 seconds ago   Up 35 seconds   0.0.0.0:3141->3141/tcp, :::3141->3141/tcp              synth.devpi
818613fc56bd   mbentley/apt-cacher-ng   "/entrypoint.sh s6-s…"   37 seconds ago   Up 35 seconds   0.0.0.0:3142->3142/tcp, :::3142->3142/tcp              synth.apt-cache
a727b172dacb   mysql:8                  "docker-entrypoint.s…"   37 seconds ago   Up 35 seconds   0.0.0.0:3306->3306/tcp, :::3306->3306/tcp, 33060/tcp   synth.mysql

$ synth --help
usage: synth [-h] [--verbose] [--no-vagrant] [--log-file] [--output OUTPUT] {analyze,build-image,datasets,experiments,synthesize} ...

Synthesize environment configurations.

optional arguments:
-h, --help            show this help message and exit
--verbose, -v         Verbose mode.
--no-vagrant          Always run, even if not inside the vagrant virtual machine.
--log-file            Send all output to a log file instead of to standard streams.
--output OUTPUT       Optional log file. If specified, logs will be output to the log file instead of standard streams. The path may be absolute or relative to the logging directory
`/vagrant/ignored/logs`.

synth subcommands:
These commands expose portions of Synth's functionality.

{analyze,build-image,datasets,experiments,synthesize}
Run one of these commands to get started.
analyze             Analyze a configuration script and record the results.
build-image         Build a Docker image for a serverless function that has a CloudFormation or Serverless configuration.
datasets            Work with Synth datasets.
experiments         Run Synth experiments.
synthesize          Synthesize an environment configuration.
```

### Installing on a Virtual Machine via Vagrant (Slower)

**1. Install Vagrant and Virtualbox**

Install both vagrant and virtualbox. Verify that your machine is configured to
support virtualization.

**2. Create the virtual machine**

```text
$ vagrant up
Bringing machine 'default' up with 'virtualbox' provider...
...
$ vagrant ssh
```

**3. Verify Docker and Synth**

```text
$ docker ps

CONTAINER ID   IMAGE                    COMMAND                  CREATED          STATUS          PORTS                                                  NAMES
74349d89e792   muccg/devpi              "/docker-entrypoint.…"   37 seconds ago   Up 35 seconds   0.0.0.0:3141->3141/tcp, :::3141->3141/tcp              synth.devpi
818613fc56bd   mbentley/apt-cacher-ng   "/entrypoint.sh s6-s…"   37 seconds ago   Up 35 seconds   0.0.0.0:3142->3142/tcp, :::3142->3142/tcp              synth.apt-cache
a727b172dacb   mysql:8                  "docker-entrypoint.s…"   37 seconds ago   Up 35 seconds   0.0.0.0:3306->3306/tcp, :::3306->3306/tcp, 33060/tcp   synth.mysql

$ synth --help
usage: synth [-h] [--verbose] [--no-vagrant] [--log-file] [--output OUTPUT] {analyze,build-image,datasets,experiments,synthesize} ...

Synthesize environment configurations.

optional arguments:
  -h, --help            show this help message and exit
  --verbose, -v         Verbose mode.
  --no-vagrant          Always run, even if not inside the vagrant virtual machine.
  --log-file            Send all output to a log file instead of to standard streams.
  --output OUTPUT       Optional log file. If specified, logs will be output to the log file instead of standard streams. The path may be absolute or relative to the logging directory
                        `/vagrant/ignored/logs`.

synth subcommands:
  These commands expose portions of Synth's functionality.

  {analyze,build-image,datasets,experiments,synthesize}
                        Run one of these commands to get started.
    analyze             Analyze a configuration script and record the results.
    build-image         Build a Docker image for a serverless function that has a CloudFormation or Serverless configuration.
    datasets            Work with Synth datasets.
    experiments         Run Synth experiments.
    synthesize          Synthesize an environment configuration.
```

## Running Tests

From the project directory, run:

```text
$ pytest tests
=========================== test session starts ============================
platform linux -- Python 3.9.7, pytest-6.2.5, py-1.10.0, pluggy-1.0.0
rootdir: /vagrant
collected 312 items                                                        

tests/synth/synthesis/test_classes.py .............................. [  9%]
.................................................................... [ 31%]
..........................................                           [ 44%]
tests/synth/synthesis/test_docker.py ............................... [ 54%]
.......................                                              [ 62%]
tests/synth/synthesis/test_knowledge_base.py ...........             [ 65%]
tests/synth/synthesis/test_search.py ..........                      [ 68%]
tests/synth/synthesis/test_serialization.py ...............          [ 73%]
tests/synth/synthesis/configuration_scripts/test_ansible.py ........ [ 76%]
                                                                     [ 76%]
tests/synth/synthesis/configuration_scripts/test_docker.py ......... [ 79%]
..........                                                           [ 82%]
tests/synth/synthesis/configuration_scripts/test_init.py ........... [ 85%]
                                                                     [ 85%]
tests/synth/synthesis/configuration_scripts/test_shell.py .......... [ 89%]
...........                                                          [ 92%]
tests/synth/util/test_text.py .......................                [100%]
```

## Example Synthesis

Synth contains a couple example Dockerfiles for synthesis under
`examples/configuration_scripts/docker/`. To synthesize new configuration 
scripts for them, run:

```text
# Make the synthesized example directory.
$ mkdir -p ignored/examples/

# Restore the database backup.
$ gunzip -c data/backups/backup.sql.gz | docker exec -i synth.mysql mysql -C --max-allowed-packet=1G synth

# Build an example Docker image.
$ docker build -f examples/configuration_scripts/docker/<dockerfile> -t synth/<example-number> .
$ docker run --rm synth/<example-number>
$ echo $?
0

# Run Synthesis.
$ synth -vv synthesize --system=docker --docker-image='synth/<example-number>' --docker-base-image='debian:11' > ignored/examples/Dockerfile.synthesized.<example_number>
$ docker build -f ignored/examples/Dockerfile.synthesized.<example_number> -t synth-synthesized/<example_number> .
$ docker run --rm synth-synthesized/<example_number> <default-command>
$ echo $?
0 
```
