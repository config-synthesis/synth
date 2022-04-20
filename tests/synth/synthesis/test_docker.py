"""Tests for synthesis Docker utilities."""


# Imports.
import json
from collections.abc import Generator
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from typing import ContextManager, Type
from unittest.mock import call, Mock, patch

import pytest
import sh
from docker.models.images import Image

from synth.synthesis import docker
from synth.synthesis.classes import (
    ConfigurationSystem,
    ConfigurationTask,
    ConfigurationTaskArgument,
    DirectoryAdd,
    DirectoryDelete,
    EnvSet,
    EnvUnset,
    FileAdd,
    FileChange,
    FileContentChange,
    FileContentChangeType,
    FileDelete,
    frozendict,
    ServiceStart,
    ServiceStop,
    WorkingDirectorySet,
)
from synth.synthesis.configuration_scripts.classes import ParseResult
from synth.synthesis.docker import (
    ANALYSIS_TIMEOUT,
    analyze_and_record,
    analyze_configuration_script,
    AnsibleTaskError,
    AnsibleTaskRunner,
    cleanup_images,
    ConfigurationTaskRunner,
    diff_files,
    diff_images,
    DockerRunContext,
    get_runner,
    RunResult,
    ShellTaskError,
    ShellTaskRunner,
)


class TestGetRunner:
    """Tests for ``get_runner``."""

    def test_get_shell(self):
        """Verify a shell runner is returned."""
        assert isinstance(get_runner('shell'), ShellTaskRunner)

    def test_get_ansible(self):
        """Verify a NotImplementedError is raised."""
        assert isinstance(get_runner('ansible'), AnsibleTaskRunner)

    def test_get_unknown(self):
        """Verify a ValueError is raised for an unknown system."""
        with pytest.raises(ValueError):
            get_runner('a runner that does not exist')


class TestDockerRunContext:
    """Tests for ``DockerRunContext``."""

    class Context(DockerRunContext):
        """A test context implementation."""

        pass

    @pytest.fixture
    def ctx_class(self) -> Type[DockerRunContext]:
        """Context class for testing."""
        return self.Context

    @pytest.fixture
    def ctx(self, ctx_class: Type[DockerRunContext]) -> DockerRunContext:
        """Context for testing.."""
        return ctx_class()

    class TestInit:
        """Tests for ``DockerRunContext.__init__``."""

        def test_overrides_run_kwargs(self, ctx_class: Type[DockerRunContext]):
            """Verify default run kwargs are overridden."""
            image = 'image'
            other_key = 'other_key'
            runner = ctx_class(
                image=image,
                other_key=other_key
            )

            assert 'image' in runner.run_kwargs
            assert runner.run_kwargs['image'] == image

            assert 'other_key' in runner.run_kwargs
            assert runner.run_kwargs['other_key'] == other_key

            defaults = [
                (key, value)
                for key, value in DockerRunContext._run_defaults
                if key != 'image' and key != 'other_key'
            ]

            for key, value in defaults:
                assert key in runner.run_kwargs
                assert runner.run_kwargs[key] == value

    class TestEnter:
        """Tests for ``__enter__``."""

        def test_runs_container(self,
                                ctx: DockerRunContext,
                                docker_client: Mock):
            """Verify enter runs a container with the run args."""
            ctx.__enter__()

            docker_client.containers.run.assert_called_with(**ctx.run_kwargs)

    class TestExit:
        """Tests for ``__exit__``."""

        @pytest.fixture(autouse=True)
        def enter(self, ctx: DockerRunContext):
            """Enter and exit the context for testing."""
            ctx.__enter__()
            ctx.__exit__(None, None, None)

        def test_stops_container(self, ctx: DockerRunContext):
            """Verify the container is stopped on exit."""
            ctx.container.stop.assert_called()

        def test_removes_container(self, ctx: DockerRunContext):
            """Verify the container is removed on exit."""
            ctx.container.remove.assert_called()

        def test_closes_client(self, ctx: DockerRunContext):
            """Verify the client is closed on exit."""
            ctx.client.close.assert_called()


class TestConfigurationTaskRunner:
    """Tests for ``ConfigurationTaskRunner``."""

    @pytest.fixture(autouse=True)
    def reset_builds(self):
        """Reset the built image flag."""
        AnsibleTaskRunner._built_image = False

    class TestNew:
        """Tests for ``ConfigurationTaskRunner.__new__``."""

        def test_builds_image(self):
            """Verify build image is invoked on creation."""
            with patch.object(AnsibleTaskRunner, 'build_runner_image') as mock:
                AnsibleTaskRunner.__new__(AnsibleTaskRunner)
                mock.assert_called_once()

    class TestInit:
        """Tests for ``ConfigurationTaskRunner.__init__``."""

        def test_overrides_image(self):
            """Verify image is overridden if the build context is set."""
            runner = AnsibleTaskRunner()

            image = runner.run_kwargs['image']
            assert image == AnsibleTaskRunner.build_context.tag

    class TestBuildRunnerImage:
        """Tests for ``ConfigurationTaskRunner.build_runner_image``."""

        def test_noop_with_no_build_context(self, docker_client: Mock):
            """Verify no actions if the class does not have a build context."""
            class Runner(ConfigurationTaskRunner):
                """Runner class for testing."""

                def run_task(
                        self,
                        task: ConfigurationTask,
                        arguments:
                        frozenset[ConfigurationTaskArgument] = frozenset(),
                ) -> RunResult:
                    """Fake running a task."""
                    return ...

            Runner.build_runner_image()
            docker_client.images.build.assert_not_called()

        def test_builds_once(self, docker_client: Mock):
            """Verify that the image is only built once."""
            AnsibleTaskRunner.build_runner_image()
            AnsibleTaskRunner.build_runner_image()

            docker_client.images.build.assert_called_once()

        def test_runs_build(self, docker_client: Mock):
            """Verify build is called with the correct arguments."""
            AnsibleTaskRunner.build_runner_image()

            docker_client.images.build.assert_called_once_with(
                path=str(AnsibleTaskRunner.build_context.context_dir),
                dockerfile=str(AnsibleTaskRunner.build_context.dockerfile),
                tag=AnsibleTaskRunner.build_context.tag,
                network_mode='synth_default',
            )


class TestAnsibleTaskRunner:
    """Tests for ``AnsibleTaskRunner``."""

    @pytest.fixture
    def runner(self) -> Generator[AnsibleTaskRunner, None, None]:
        """Create a runner for testing."""
        with AnsibleTaskRunner() as runner:
            yield runner

    @pytest.fixture
    def task(self) -> ConfigurationTask:
        """Create a task for testing."""
        return ConfigurationTask(
            system=ConfigurationSystem.ANSIBLE,
            executable='ansible.builtin.file',
            arguments=frozendict({
                'name': 'file.txt',
                'state': 'touch',
            }),
            changes=frozenset(),
        )

    class TestRunTask:
        """Tests for ``AnsibleTaskRunner.run_task``."""

        def test_no_stdout(self,
                           task: ConfigurationTask,
                           runner: AnsibleTaskRunner):
            """Verify missing stdout is replaced with empty string."""
            runner.container.exec_run.return_value = (0, (None, b''))

            result = runner.run_task(task)

            assert result.stdout == ''

        def test_no_stderr(self,
                           task: ConfigurationTask,
                           runner: AnsibleTaskRunner):
            """Verify missing stderr is replaced with empty string."""
            runner.container.exec_run.return_value = (0, (b'', None))

            result = runner.run_task(task)

            assert result.stderr == ''

        def test_returns_result(self,
                                task: ConfigurationTask,
                                runner: AnsibleTaskRunner):
            """Verify a result is correctly returned."""
            exit_code = 0
            stdout = 'stdout'
            stderr = 'stderr'
            runner.container.exec_run.return_value = (
                exit_code, (stdout.encode(), stderr.encode())
            )

            result = runner.run_task(task)

            assert result.exit_code == exit_code
            assert result.stdout == stdout
            assert result.stderr == stderr

        def test_raises_ansible_task_error(self,
                                           task: ConfigurationTask,
                                           runner: AnsibleTaskRunner):
            """Verify an AnsibleTaskError is raised on error."""
            output = {
                'plays': [{
                    'tasks': [
                        {},  # Gather facts task.
                        {
                            'hosts': {
                                'localhost': {
                                    'changed': False,
                                    'msg': 'error',
                                },
                            },
                        },
                    ],
                }],
            }
            runner.container.exec_run.return_value = (
                1,
                (
                    json.dumps(output).encode(),
                    b'',
                ),
            )

            with pytest.raises(AnsibleTaskError):
                runner.run_task(task)

        def test_runs_with_timeout(self,
                                   task: ConfigurationTask,
                                   runner: AnsibleTaskRunner):
            """Verify a timeout error is raised."""
            runner.container.exec_run.side_effect = (
                lambda *args, **kwargs: sleep(10)
            )

            with pytest.raises(TimeoutError):
                runner.run_task(task, timeout=1)


class TestShellTaskRunner:
    """Tests for ``ShellTaskRunner``."""

    @pytest.fixture
    def runner(self) -> Generator[ShellTaskRunner, None, None]:
        """Create a runner for testing."""
        with ShellTaskRunner() as runner:
            yield runner

    @pytest.fixture
    def task(self) -> ConfigurationTask:
        """Create a task for testing."""
        return ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='touch',
            arguments=('file.txt',),
            changes=frozenset(),
        )

    class TestRunTask:
        """Tests for ``ShellTaskRunner.run_task``."""

        def test_no_stdout(self,
                           task: ConfigurationTask,
                           runner: ShellTaskRunner):
            """Verify missing stdout is replaced with empty string."""
            runner.container.exec_run.return_value = (0, (None, b''))

            result = runner.run_task(task)

            assert result.stdout == ''

        def test_no_stderr(self,
                           task: ConfigurationTask,
                           runner: ShellTaskRunner):
            """Verify missing stderr is replaced with empty string."""
            runner.container.exec_run.return_value = (0, (b'', None))

            result = runner.run_task(task)

            assert result.stderr == ''

        def test_returns_result(self,
                                task: ConfigurationTask,
                                runner: ShellTaskRunner):
            """Verify a result is correctly returned."""
            exit_code = 0
            stdout = 'stdout'
            stderr = 'stderr'
            runner.container.exec_run.return_value = (
                exit_code, (stdout.encode(), stderr.encode())
            )

            result = runner.run_task(task)

            assert result.exit_code == exit_code
            assert result.stdout == stdout
            assert result.stderr == stderr

        def test_raises_shell_task_error(self,
                                         task: ConfigurationTask,
                                         runner: ShellTaskRunner):
            """Verify a ShellTaskError is raised on error."""
            runner.container.exec_run.return_value = (1, (b'', b'error'))

            with pytest.raises(ShellTaskError):
                runner.run_task(task)

        def test_runs_with_timeout(self,
                                   task: ConfigurationTask,
                                   runner: AnsibleTaskRunner):
            """Verify a timeout error is raised."""
            runner.container.exec_run.side_effect = (
                lambda *args, **kwargs: sleep(10)
            )

            with pytest.raises(TimeoutError):
                runner.run_task(task, timeout=1)


class TestCleanupImages:
    """Tests for ``cleanup_images``."""

    class TestEnter:
        """Tests for ``cleanup_images.__enter__``."""

        def test_raises_value_error_without_labels(self, docker_client: Mock):
            """Verify ValueError is raised if no labels are provided."""
            with pytest.raises(ValueError):
                ctx = cleanup_images(docker_client, {})
                ctx.__enter__()

    class TestExit:
        """Tests for ``cleanup_images.__exit__``."""

        @pytest.fixture
        def labels(self) -> dict[str, str]:
            """Create labels for testing."""
            return {
                'test label': 'test value',
            }

        @pytest.fixture
        def ctx(self,
                docker_client: Mock,
                labels: dict[str, str]) -> ContextManager:
            """Create a context manager for testing."""
            ctx = cleanup_images(docker_client, labels)
            ctx.__enter__()
            return ctx

        def test_removes_all_images(self,
                                    docker_client: Mock,
                                    ctx: ContextManager,
                                    labels: dict[str, str]):
            """Verify all images are removed on exit."""
            ids = ['1', '2', '3']
            images = [
                Image(attrs={'Id': image_id, 'Tags': [f'tag-{image_id}']})
                for image_id in ids
            ]
            docker_client.api.images.return_value = ids
            docker_client.images.get.side_effect = images

            ctx.__exit__(None, None, None)

            docker_client.api.images.assert_called_once_with(
                quiet=True,
                filters={
                    'label': [
                        f'{key}={value}'
                        for key, value in labels.items()
                    ],
                },
            )
            docker_client.images.get.assert_has_calls(
                [call(image_id) for image_id in ids]
            )
            docker_client.images.remove.assert_has_calls(
                [call(image_id) for image_id in ids]
            )

        def test_noop_if_no_images(self,
                                   docker_client: Mock,
                                   ctx: ContextManager):
            """Verify no images are removed if no ids are returned."""
            docker_client.api.images.return_value = []

            ctx.__exit__(None, None, None)

            docker_client.images.get.assert_not_called()
            docker_client.images.remove.assert_not_called()


class TestDiffFiles:
    """Tests for ``diff_files``."""

    @pytest.fixture
    def image1_cache(self) -> Generator[Path, None, None]:
        """Create a cache directory for testing."""
        with TemporaryDirectory() as path:
            yield Path(path)

    @pytest.fixture
    def image2_cache(self) -> Generator[Path, None, None]:
        """Create a cache directory for testing."""
        with TemporaryDirectory() as path:
            yield Path(path)

    @pytest.fixture
    def file(self) -> Path:
        """Create a file path for testing."""
        return Path('/file.txt')

    @pytest.fixture
    def image1_file(self, image1_cache: Path, file: Path) -> Path:
        """Create a file path in the image 1 cache for testing."""
        return image1_cache / file.relative_to('/')

    @pytest.fixture
    def image2_file(self, image2_cache: Path, file: Path) -> Path:
        """Create a file path in the image 2 cache for testing."""
        return image2_cache / file.relative_to('/')

    def test_raises_if_image1_cache_is_not_dir(self,
                                               image1_cache: Path,
                                               image2_cache: Path,
                                               file: Path):
        """Verify a ValueError is raised if the cache path is not a dir."""
        image1_cache.rmdir()

        with pytest.raises(ValueError):
            diff_files(image1_cache, image2_cache, file)

    def test_raises_if_image2_cache_is_not_dir(self,
                                               image1_cache: Path,
                                               image2_cache: Path,
                                               file: Path):
        """Verify a ValueError is raised if the cache path is not a dir."""
        image2_cache.rmdir()

        with pytest.raises(ValueError):
            diff_files(image1_cache, image2_cache, file)

    def test_raises_if_file_does_not_exist_in_either_cache(self,
                                                           image1_cache: Path,
                                                           image2_cache: Path,
                                                           file: Path):
        """Verify a ValueError is raised if the file does not exist at all."""
        with pytest.raises(ValueError):
            diff_files(image1_cache, image2_cache, file)

    def test_image1_file_does_not_exist(self,
                                        image1_cache: Path,
                                        image2_cache: Path,
                                        file: Path,
                                        image2_file: Path):
        """Verify a diff is generated if the file doesn't exist in image 1."""
        lines = ['a\n', 'b\n', 'c\n']
        with open(image2_file, 'w') as fd:
            fd.writelines(lines)

        change = diff_files(image1_cache, image2_cache, file)

        assert change == FileChange.from_primitives(
            path=str(file),
            changes=frozenset({
                FileContentChange.from_primitives(
                    change_type=FileContentChangeType.ADDITION,
                    content=''.join(lines),
                ),
            }),
        )

    def test_image2_file_does_not_exist(self,
                                        image1_cache: Path,
                                        image2_cache: Path,
                                        file: Path,
                                        image1_file: Path):
        """Verify a diff is generated if the file doesn't exist in image 2."""
        lines = ['a\n', 'b\n', 'c\n']
        with open(image1_file, 'w') as fd:
            fd.writelines(lines)

        change = diff_files(image1_cache, image2_cache, file)

        assert change == FileChange.from_primitives(
            path=str(file),
            changes=frozenset({
                FileContentChange.from_primitives(
                    change_type=FileContentChangeType.DELETION,
                    content=''.join(lines),
                ),
            }),
        )

    def test_file_diff(self,
                       image1_cache: Path,
                       image2_cache: Path,
                       file: Path,
                       image1_file: Path,
                       image2_file: Path):
        """Verify a diff is generated if the file exists in both images."""
        i1_lines = ['a\n', 'b\n', 'c\n']
        with open(image1_file, 'w') as fd:
            fd.writelines(i1_lines)

        i2_lines = ['a\n', '1\n', 'c\n']
        with open(image2_file, 'w') as fd:
            fd.writelines(i2_lines)

        change = diff_files(image1_cache, image2_cache, file)

        assert change == FileChange.from_primitives(
            path=str(file),
            changes=frozenset({
                FileContentChange.from_primitives(
                    change_type=FileContentChangeType.DELETION,
                    content='b\n',
                ),
                FileContentChange.from_primitives(
                    change_type=FileContentChangeType.ADDITION,
                    content='1\n',
                ),
            }),
        )


class TestDiffImages:
    """Tests for ``diff_images``."""

    @pytest.fixture(autouse=True)
    def container_diff(self) -> Generator[Mock, None, None]:
        """Mock the container diff command for testing."""
        with patch.object(sh, 'container_diff') as mock:
            yield mock

    @pytest.fixture(autouse=True)
    def container_diff_stdout(self, container_diff: Mock):
        """Mock no changes for container-diff by default."""
        diff = json.dumps([{
            'Diff': {
                'Adds': [],
                'Dels': [],
                'Mods': [],
            },
        }])
        container_diff.return_value.stdout = diff

    @pytest.fixture
    def image1(self) -> str:
        """Get an image1 name for testing."""
        return 'image1'

    @pytest.fixture
    def image2(self) -> str:
        """Get an image2 name for testing."""
        return 'image2'

    @pytest.fixture(autouse=True)
    def cache_dir(self) -> Generator[str, None, None]:
        """Get a cache temporary directory for testing."""
        with TemporaryDirectory() as path:
            yield path

    @pytest.fixture(autouse=True)
    def image1_cache_dir(self, cache_dir: str, image1: str) -> Path:
        """Create the image1 cache dir for testing."""
        path = Path(cache_dir) / f'.container-diff/cache/daemon_{image1}'
        path.mkdir(exist_ok=True, parents=True)
        return path

    @pytest.fixture(autouse=True)
    def image2_cache_dir(self, cache_dir: str, image2: str) -> Path:
        """Create the image2 cache dir for testing."""
        path = Path(cache_dir) / f'.container-diff/cache/daemon_{image2}'
        path.mkdir(exist_ok=True, parents=True)
        return path

    @pytest.fixture(autouse=True)
    def image2_validation_files(self,
                                image2_cache_dir: str) -> dict[str, Path]:
        """Generate validation files for testing."""
        validation_dir = Path(image2_cache_dir) / 'validation'
        validation_dir.mkdir()

        paths = {}
        for dir_type in ('pre', 'post'):

            dir_path = validation_dir / dir_type
            dir_path.mkdir()

            cwd_path = dir_path / 'cwd'
            cwd_path.touch()
            paths[f'cwd_{dir_type}'] = cwd_path

            env_path = dir_path / 'env'
            env_path.touch()
            paths[f'env_{dir_type}'] = env_path

            services_path = dir_path / 'services'
            services_path.touch()
            paths[f'services_{dir_type}'] = services_path

        return paths

    def test_executes_container_diff(self,
                                     image1: str,
                                     image2: str,
                                     cache_dir: str,
                                     container_diff: Mock):
        """Verify the correct container-diff command is executed."""
        diff_images(image1, image2, cache_dir)

        container_diff.assert_called()

    def test_creates_temporary_directory(self,
                                         image1: str,
                                         image2: str):
        """Verify a temporary directory is created if no cache is provided."""
        with patch.object(docker, 'TemporaryDirectory') as mock:
            diff_images(image1, image2)
            mock.assert_called()

    def test_excludes_null_diff_items(self,
                                      image1: str,
                                      image2: str,
                                      cache_dir: str,
                                      container_diff: Mock):
        """Verify diff items that are null are excluded from the results."""
        diff = json.dumps([{
            'Diff': {
                'Adds': None,
                'Dels': None,
                'Mods': None,
            },
        }])
        container_diff.return_value.stdout = diff

        changes = diff_images(image1, image2, cache_dir)

        assert changes == set()

    def test_excludes_standard_ignored_paths(self,
                                             image1: str,
                                             image2: str,
                                             cache_dir: str,
                                             container_diff: Mock):
        """Verify paths with prefixes in _IMAGE_DIFF_EXCLUDES are ignored."""
        diff = json.dumps([{
            'Diff': {
                'Adds': [
                    {
                        'Name': '/tmp/systemd-private-dir/test',  # noqa: S108
                    },
                    {
                        'Name': '/validation/test',
                    },
                    {
                        'Name': '/var/log/test',
                    },
                    {
                        'Name': '/var/tmp/systemd-private-dir'  # noqa: S108
                                '/test'
                    },
                ],
                'Dels': None,
                'Mods': None,
            },
        }])
        container_diff.return_value.stdout = diff

        changes = diff_images(image1, image2, cache_dir)

        assert changes == set()

    def test_text_file_adds(self,
                            image1: str,
                            image1_cache_dir: str,
                            image2: str,
                            image2_cache_dir: str,
                            cache_dir: str,
                            container_diff: Mock):
        """Verify text files get a FileAdd and FileChange."""
        diff = json.dumps([{
            'Diff': {
                'Adds': [
                    {
                        'Name': '/file.txt',
                    },
                ],
                'Dels': None,
                'Mods': None,
            },
        }])
        container_diff.return_value.stdout = diff

        with open(f'{image2_cache_dir}/file.txt', 'w') as fd:
            fd.write('file contents\n')

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            FileAdd.from_primitives(
                path='/file.txt',
            ),
            FileChange.from_primitives(
                path='/file.txt',
                changes=frozenset({
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='file contents\n',
                    ),
                }),
            ),
        }

    def test_binary_file_adds(self,
                              image1: str,
                              image1_cache_dir: str,
                              image2: str,
                              image2_cache_dir: str,
                              cache_dir: str,
                              container_diff: Mock):
        """Verify binary files get a FileAdd."""
        diff = json.dumps([{
            'Diff': {
                'Adds': [
                    {
                        'Name': '/bin/file',
                    },
                ],
                'Dels': None,
                'Mods': None,
            },
        }])
        container_diff.return_value.stdout = diff

        (Path(image2_cache_dir) / 'bin').mkdir()
        with open(f'{image2_cache_dir}/bin/file', 'wb') as fd:
            fd.write(b'\xff')

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            FileAdd.from_primitives(
                path='/bin/file',
            ),
        }

    def test_directory_adds(self,
                            image1: str,
                            image1_cache_dir: str,
                            image2: str,
                            image2_cache_dir: str,
                            cache_dir: str,
                            container_diff: Mock):
        """Verify directories get a DirectoryAdd."""
        diff = json.dumps([{
            'Diff': {
                'Adds': [
                    {
                        'Name': '/dir',
                    },
                ],
                'Dels': None,
                'Mods': None,
            },
        }])
        container_diff.return_value.stdout = diff

        (Path(image2_cache_dir) / 'dir').mkdir(exist_ok=True, parents=True)

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            DirectoryAdd.from_primitives(
                path='/dir',
            ),
        }

    def test_file_deletes(self,
                          image1: str,
                          image1_cache_dir: str,
                          image2: str,
                          image2_cache_dir: str,
                          cache_dir: str,
                          container_diff: Mock):
        """Verify all files get a FileDelete."""
        diff = json.dumps([{
            'Diff': {
                'Adds': None,
                'Dels': [
                    {
                        'Name': '/file.txt',
                    },
                ],
                'Mods': None,
            },
        }])
        container_diff.return_value.stdout = diff

        with open(f'{image1_cache_dir}/file.txt', 'w') as fd:
            fd.write('file contents\n')

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            FileDelete.from_primitives(
                path='/file.txt',
            ),
        }

    def test_directory_delete(self,
                              image1: str,
                              image1_cache_dir: str,
                              image2: str,
                              image2_cache_dir: str,
                              cache_dir: str,
                              container_diff: Mock):
        """Verify directories get a DirectoryDelete."""
        diff = json.dumps([{
            'Diff': {
                'Adds': None,
                'Dels': [
                    {
                        'Name': '/dir',
                    },
                ],
                'Mods': None,
            },
        }])
        container_diff.return_value.stdout = diff

        (Path(image1_cache_dir) / 'dir').mkdir(exist_ok=True, parents=True)

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            DirectoryDelete.from_primitives(
                path='/dir',
            ),
        }

    def test_text_file_changes(self,
                               image1: str,
                               image1_cache_dir: str,
                               image2: str,
                               image2_cache_dir: str,
                               cache_dir: str,
                               container_diff: Mock):
        """Verify text files get a FileChange."""
        diff = json.dumps([{
            'Diff': {
                'Adds': None,
                'Dels': None,
                'Mods': [
                    {
                        'Name': '/dir/file.txt',
                    },
                ],
            },
        }])
        container_diff.return_value.stdout = diff

        i1_file_dir = Path(image1_cache_dir) / 'dir'
        i2_file_dir = Path(image2_cache_dir) / 'dir'

        i1_file_dir.mkdir()
        i2_file_dir.mkdir()

        i1_file = i1_file_dir / 'file.txt'
        with open(i1_file, 'w') as fd:
            fd.write('start value')

        i2_file = i2_file_dir / 'file.txt'
        with open(i2_file, 'w') as fd:
            fd.write('end value')

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            FileChange.from_primitives(
                path='/dir/file.txt',
                changes=frozenset({
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.DELETION,
                        content='start value',
                    ),
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='end value',
                    ),
                }),
            )
        }

    def test_binary_file_changes(self,
                                 image1: str,
                                 image1_cache_dir: str,
                                 image2: str,
                                 image2_cache_dir: str,
                                 cache_dir: str,
                                 container_diff: Mock):
        """Verify binary file changes get a file delete and file add."""
        diff = json.dumps([{
            'Diff': {
                'Adds': None,
                'Dels': None,
                'Mods': [
                    {
                        'Name': '/bin/file',
                    },
                ],
            },
        }])
        container_diff.return_value.stdout = diff

        i1_file_dir = Path(image1_cache_dir) / 'bin'
        i2_file_dir = Path(image2_cache_dir) / 'bin'

        i1_file_dir.mkdir()
        i2_file_dir.mkdir()

        i1_file = i1_file_dir / 'file'
        with open(i1_file, 'wb') as fd:
            fd.write(b'\xff')

        i2_file = i2_file_dir / 'file'
        with open(i2_file, 'wb') as fd:
            fd.write(b'\x00')

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            FileDelete.from_primitives(path='/bin/file'),
            FileAdd.from_primitives(path='/bin/file'),
        }

    def test_set_cwd(self,
                     image1: str,
                     image2: str,
                     cache_dir: str,
                     image2_validation_files: dict[str, Path]):
        """Verify a change in cwd gets a SetWorkingDirectory."""
        cwd_pre = image2_validation_files['cwd_pre']
        cwd_post = image2_validation_files['cwd_post']

        cwd_pre.write_text('dir1')
        cwd_post.write_text('dir2')

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            WorkingDirectorySet.from_primitives(path='dir2'),
        }

    def test_set_env(self,
                     image1: str,
                     image2: str,
                     cache_dir: str,
                     image2_validation_files: dict[str, Path]):
        """Verify a new env var gets a SetEnv."""
        env_post = image2_validation_files['env_post']
        env_post.write_text('env1=value1\nenv2=value2')

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            EnvSet.from_primitives(
                key='env1',
                value='value1',
            ),
            EnvSet.from_primitives(
                key='env2',
                value='value2',
            ),
        }

    def test_unset_env(self,
                       image1: str,
                       image2: str,
                       cache_dir: str,
                       image2_validation_files: dict[str, Path]):
        """Verify unsetting an env gets an UnsetEnv."""
        env_pre = image2_validation_files['env_pre']
        env_pre.write_text('env1=value1\nenv2=value2')

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            EnvUnset.from_primitives(
                key='env1',
            ),
            EnvUnset.from_primitives(
                key='env2',
            ),
        }

    def test_start_service(self,
                           image1: str,
                           image2: str,
                           cache_dir: str,
                           image2_validation_files: dict[str, Path]):
        """Verify a service start gets a ServiceStart."""
        services_pre = image2_validation_files['services_pre']
        services_pre.write_text(
            'UNIT,ACTIVE\n'
        )

        services_post = image2_validation_files['services_post']
        services_post.write_text(
            'UNIT,ACTIVE\n'
            'service-name,active\n'
        )

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            ServiceStart.from_primitives(name='service-name'),
        }

    def test_stop_service(self,
                          image1: str,
                          image2: str,
                          cache_dir: str,
                          image2_validation_files: dict[str, Path]):
        """Verify a service stop gets a ServiceStop."""
        services_pre = image2_validation_files['services_pre']
        services_pre.write_text(
            'UNIT,ACTIVE\n'
            'service-name,active\n'
        )

        services_post = image2_validation_files['services_post']
        services_post.write_text(
            'UNIT,ACTIVE\n'
        )

        changes = diff_images(image1, image2, cache_dir)

        assert changes == {
            ServiceStop.from_primitives(name='service-name'),
        }


class TestAnalyzeConfigurationScript:
    """Tests for ``analyze_configuration_script``."""

    @pytest.fixture(autouse=True)
    def runner(self) -> Generator[Mock, None, None]:
        """Patch ``run_task`` for testing."""
        with patch.object(docker, 'get_runner') as mock:
            # The search process uses the runner returned by
            # get_runner().__enter__(). Note that the mock __enter__ isn't
            # configured to return the same object (like the actual
            # implementation) so that tests can verify calls without also
            # checking for __enter__ and __exit__.
            yield mock().__enter__()

    @pytest.fixture(autouse=True)
    def container_diff(self) -> Generator[Mock, None, None]:
        """Mock the container diff process."""
        with patch.object(sh, 'container_diff') as mock:
            diff = json.dumps([{
                'Diff': {
                    'Adds': [],
                    'Dels': [],
                    'Mods': [],
                },
            }])
            mock.return_value.stdout = diff
            yield mock

    @pytest.fixture
    def context(self) -> Path:
        """Create a context directory for testing."""
        return Path()

    @pytest.fixture
    def task1(self) -> ConfigurationTask:
        """Create a task for testing."""
        return ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='exe1',
            arguments=(),
            changes=frozenset(),
        )

    @pytest.fixture
    def task2(self) -> ConfigurationTask:
        """Create a task for testing."""
        return ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='exe2',
            arguments=(),
            changes=frozenset(),
        )

    @pytest.fixture
    def error(self) -> ShellTaskError:
        """Create an error for testing."""
        return ShellTaskError.from_primitives(
            exit_code=1,
            stdout='',
            stderr='error'
        )

    def test_no_tasks(self, context: Path):
        """Verify no tasks results in the empty set."""
        result = ParseResult()

        analysis_result = analyze_configuration_script(context, result)

        assert analysis_result.success
        assert analysis_result.tasks == frozenset()
        assert not analysis_result.failed_at_task
        assert not analysis_result.configuration_task_error

    def test_error(self,
                   runner: Mock,
                   context: Path,
                   task1: ConfigurationTask,
                   error: ShellTaskError):
        """Verify an unresolved error is handled correctly."""
        runner.run_task.side_effect = [error] * 2

        result = ParseResult(tasks=[task1])

        analysis_result = analyze_configuration_script(context, result)

        assert not analysis_result.success
        assert analysis_result.tasks == frozenset()
        assert analysis_result.failed_at_task == task1
        assert analysis_result.configuration_task_error == error
        runner.run_task.assert_has_calls(
            [call(task1, timeout=ANALYSIS_TIMEOUT)] * 2
        )

    def test_success(self,
                     runner: Mock,
                     context: Path,
                     task1: ConfigurationTask):
        """Verify all results are returned on success."""
        result = ParseResult(tasks=[task1])

        analysis_result = analyze_configuration_script(context, result)

        assert analysis_result.success
        assert analysis_result.tasks == frozenset({
            (None, task1, None, None),
        })
        assert not analysis_result.failed_at_task
        assert not analysis_result.configuration_task_error
        runner.run_task.assert_has_calls(
            [call(task1, timeout=ANALYSIS_TIMEOUT)]
        )

    def test_success_resolved_errors(self,
                                     runner: Mock,
                                     context: Path,
                                     task1: ConfigurationTask,
                                     task2: ConfigurationTask,
                                     error: ShellTaskError):
        """Verify all resolved errors are returned."""
        runner.run_task.side_effect = [None, error, None, None]

        result = ParseResult(tasks=[task1, task2])

        analysis_result = analyze_configuration_script(context, result)

        assert analysis_result.success
        assert analysis_result.tasks == frozenset({
            (None, task1, None, None),
            (None, task2, error, (task1,)),
        })
        assert not analysis_result.failed_at_task
        assert not analysis_result.configuration_task_error
        runner.run_task.assert_has_calls(
            [
                call(task1, timeout=ANALYSIS_TIMEOUT),
                call(task2, timeout=ANALYSIS_TIMEOUT),
            ] * 2
        )


class TestAnalyzeAndRecord:
    """Tests for ``analyze_and_record``."""

    @pytest.fixture(autouse=True)
    def get_parser(self) -> Generator[Mock, None, None]:
        """Mock the get_parser method for testing."""
        with patch.object(docker, 'get_parser') as mock:
            yield mock

    @pytest.fixture(autouse=True)
    def analyze_configuration_script(self) -> Generator[Mock, None, None]:
        """Mock the analysis method for testing."""
        with patch.object(docker, 'analyze_configuration_script') as mock:
            yield mock

    @pytest.fixture(autouse=True)
    def insert_task_executions(self) -> Generator[Mock, None, None]:
        """Mock the insert method for testing."""
        with patch.object(docker, 'insert_task_executions') as mock:
            yield mock

    @pytest.fixture
    def context(self) -> Path:
        """Create a context directory for testing."""
        return Path()

    def test_analyze_and_record(self,
                                get_parser: Mock,
                                analyze_configuration_script: Mock,
                                insert_task_executions: Mock,
                                context: Path):
        """Verify all methods are called correctly."""
        path = Path('file')
        get_parser.return_value.__name__ = 'mock parser'

        analyze_and_record(path, context)

        get_parser.assert_called_with(path)
        get_parser.return_value.assert_called_with(path, context=context)
        analyze_configuration_script.assert_called_with(
            context, get_parser.return_value.return_value, setup=None,
        )
        insert_task_executions.assert_called_with(
            analyze_configuration_script.return_value.tasks,
        )

    def test_parse_only(self,
                        get_parser: Mock,
                        analyze_configuration_script: Mock,
                        insert_task_executions: Mock,
                        context: Path):
        """Verify the script is parsed but not executed."""
        path = Path('file')
        get_parser.return_value.__name__ = 'mock parser'

        analyze_and_record(path, context, parse_only=True)

        get_parser.assert_called_with(path)
        get_parser.return_value.assert_called_with(path, context=context)
        analyze_configuration_script.assert_not_called()
        insert_task_executions.assert_not_called()
