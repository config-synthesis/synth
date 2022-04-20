"""Synth serverless frameworks."""


# Imports.
from __future__ import annotations

import inspect
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Type

import boto3
import docker
from samcli.commands.local.cli_common.user_exceptions import (
    InvalidSamTemplateException,
)
from samcli.commands.validate.lib.sam_template_validator import (
    SamTemplateValidator,
)
from samcli.lib.providers.sam_function_provider import SamFunctionProvider
from samcli.local.docker.lambda_image import LambdaImage
from samcli.local.layers.layer_downloader import LayerDownloader
from samcli.yamlhelper import yaml_parse
from samtranslator.translator.managed_policy_translator import (
    ManagedPolicyLoader,
)
from yaml.error import YAMLError

from synth.logging import logger
from synth.serverless.exceptions import (
    FrameworkDefinitionException,
    InvalidFrameworkConfigFileException,
    MissingFrameworkConfigFileException,
    NoDefaultFunctionException,
    NoSuchFunctionException,
    UnknownFrameworkException,
)


class Framework(ABC):
    """Serverless framework."""

    _REQUIRED_ATTRIBUTES = [
        ('code', str),
        ('name', str),
        ('default_config_file_name', str),
    ]
    _sentinel = object()
    code: str = _sentinel
    name: str = _sentinel
    default_config_file_name: str = _sentinel

    ALL_FRAMEWORKS: dict[str, Type[Framework]] = {}

    def __init_subclass__(cls: Type[Framework], **kwargs):
        """Initialize Framework subclasses."""
        # Do nothing if the subclass is abstract.
        if inspect.isabstract(cls):
            return

        # Validate required class attributes.
        for attr_name, attr_type in Framework._REQUIRED_ATTRIBUTES:
            attr = getattr(cls, attr_name)
            if attr is Framework._sentinel:
                raise FrameworkDefinitionException(
                    f'Class `{cls.__name__}` is missing required class '
                    f'attribute `{attr_name}`.',
                )
            if not isinstance(attr, attr_type):
                raise FrameworkDefinitionException(
                    f'Class attribute `{cls.__name__}.{attr_name}` is is of '
                    f'the wrong type. Was `{type(getattr(cls, attr_name))}`, '
                    f'expected `{attr_type}`.',
                )

        # Register concrete subclass.
        Framework.ALL_FRAMEWORKS[cls.code] = cls

    @classmethod
    def for_path(cls, path: Path, code: Optional[str] = None) -> Framework:
        """Get a framework for a serverless function.

        Parameters
        ----------
        path : Path
            Path to a serverless function.
        code : Optional[str]
            Optional framework code.

        Raises
        ------
        UnknownFrameworkException
            Raised if the framework cannot be inferred from the file path.

        Returns
        -------
        Framework
            The inferred framework.
        """
        path = path.absolute()

        # If the framework is specified, try to load it.
        if code is not None:
            if code not in cls.ALL_FRAMEWORKS:
                raise UnknownFrameworkException(
                    f'Unknown framework `{code}`.',
                )
            return cls.ALL_FRAMEWORKS[code](path)

        # Otherwise, try to infer the correct framework by taking the first
        # one that loads using the configuration file.
        logger.verbose(f'Inferring framework used by `{path}`.')
        for Class in Framework.ALL_FRAMEWORKS.values():
            logger.verbose(f'Testing framework `{Class.code}`.')
            try:
                framework = Class(path)
            except MissingFrameworkConfigFileException:
                logger.verbose(f'No config file for `{Class.code}`.')
            except InvalidFrameworkConfigFileException:
                logger.verbose(f'Invalid config file for `{Class.code}`.')
            else:
                logger.info(f'Inferred framework `{Class.code}`.')
                return framework

        # No framework could be loaded.
        raise UnknownFrameworkException(
            f'Cannot determine the serverless framework in use at `{path}`.',
        )

    @classmethod
    @abstractmethod
    def is_valid_config_file(cls, path: Path) -> bool:
        """Determine if a configuration file is valid.

        Parameters
        ----------
        path : Path
            Path to a configuration file.

        Returns
        -------
        bool
            True iff the configuration file is valid.
        """
        if not path or not path.is_file():
            return False

        try:
            fd = open(path, 'r')
        except IOError:
            return False
        else:
            fd.close()

        return True

    @classmethod
    def resolve_config_file(cls, path: Path) -> Path:
        """Resolve a config file path from a serverless function path.

        Parameters
        ----------
        path : Path
            Function path. This may either be the path to a directory
            containing the configuration file, or a path to the configuration
            file.

        Raises
        ------
        MissingFrameworkConfigFileException
            Raised if the configuration file cannot be found.

        Returns
        -------
        Path
            Absolute path pointing to the configuration file.
        """
        if inspect.isabstract(cls):
            raise NotImplementedError('not implemented')

        if path.is_file():
            return path.absolute()

        if path.is_dir():
            config = path / cls.default_config_file_name
            if config.is_file:
                return config.absolute()

        raise MissingFrameworkConfigFileException(
            f'No config file found at `{path}` for framework `{cls.code}`',
        )

    def __init__(self, path: Path):
        """Create a new framework for a specific serverless function.

        Parameters
        ----------
        path : Path
            Path to the framework configuration file.

        Raises
        ------
        MissingFrameworkConfigFileException
            Raised if the configuration file cannot be found.
        InvalidFrameworkConfigFileException
            Raised if the configuration file is invalid.
        """
        self.original_path = path
        self.config_file = self.resolve_config_file(path)
        self.function_dir = self.config_file.parent
        logger.verbose(f'Using configuration file at `{self.config_file}`.')

        if not self.is_valid_config_file(self.config_file):
            raise InvalidFrameworkConfigFileException(
                f'The configuration file at `{self.config_file}` is not valid '
                f'for `{self.code}`.',
            )

    @abstractmethod
    def build_image(self, function_name: Optional[str] = None) -> dict:
        """Build a framework Docker image for a serverless function.

        Parameters
        ----------
        function_name : Optional[str]
            Optional function name to build. If not specified, the framework
            will build the default function.

        Returns
        -------
        dict
            Result of running `docker image inspect` on the built image.
        """
        raise NotImplementedError('not implemented')

    def inspect_image(self, image: str) -> dict:
        """Inspect a Docker image.

        Parameters
        ----------
        image : str
            Image name.

        Returns
        -------
        dict
            Docker inspect result.
        """
        return docker.from_env().api.inspect_image(image)


class AWSLambda(Framework):
    """AWS Lambda serverless framework."""

    code = 'aws'
    name = 'AWS Lambda'
    default_config_file_name = 'template.yaml'

    @classmethod
    def is_valid_config_file(cls, path: Path) -> bool:
        """Determine if a configuration file is valid.

        Parameters
        ----------
        path : Path
            Path to a configuration file.

        Returns
        -------
        bool
            True iff the configuration file is valid.
        """
        if not super().is_valid_config_file(path):
            return False

        # Run SAM validation.
        try:
            logger.verbose('Validating SAM template.')
            template = yaml_parse(path.read_text(encoding='utf-8'))
            client = boto3.client('iam', region_name='us-east-1')
            loader = ManagedPolicyLoader(client)
            validator = SamTemplateValidator(
                sam_template=template,
                managed_policy_loader=loader,
            )
            validator.is_valid()
        except (YAMLError, InvalidSamTemplateException) as e:
            logger.verbose(f'Template is invalid: {e}.')
            return False

        logger.verbose('Template is valid.')
        return True

    def build_image(self, function_name: Optional[str] = None) -> dict:
        """Build a framework Docker image for a serverless function.

        Parameters
        ----------
        function_name : Optional[str]
            Optional function name to build. If not specified, the framework
            will build the default function.

        Returns
        -------
        dict
            Result of running `docker image inspect` on the built image.
        """
        # Load the template configuration and initialize a function provider.
        template = yaml_parse(self.config_file.read_text(encoding='utf-8'))
        function_provider = SamFunctionProvider(template_dict=template)

        # Load the function definition, either by name or by default.
        if function_name:
            logger.verbose('Getting function by name.')
            function = function_provider.get(function_name)
            if not function:
                raise NoSuchFunctionException(
                    f'The function `{function_name}` does not exist.',
                )
        else:
            logger.verbose('Getting default function.')
            functions = list(function_provider.get_all())
            if len(functions) != 1:
                raise NoDefaultFunctionException(
                    'No default function available.',
                )
            function = functions[0]

        # Build the image.
        logger.info(f'Building image for `{function.name}`.')
        layer_downloader = LayerDownloader(
            layer_cache=str(self.function_dir / 'layers-pkg'),
            cwd=self.function_dir,
        )
        image_builder = LambdaImage(
            layer_downloader=layer_downloader,
            skip_pull_image=False,
            force_image_build=True,
        )
        with open(os.devnull, 'w') as devnull:
            image_tag = image_builder.build(
                runtime=function.runtime,
                layers=function.layers,
                is_debug=False,
                stream=devnull,
            )

        # Inspect the built image.
        return self.inspect_image(image_tag)


class Serverless(Framework):
    """The https://www.serverless.com/ framework."""

    code = 'sls'
    name = 'Serverless'
    default_config_file_name = 'serverless.yml'
