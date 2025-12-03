from __future__ import annotations


class OgdcRunnerException(Exception):
    """Base exception for `ogdc-runner`."""

    pass


class OgdcDataAlreadyPublished(OgdcRunnerException):
    """Exception that is raised when data for a recipe have already been published."""

    pass


class OgdcWorkflowExecutionError(OgdcRunnerException):
    """Exception for unexpected failures resulting from argo workflow executions."""

    pass


class OgdcInvalidRecipeDir(OgdcRunnerException):
    """Exception that is raised when recipe directory is not available/valid."""

    pass


class OgdcInvalidRecipeConfig(OgdcRunnerException):
    """Exception raised if invalid OGDC recipe configuration is encountered."""

    pass


class OgdcServiceApiError(OgdcRunnerException):
    """Exception raised when interacting with the OGDC Service API."""

    pass
