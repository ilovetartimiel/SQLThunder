### --- Standard library imports --- ###
import logging

# Create a package-level logger (used across SQLThunder)
logger = logging.getLogger("SQLThunder")
logger.addHandler(logging.NullHandler())  # Prevents 'No handler found' warnings


# Create a user function to configure logging_config and also be used in CLI --verbose
def configure_logging(
    level: int = logging.INFO, format: str = "[%(levelname)s] %(name)s - %(message)s"
) -> None:
    """
    Configure the SQLThunder logger to output to stdout.

    This function should be called by the user (or CLI via --verbose) to enable logs.
    Prevents duplicate handlers from being added if called multiple times.

    Args:
        level (int): Logging level (e.g., logging.DEBUG, logging.WARNING).
        format (str): Logging format string. Defaults to simple console output.
    """
    pkg_logger = logging.getLogger("SQLThunder")
    pkg_logger.setLevel(level)

    # Avoid duplicate handlers if configure_logging() is called multiple times
    if not any(isinstance(h, logging.StreamHandler) for h in pkg_logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(format))
        pkg_logger.addHandler(handler)
