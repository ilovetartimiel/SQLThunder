### --- Standard library imports --- ###
import io
import logging

### --- Internal package imports --- ###
from SQLThunder.logging_config.logger import configure_logging

### --- Test Configure Logging --- ###


class TestConfigureLogging:

    def test_logger_sets_correct_level(self):
        configure_logging(level=logging.DEBUG)
        logger = logging.getLogger("SQLThunder")
        assert logger.level == logging.DEBUG

    def test_logger_applies_correct_format(self):
        test_format = "[%(levelname)s] %(message)s"
        log_stream = io.StringIO()

        # Custom handler for in-memory capture
        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(logging.Formatter(test_format))

        logger = logging.getLogger("SQLThunder")
        logger.handlers.clear()  # prevent duplicate handlers
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        logger.info("Hello World")
        handler.flush()  # flush to ensure everything is written

        output = log_stream.getvalue()
        assert "[INFO] Hello World" in output

    def test_logger_does_not_duplicate_handlers(self):
        logger = logging.getLogger("SQLThunder")
        original_handler_count = len(logger.handlers)

        configure_logging()
        configure_logging()

        # Ensure we didn't double-register StreamHandlers
        stream_handler_count = sum(
            isinstance(h, logging.StreamHandler) for h in logger.handlers
        )
        assert stream_handler_count == 1
        assert len(logger.handlers) >= original_handler_count
