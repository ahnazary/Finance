from finance.src.utils import custom_logger


def test_custom_logger():
    """
    Test the custom logger function.
    """

    logger = custom_logger(logger_name="test_logger")

    assert logger.name == "test_logger"