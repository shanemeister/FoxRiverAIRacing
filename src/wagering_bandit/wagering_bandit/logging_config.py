import logging, sys

def configure_logging():
    fmt = "[%(asctime)s] %(levelname)5s %(name)10s | %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            # add FileHandler(...) here if you like
        ]
    )