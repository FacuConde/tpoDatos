import logging


def setup_logging():
    """Configura el logging para salida clara en consola.

    Formato: TIMESTAMP LEVEL [logger_name] message
    """
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-8s [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        # Reducir ruido de librerías externas que imprimen muchos warnings
        noisy = [
            'cassandra',
            'cassandra.cluster',
            'cassandra.policies',
            'cassandra.pool',
            'urllib3',
            'pika',
            'asyncio',
            'dns.resolver',
            'dnspython'
        ]
        for name in noisy:
            logging.getLogger(name).setLevel(logging.ERROR)

        # Capturar warnings de la librería warnings y redirigirlos al logging
        try:
            import warnings
            logging.captureWarnings(True)
        except Exception:
            pass


# Configure logging on import for simplicity in small scripts
setup_logging()
