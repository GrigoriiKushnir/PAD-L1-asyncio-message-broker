import logging.config
from notibroker.broker import run_server

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s | %(asctime)s | %(name)s:%(funcName)s | %(process)d | %(thread)d | %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        }
    },
    'loggers': {
        'notibroker': {
            'level': 'DEBUG',
            'handlers': ['console']
        }
    }
})

if __name__ == '__main__':
    run_server()
