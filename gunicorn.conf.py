import multiprocessing

# Bind
bind = "0.0.0.0:8081"

# Workers
workers = 4
worker_class = "sync"
timeout = 3600

# Logging
accesslog = "logs/access.log"
errorlog = "logs/error.log"
loglevel = "info"

# Process
pidfile = "sgc.pid"
daemon = False

# Reload (util para debug, desligar em prod se quiser)
reload = False
