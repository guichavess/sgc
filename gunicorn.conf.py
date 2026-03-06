import multiprocessing

# Bind
bind = "0.0.0.0:8081"

# Workers
# 1 worker + 4 threads para compartilhar estado em memória (ex: SIAFE status)
workers = 1
threads = 4
worker_class = "gthread"
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
