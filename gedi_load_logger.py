class logger:
    def __init__(self, engine):
        self.engine = engine    
        self.info = 'INFO'
        self.warn = 'WARN'
        self.error = 'ERROR'
        self.fatal = 'FATAL'
        self.is_local_debug = False
        
    def log(self, log_level, message, ex = None, filename = None):
        # error_message = str()
        if(self.is_local_debug):
            print_message = f'{log_level} | {message}'
            if filename: print_message += f' | {filename}'
            if ex: print_message += f' | {ex}'
            print(print_message)
        else:
            with self.engine.begin() as connection:
                pk = connection.exec_driver_sql("INSERT INTO gedi_load_log (log_level, message, exception, filename) VALUES (%s, %s, %s, %s) RETURNING id", [(log_level, message, ex, filename)])