from celery import Celery

# class CeleryApplication:
#
#     def make_celery( self, name, config ):
#         celery = Celery( name, backend=config['DATABASE_URI'], broker=config['CELERY_BROKER_URL'] )
#         celery.conf.update( config )
#
#         class ContextTask(celery.TaskHandle):
#             def __call__(self, *args, **kwargs):
#                 with app.app_context():
#                     return self.run(*args, **kwargs)
#
#         celery.TaskHandle = ContextTask
#         return celery
#
