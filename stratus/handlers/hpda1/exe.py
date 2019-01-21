from .handler import Handler

def post( request ):
    return Handler.exe( request )