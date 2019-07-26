from stratus.handlers.zeromq.app import StratusApp
from stratus.app.core import StratusCore
import os
HERE: str = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE: str = os.path.join( HERE, "zmq_server_settings.ini" )

if __name__ == '__main__':

# Start up a STRATUS server as configured in the settings file

    core: StratusCore = StratusCore(SETTINGS_FILE)
    app: StratusApp = core.getApplication()
    app.run()
