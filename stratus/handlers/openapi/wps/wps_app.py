from stratus.handlers.openapi.app import StratusApp
import os
HERE = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join( HERE, "test_settings.ini" )

if __name__ == "__main__":
    app = StratusApp( settings=SETTINGS_FILE )
    app.run()
