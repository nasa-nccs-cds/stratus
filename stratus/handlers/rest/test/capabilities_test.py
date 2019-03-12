from stratus.handlers.core import StratusCore

if __name__ == "__main__":

    settings = dict(stratus=dict(type="rest", host="127.0.0.1", port="5000", api="core"))
    stratus = StratusCore(settings)
    client = stratus.getClient()
    response = client.capabilities( "collection" )
    print( str(response) )

