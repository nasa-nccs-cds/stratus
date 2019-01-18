import json

def getUserNames():
    return [ "tpmaxwel" ]

def exe( variables, domains, operations ):
    return { "variables": json.loads(variables), "domains": json.loads(domains), "operations": json.loads(operations) }

def exeStat( id ):
    return { "Status": "Executing" }

def exeKill( id ):
    return { "Status": "Killed" }

