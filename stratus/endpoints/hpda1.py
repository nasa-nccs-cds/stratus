import json, string, random

class Endpoint:
    current_exe_id = None

    @classmethod
    def randomStr(cls, length) -> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

    @classmethod
    def getDefaultStatus(cls,  stat="executing", create=False ):
        if create: cls.current_exe_id = cls.randomStr(6)
        elif not  cls.current_exe_id: stat = "undefined"
        return  { "id": cls.current_exe_id, "status": stat }

    @classmethod
    def exe(cls, request ):
        result = cls.getDefaultStatus(create=True)
        result.update( request )
        return result

    @classmethod
    def exeStat(cls,  id ):
        return cls.getDefaultStatus()

    @classmethod
    def exeKill(cls,  id ):
        result = cls.getDefaultStatus(stat="killed")
        cls.current_exe_id = None
        return result

def exe( request ):
    return Endpoint.exe( request )

def stat( id ):
    return Endpoint.exeStat( id )


def kill( id ):
    return Endpoint.exeKill( id )



