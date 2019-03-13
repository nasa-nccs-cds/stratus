import requests, json
from xml.etree.ElementTree import Element
import defusedxml.ElementTree as ET
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.util.config import StratusLogger

def execRequest( requestURL ) -> Element:
    response: requests.Response = requests.get( requestURL )
    return ET.fromstring( response.text )

class Variable:
    def __init__( self, uid, uri, varname, domain_id ):
        self._uid = uid
        self._uri = uri
        self._varname = varname
        self._dom_id = domain_id

    def toWps(self): return '{"uri":"%s","name":"%s:%s","domain":"%s"}' % ( self._uri, self._varname, self._uid, self._dom_id )

class Domain:
    def __init__( self, id, axes=None ):
        self._id = id
        self._axes = axes if axes is not None else []

    def addAxis( self,  a ): self._axes.append( a )

    def toWps(self):
        return ('{"name":"%s",%s}' % ( self._id, ",".join([axis.toWps() for axis in self._axes]))) if len(self._axes) else ('{"name":"%s"}' % ( self._id ))

class Axis:
    def __init__( self, id, start, end, system ):
        self._id = id
        self._start = start
        self._end = end
        self._system = system

    def toWps(self): return '"%s":{"start":%s,"end":%s,"system":"%s"}' % ( self._id, self._start, self._end, self._system )

class Operation:

    def __init__( self, package, kernel, input_uids, args, result_id = None ):
        self._result_id = result_id
        self._package = package
        self._kernel = kernel
        self._args =  {}
        for item in args.iteritems(): self._args[ '"' + item[0] + '"'] = '"' + item[1] + '"'
        self._input_uids = input_uids
        if len(input_uids): self._args['"input"'] = "%s" % ( ",".join( [ '"%s"' % item  for item in input_uids ] )  )
        self._args['"name"'] = '"' + self.getIdentifier() + '"'
        if result_id: self._args['"rid"'] = '"' + result_id + '"'

    def getIdentifier(self): return '%s.%s' % ( self._package, self._kernel )

    def toWps(self):
        return "{%s}" % ( ",".join( [ '%s:%s' % (item[0],item[1])  for item in self._args.iteritems() ] )  )

def boolStr( bval ): return "true" if bval else "false"

class WPSExecuteRequest:

    def __init__( self, host_address ):
        self.logger = StratusLogger.getLogger()
        self._host_address = host_address
        self._variables = []
        self._domains = []
        self._operations = []
        self.ns = {'wps': "http://www.opengis.net/wps/1.0.0", "ows": "http://www.opengis.net/ows/1.1"}

    def _getBaseRequest( self, async ): return '%s?request=Execute&service=cwt&status=%s' % ( self._host_address, boolStr(async) )
    def _getCapabilities( self ): return '%s?request=getCapabilities&service=cwt' % ( self._host_address )
    def _describeProcess( self, processId ): return '%s?request=describeProcess&service=cwt&identifier=%s' % ( self._host_address, processId )
    def _getStratusRequest( self ): return '%s?request=Execute&service=cwt&status=true&identifier=cwt.workflow' % ( self._host_address )

    def _getIdentifier(self ):
        if   len( self._operations ) == 0: return '&identifier=util.cache'
        elif len( self._operations ) == 1: return ( '&identifier=%s' % self._operations[0].getIdentifier() )
        else: return ( '&identifier=CDS.workflow' )

    def _getDatainputs( self ):
        return '&datainputs=[%s,%s,%s]' % ( self._getDomains(), self._getVariables(), self._getOperations() )

    def _toDatainputs(self, requestJson: Dict  ) -> str:
        domains = json.dumps( requestJson["domain"] )
        variables = json.dumps(requestJson["input"])
        operations = json.dumps(requestJson["operation"])
        return f'&datainputs=[domain={domains},variable={variables},operation={operations}]'

    def _getDomains( self ):
        return 'domain=[%s]' % ( ",".join( [ domain.toWps() for domain in self._domains ] )  )

    def _getVariables( self ):
        return 'variable=[%s]' % ( ",".join( [ variable.toWps() for variable in self._variables ] )  )

    def _getOperations( self ):
        return 'operation=[%s]' % ( ",".join( [ operation.toWps() for operation in self._operations ] )  )

    def addInputVariable( self, v ): self._variables.append( v )

    def addDomain( self, d ): self._domains.append( d )

    def addOperation(self, o ): self._operations.append( o )

    def toWps( self, async ):
        return self._getBaseRequest( async ) + self._getIdentifier( ) + self._getDatainputs( )

    def getWps( self, requestSpec: Dict ) -> str:
        return self._getStratusRequest() + self._toDatainputs( requestSpec )

    def execute( self, async ):
        request = self.toWps(async)
        self.logger.info( "\nExecuting Request:\n\n%s\n\nResponse:\n" % ( request ) )
        return execRequest( request )

    def exe( self, requestJson ) -> Dict:
        requestURL = self.getWps( requestJson )
        self.logger.info( "\nExecuting Request:\n\n%s\n\nResponse:\n" % ( requestURL ) )
        responseXML: requests.Response = requests.get(requestURL).text
        refs = {}
        root =   ET.fromstring(responseXML)
        for pout_elem in root.findall("wps:ProcessOutputs",self.ns):
            for out_elem in pout_elem.findall("wps:Output", self.ns):
                for ref_elem in out_elem.findall("wps:Reference", self.ns):
                    refs[ ref_elem.attrib["id"] ] = ref_elem.attrib["href"]
        return { "xml": responseXML, "refs": refs }

    def getCapabilities( self ) -> Dict:
        request = self._getCapabilities()
        root = execRequest( request )
        print( str(root))
        epas = []
        for module_elem in root.iter("module"):
            for op_elem in module_elem.iter("kernel"):
                modname = module_elem.attrib["name"]
                opName = op_elem.attrib["name"]
                epas.append( f"{modname}.{opName}")
        return { "xml": str(root), "epas": epas }

    def describeProcess( self, processId ):
        request = self._describeProcess( processId )
        return execRequest( request )


