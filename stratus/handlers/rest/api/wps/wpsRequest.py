import requests, json
from xml.etree.ElementTree import Element
import defusedxml.ElementTree as ET
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus.util.config import StratusLogger

def execRequest( requestURL ) -> Element:
    response: requests.Response = requests.get( requestURL )
    return ET.fromstring( response.text )

def boolStr( bval ): return "true" if bval else "false"

class WPSExecuteRequest:

    def __init__( self, host_address ):
        self.logger = StratusLogger.getLogger()
        self._host_address = host_address
        self.ns = {'wps': "http://www.opengis.net/wps/1.0.0", "ows": "http://www.opengis.net/ows/1.1"}

    def _getBaseRequest( self, async ): return '%s?request=Execute&service=cwt&status=%s' % ( self._host_address, boolStr(async) )
    def _getCapabilities( self ): return '%s?request=getCapabilities&service=cwt' % ( self._host_address )
    def _describeProcess( self, processId ): return '%s?request=describeProcess&service=cwt&identifier=%s' % ( self._host_address, processId )
    def _getStratusRequest( self ): return '%s?request=Execute&service=cwt&status=true&identifier=cwt.workflow' % ( self._host_address )

    def _toDatainputs(self, requestJson: Dict  ) -> str:
        domains = json.dumps( requestJson["domain"] )
        variables = json.dumps(requestJson["input"])
        operations = json.dumps(requestJson["operation"])
        return f'&datainputs=[domain={domains},variable={variables},operation={operations}]'

    def getWps( self, requestSpec: Dict ) -> str:
        return self._getStratusRequest() + self._toDatainputs( requestSpec )

    def exe( self, requestJson ) -> Dict:
        requestURL = self.getWps( requestJson )
        self.logger.info( "\nExecuting Request:\n\n%s\n\nResponse:\n" % ( requestURL ) )
        responseXML: requests.Response = requests.get(requestURL).text
        refs = {}
        root =   ET.fromstring(responseXML)
        for eProcOut in root.findall("wps:ProcessOutputs",self.ns):
            for eOut in eProcOut.findall("wps:Output", self.ns):
                for eRef in eOut.findall("wps:Reference", self.ns):
                    refs[ eRef.attrib["id"] ] = eRef.attrib["href"]
        return { "xml": responseXML, "refs": refs }

    def getStatus(self, statusUrl: str ) -> Dict:
        responseXML: str = requests.get(statusUrl).text
        root = ET.fromstring(responseXML)
        for eStat in root.findall("wps:Status", self.ns):
            for eStatValue in eStat.findall("*", self.ns):
                elem: Element = eStatValue
                return dict( status=elem.tag.split("}")[1], message=elem.text )

    def downloadFile( self, filePath: str, fileUrl: str ):
        r = requests.get(fileUrl, allow_redirects=True)
        open(filePath, 'wb').write(r.content)

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


