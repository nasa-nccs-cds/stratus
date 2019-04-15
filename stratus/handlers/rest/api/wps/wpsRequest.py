import requests, json, pickle
from xml.etree.ElementTree import Element
import defusedxml.ElementTree as ET
import xarray as xa
from stratus_endpoint.util.config import Config, StratusLogger, UID
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus_endpoint.util.config import StratusLogger

def boolStr( bval ): return "true" if bval else "false"

class WPSExecuteRequest:

    def __init__( self, host_address ):
        self.logger = StratusLogger.getLogger()
        self._host_address = host_address
        self.ns = {'wps': "http://www.opengis.net/wps/1.0.0", "ows": "http://www.opengis.net/ows/1.1"}

    def _getCapabilitiesStr( self, type ): return '%s?request=getCapabilities&service=WPS&identifier=%s' % ( self._host_address, type )
    def _describeProcessStr( self, processId ): return '%s?request=describeProcess&service=WPS&identifier=%s' % ( self._host_address, processId )
    def _getStratusRequestStr( self ): return '%s?request=Execute&service=WPS&status=true&version=1.0.0' % ( self._host_address )

    def _getCapabilitiesDict( self, type ): return dict( request='getCapabilities', service='WPS', identifier=type )
    def _describeProcessDict( self, processId ): return dict( request='describeProcess', service='WPS', identifier=processId )
    def _getStratusRequestDict( self ): return dict( request='Execute', service='WPS', status='true', version='1.0.0'  )

    def _toDatainputsStr(self, requestJson: Dict  ) -> str:
        domains = json.dumps( requestJson["domain"] )
        variables = json.dumps(requestJson["input"])
        operations = json.dumps(requestJson["operation"])
        return f"&datainputs=[domain={domains},variable={variables},operation={operations}]"

    def _toDatainputsDict(self, requestJson: Dict  ) -> Dict:
        domains = json.dumps( requestJson["domain"] )
        variables = json.dumps(requestJson["input"])
        operations = json.dumps(requestJson["operation"])
        return dict( datainputs=f"[domain={domains},variable={variables},operation={operations}]" )

    def getWpsStr( self, requestSpec: Dict ) -> str:
        return self._getStratusRequestStr() + self._toDatainputsStr( requestSpec )

    def getWpsParms( self, requestSpec: Dict ) -> Dict:
        return { **self._getStratusRequestDict(), **self._toDatainputsDict( requestSpec ) }

    def exe( self, requestJson ) -> Dict:
        requestParms = self.getWpsParms( requestJson )
        self.logger.info( "\nExecuting Request: host = %s\n Params: %s\nResponse:\n" % ( self._host_address, str(requestParms) ) )
        responseXML: requests.Response = requests.get( self._host_address, params=requestParms ).text
        refs = {}
        root =   ET.fromstring(responseXML)
        for eProcOut in root.findall("wps:ProcessOutputs",self.ns):
            for eOut in eProcOut.findall("wps:Output", self.ns):
                for eRef in eOut.findall("wps:Reference", self.ns):
                    refs[ eRef.attrib["id"] ] = eRef.attrib["href"]
        return { "xml": responseXML, "refs": refs }

    def getStatus(self, statusUrl: str ) -> Dict:
        responseXML: str = requests.get(statusUrl).text
        self.logger.info( "GetStatus Response XML: \n" + responseXML )
        root = ET.fromstring(responseXML)
        for eStat in root.findall("wps:Status", self.ns):
            for eStatValue in eStat.findall("*", self.ns):
                elem: Element = eStatValue
                return dict( status=elem.tag.split("}")[1], message=elem.text )

    def downloadFile( self, filePath: str, fileUrl: str ):
        r = requests.get(fileUrl, allow_redirects=True)
        contentType = r.headers['Content-Type']
        if   contentType == 'application/octet-stream': open(filePath, 'wb').write(r.content)
        elif contentType == 'application/x-netcdf':     open(filePath, 'wb').write(r.content)
        elif contentType == 'application/json':         self.logger.info( "Got result for file download: " + str(r.json()) )
        else:                                           self.logger.error("Got result with contentType: " + str(contentType))

    def downloadData( self, dataUrl: str ) -> xa.Dataset:
        r = requests.get( dataUrl, allow_redirects=True )
        contentType = r.headers['Content-Type']
        if   contentType == 'application/octet-stream': return pickle.loads( r.content, encoding="bytes" )
        elif contentType == 'application/x-netcdf':     return pickle.loads(r.content, encoding="bytes")
        elif contentType == 'application/json':         self.logger.info( "Got result for data download: " + str(r.json()) )
        else:                                           self.logger.error("Got result with contentType: " + str(contentType))

    def execJsonRequest( self, requestURL, parms: Dict) -> Dict:
        response: requests.Response = requests.get(requestURL, params=parms)
        self.logger.info( f"SUBMIT JSON Request {requestURL} with parms: {parms}\n  Response: \n {response.text}" )
        if response.ok:
            return json.loads(response.text)
        else:
            raise Exception(response.text)

    def execRequest(self, requestURL, parms: Dict) -> Element:
        response: requests.Response = requests.get(requestURL, params=parms)
        return ET.fromstring(response.text)

    def getCapabilities( self, type="processes" ) -> Dict:
#        requestParms = self._getCapabilitiesDict(type)
        requestParms = {'request': 'getCapabilities', 'service': 'WPS'}
        root = self.execRequest(  self._host_address, requestParms )
        epas = []
        for module_elem in root.iter("module"):
            for op_elem in module_elem.iter("kernel"):
                modname = module_elem.attrib["name"]
                opName = op_elem.attrib["name"]
                epas.append( f"{modname}.{opName}")
        return { "epas": ET.tostring(root, encoding='utf8', method='xml') }

    def describeProcess( self, processId ):
        requestParms = self._describeProcessDict( processId )
        return self.execRequest( self._host_address, requestParms )


