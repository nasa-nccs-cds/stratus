from jinja2 import Template

class WPSXML:
    header = """ xmlns: wps = "http://www.opengis.net/wps/1.0.0" xmlns: ows = "http://www.opengis.net/ows/1.1" xmlns: xlink = "http://www.w3.org/1999/xlink"
                 xmlns: xsi = "http://www.w3.org/2001/XMLSchema-instance" xsi: schemaLocation = "http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd"
                 service = "WPS" version = "1.0.0" xml: lang = "en-CA" """

    def __init__(self):
        pass

    def getExecuteResponse( self, message ):
        template = Template(
        """"< wps: ExecuteResponse  creation_time = {currentTime} >
            < wps: Status >
            < wps: ProcessStarted
                elapsed = {timeInStatus.toString} > {statusMessage} < / wps: ProcessStarted >
            < / wps: Status >
            < / wps: ExecuteResponse >""" )
        return template.render( x=message['x'] )
