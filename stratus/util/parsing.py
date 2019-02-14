import six

class HtmlCodec:
    def marshal(self, value, **kwargs):
        return value

    def unmarshal(self, data, **kwargs):
        if isinstance(data, six.binary_type):
            data = data.decode('utf-8')
        return { "message": data, "code": 0 }

def s2b( s: str ):
    return s.encode( 'utf-8'  )

def b2s( b: bytearray ):
    return b.decode( 'utf-8'  )