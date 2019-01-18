import six

class HtmlCodec:
    def marshal(self, value, **kwargs):
        return value

    def unmarshal(self, data, **kwargs):
        if isinstance(data, six.binary_type):
            data = data.decode('utf-8')
        return { "message": data, "code": 0 }