import pickle, hmac, hashlib

def send( sender_socket, data ):
    pickled_data = pickle.dumps(data)
    digest =  hmac.new('shared-key', pickled_data, hashlib.sha1).hexdigest()
    header = '%s' % (digest)
    sender_socket.send(header + ' ' + pickled_data)

def receive( receiver_socket ):
    conn,addr = receiver_socket.accept()
    data = conn.recv(1024)
    recvd_digest, pickled_data = data.split(' ')
    new_digest = hmac.new('shared-key', pickled_data, hashlib.sha1).hexdigest()
    if recvd_digest != new_digest:
        raise Exception( 'Integrity check failed' )
    else:
        unpickled_data = pickle.loads(pickled_data)
    return unpickled_data
