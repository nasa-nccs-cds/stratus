import os, sys
import shutil
import zmq.auth
from stratus_endpoint.util.config import StratusLogger

def generate_certificates( base_dir, client_only = False ):
    ''' Generate client and server CURVE certificate files'''
    logger = StratusLogger.getLogger()
    keys_dir = os.path.join(base_dir, 'certificates')
    if not os.path.exists(keys_dir): os.makedirs(keys_dir)
    logger.info( f"Generating ZMQ key and certificate files in {base_dir}")
    public_keys_dir = os.path.join(base_dir, 'public_keys')
    secret_keys_dir = os.path.join(base_dir, 'private_keys')

    # Create directories for certificates, remove old content if necessary
    for d in [keys_dir, public_keys_dir, secret_keys_dir]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.mkdir(d)

    # create new keys in certificates dir
    server_public_file, server_secret_file = zmq.auth.create_certificates(keys_dir, "server")
    client_public_file, client_secret_file = zmq.auth.create_certificates(keys_dir, "client")

    # move public keys to appropriate directory
    for key_file in os.listdir(keys_dir):
        if key_file.endswith(".key"):
            shutil.move( os.path.join(keys_dir, key_file), os.path.join(public_keys_dir, '.') )

    # move secret keys to appropriate directory
    for key_file in os.listdir(keys_dir):
        if key_file.endswith(".key_secret"):
            shutil.move( os.path.join(keys_dir, key_file), os.path.join(secret_keys_dir, '.') )

if __name__ == '__main__':
    if zmq.zmq_version_info() < (4,0):
        raise RuntimeError("Security is not supported in libzmq version < 4.0. libzmq version {0}".format(zmq.zmq_version()))
    cert_dir = sys.argv[1] if len(sys.argv ) > 1 else os.path.expanduser( "~/.stratus/zmq" )
    generate_certificates( cert_dir )