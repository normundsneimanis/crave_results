import cryptography
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives import hashes
import cryptography.hazmat.primitives.serialization
import cryptography.hazmat.backends
import errno
import struct
import socket
import os


def _get_payload(log, connection, client_address, new_data):
    payload_length = struct.unpack("!L", new_data[0:4])[0]
    new_data_len = len(new_data[4:])
    log.debug("_get_payload() got request for %d bytes with %d bytes already" % (payload_length, new_data_len))
    new_data_ = [new_data[4:]]

    if payload_length == new_data_len:
        return b''.join(new_data_)

    data = None
    try:
        while True:
            received = connection.recv(65535)
            if len(received) == 0:
                log.warning("Client closed connection.")
                break
            new_data_.append(received)
            new_data_len += len(received)
            if new_data_len == payload_length:
                # log.info("Received all data.")
                data = b''.join(new_data_)
                break
    except socket.error as e:
        err = e.args[0]
        if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
            # log.info('No data available')
            if new_data_len == payload_length:
                # log.info("Received all data2.")
                data = b''.join(new_data_)
                # log.info("Closing connection")
                # connection.close()
        else:
            log.error("Socket error: %s" % str(e))
            log.exception(e)
    except Exception as e:
        log.error("Another exception: %s" % str(e))
        log.exception(e)

    return data


def encrypt(receiver_pubkey, sender_key, nonce, message, authenticated_data):
    shared_key1 = sender_key.exchange(receiver_pubkey)  # X448, X25519
    derived_key1 = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=None,
        backend=cryptography.hazmat.backends.default_backend()
    ).derive(shared_key1)
    chacha1 = ChaCha20Poly1305(derived_key1)
    return chacha1.encrypt(nonce, message, authenticated_data)


def decrypt(sender_pubkey, receiver_key, nonce, ciphertext, authenticated_data):
    shared_key2 = receiver_key.exchange(sender_pubkey)  # X448, X25519
    derived_key2 = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=None,
        backend=cryptography.hazmat.backends.default_backend()
    ).derive(shared_key2)
    chacha2 = ChaCha20Poly1305(derived_key2)
    return chacha2.decrypt(nonce, ciphertext, authenticated_data)


class CraveCryptError(Exception):
    pass


class CraveCryptServer:
    def __init__(self, logger):
        self.server_key, self.server_pubkey = get_client_keys("server")
        self.client_pubkeys = {}
        for f in os.listdir("."):
            if f.endswith(".pub"):
                if f == 'server.pub':
                    continue
                with open(f, "rb") as fi:
                    self.client_pubkeys[f.replace(".pub", "")] = X25519PublicKey.from_public_bytes(fi.read())
        self.logger = logger

    def encrypt(self, client, payload):
        if client not in self.client_pubkeys.keys():
            raise CraveCryptError("Client %s not known" % client)
        nonce = os.urandom(12)
        return nonce + encrypt(self.client_pubkeys[client], self.server_key, nonce, payload, b"")

    def decrypt(self, payload):
        auth_data_len = struct.unpack("!H", payload[:2])[0]
        message = payload[2:]
        if len(message) < auth_data_len:
            raise CraveCryptError("Cannot decrypt. Requested auth data longer than message.")
        auth_data = struct.unpack(f"!{auth_data_len}s", message[:auth_data_len])[0]
        sender_name = auth_data.decode()
        if sender_name not in list(self.client_pubkeys.keys()):
            raise CraveCryptError("Error: client '%s' not in known clients list." % sender_name)

        payload = message[auth_data_len:]

        nonce_ = payload[:12]
        try:
            result_ = decrypt(self.client_pubkeys[sender_name], self.server_key, nonce_, payload[12:], auth_data)
            return sender_name, result_
        except Exception as e:
            self.logger.error("Failed decrypting answer: %s" % str(e))
            self.logger.exception(e)
            raise CraveCryptError("Failed to decrypt data") from e


class CraveCrypt:
    def __init__(self, logger):
        self.server_pubkey = get_server_pubkey()
        self.client_key, self.client_pubkey = get_client_keys()
        self.logger = logger

    def encrypt(self, payload):
        return encrypt_with_header(self.server_pubkey, self.client_key, payload)

    def decrypt(self, payload):
        nonce_ = payload[:12]
        try:
            result_ = decrypt(self.server_pubkey, self.client_key, nonce_, payload[12:], b"")
            return result_
        except Exception as e:
            self.logger.error("Failed decrypting answer: %s" % str(e))
            self.logger.exception(e)
            raise CraveCryptError("Decrypt failure") from e


class CraveCryptTest:
    """
    Class used for testing server by sending unencrypted data
    """
    def __init__(self, logger):
        self.server_pubkey = get_server_pubkey()
        self.client_key, self.client_pubkey = get_client_keys()
        self.logger = logger

    def encrypt(self, payload):
        return payload

    def decrypt(self, payload):
        nonce_ = payload[:12]
        try:
            result_ = decrypt(self.server_pubkey, self.client_key, nonce_, payload[12:], b"")
            return result_
        except Exception as e:
            self.logger.error("Failed decrypting answer: %s" % str(e))
            self.logger.exception(e)
            raise CraveCryptError("Decrypt failure") from e


def get_server_pubkey():
    """
    Reads server X25519 public key from `server.pub` and returns it

    :return: Server public key
    """
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "server.pub"), "rb") as f:
        return X25519PublicKey.from_public_bytes(f.read())


def get_client_keys(name=socket.gethostname()):
    """
    Reads client X25519 private key from file `<hostname>.key` and returns private public key

    :return: Client private key, client public key
    """
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "%s.key" % name), "rb") as f:
        client_key = X25519PrivateKey.from_private_bytes(f.read())
        client_pub = client_key.public_key()
    return client_key, client_pub


def encrypt_with_header(server_pubkey, client_key, request_payload):
    """
    Creates header for packet encryption

    :return: nonce, network encoded header
    """

    nonce = os.urandom(12)
    auth_data = socket.gethostname().encode()
    auth_data_len = len(auth_data)
    header = struct.pack(f"!H{auth_data_len}s", auth_data_len, auth_data)

    return header + nonce + encrypt(server_pubkey, client_key, nonce, request_payload, auth_data)
