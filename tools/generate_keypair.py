import cryptography.hazmat.primitives.serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--name", required=True, type=str, help="Name of key pair to save")
args = parser.parse_args()


if os.path.isfile("%s.key" % args.name):
    print("Error: file %s.key exists" % args.name)
    exit(1)

private_key = X25519PrivateKey.generate()
public_key = private_key.public_key()

with open("%s.key" % args.name, "wb") as f:
    f.write(private_key.private_bytes(cryptography.hazmat.primitives.serialization.Encoding.Raw,
                                      cryptography.hazmat.primitives.serialization.PrivateFormat.Raw,
                                      cryptography.hazmat.primitives.serialization.NoEncryption()))

with open("%s.pub" % args.name, "wb") as f:
    f.write(public_key.public_bytes(cryptography.hazmat.primitives.serialization.Encoding.Raw,
                                    cryptography.hazmat.primitives.serialization.PublicFormat.Raw))
