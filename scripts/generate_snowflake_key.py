"""Generate an RSA key pair for Snowflake key-pair authentication.

Writes to C:\\Users\\<you>\\.snowflake\\keys\\  (OUTSIDE the repo, on purpose --
the private key must never be committed or shared).

Run from the project venv:  python scripts/generate_snowflake_key.py
"""

import pathlib

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

key_dir = pathlib.Path.home() / ".snowflake" / "keys"
key_dir.mkdir(parents=True, exist_ok=True)

private_path = key_dir / "snowflake_key.p8"
public_path = key_dir / "snowflake_key.pub"

if private_path.exists():
    raise SystemExit(f"Refusing to overwrite existing key: {private_path}")

key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

private_path.write_bytes(
    key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
)

public_path.write_bytes(
    key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
)

print(f"Private key (SECRET, stays on this machine): {private_path}")
print(f"Public key  (goes into Snowflake):           {public_path}")
print("\nNext: open the .pub file, copy ONLY the lines between the BEGIN/END")
print("markers, and register it in Snowsight with:")
print("  ALTER USER PERCYABS SET RSA_PUBLIC_KEY = '<pasted-key-body>';")
