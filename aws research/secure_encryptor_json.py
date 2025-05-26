
import os
import gzip
import io
import base64
import argparse
import json
from pathlib import Path
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding as sym_padding
from cryptography.hazmat.backends import default_backend
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

def gzip_compress(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as f:
        f.write(data)
    return buf.getvalue()

def gzip_decompress(data: bytes) -> bytes:
    buf = io.BytesIO(data)
    with gzip.GzipFile(fileobj=buf, mode='rb') as f:
        return f.read()

def aes_encrypt(data: bytes, key: bytes, iv: bytes) -> bytes:
    padder = sym_padding.PKCS7(128).padder()
    padded = padder.update(data) + padder.finalize()
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    return cipher.encryptor().update(padded) + cipher.encryptor().finalize()

def aes_decrypt(data: bytes, key: bytes, iv: bytes) -> bytes:
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    padded = cipher.decryptor().update(data) + cipher.decryptor().finalize()
    unpadder = sym_padding.PKCS7(128).unpadder()
    return unpadder.update(padded) + unpadder.finalize()

def rsa_encrypt_key(aes_key: bytes, rsa_pub: bytes) -> bytes:
    pub_key = RSA.import_key(rsa_pub)
    cipher = PKCS1_OAEP.new(pub_key)
    return cipher.encrypt(aes_key)

def rsa_decrypt_key(enc_key: bytes, rsa_priv: bytes) -> bytes:
    priv_key = RSA.import_key(rsa_priv)
    cipher = PKCS1_OAEP.new(priv_key)
    return cipher.decrypt(enc_key)

def encrypt_file_to_json(input_file: str):
    data = Path(input_file).read_bytes()
    compressed = gzip_compress(data)

    aes_key = os.urandom(32)
    iv = os.urandom(16)
    encrypted_data = aes_encrypt(compressed, aes_key, iv)

    rsa_key = RSA.generate(2048)
    public_pem = rsa_key.publickey().export_key()
    private_pem = rsa_key.export_key()

    encrypted_key = rsa_encrypt_key(aes_key, public_pem)

    json_payload = {
        "enc.key": base64.b64encode(encrypted_key).decode(),
        "iv": base64.b64encode(iv).decode(),
        "payload": base64.b64encode(encrypted_data).decode()
    }

    base_name = Path(input_file).stem
    json_path = f"{base_name}.encrypted.json"
    Path(json_path).write_text(json.dumps(json_payload, indent=2))
    Path(f"{base_name}.rsa_priv.pem").write_bytes(private_pem)
    Path(f"{base_name}.rsa_pub.pem").write_bytes(public_pem)

    print(f"[+] Encrypted JSON written to: {json_path}")
    print(f"[+] RSA keys saved to: {base_name}.rsa_priv.pem / .rsa_pub.pem")

def decrypt_file_from_json(base_name: str):
    json_path = f"{base_name}.encrypted.json"
    priv_path = f"{base_name}.rsa_priv.pem"
    output_path = f"{base_name}.restored.txt"

    payload = json.loads(Path(json_path).read_text())
    encrypted_key = base64.b64decode(payload["enc.key"])
    iv = base64.b64decode(payload["iv"])
    encrypted_data = base64.b64decode(payload["payload"])
    private_key = Path(priv_path).read_bytes()

    aes_key = rsa_decrypt_key(encrypted_key, private_key)
    decrypted = aes_decrypt(encrypted_data, aes_key, iv)
    decompressed = gzip_decompress(decrypted)

    Path(output_path).write_bytes(decompressed)
    print(f"[+] File restored: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Encrypt or decrypt using JSON bundle format")
    parser.add_argument("mode", choices=["encrypt", "decrypt"], help="Mode")
    parser.add_argument("file", help="Input file (for encryption) or base name (for decryption)")
    args = parser.parse_args()

    if args.mode == "encrypt":
        encrypt_file_to_json(args.file)
    elif args.mode == "decrypt":
        decrypt_file_from_json(Path(args.file).stem)
