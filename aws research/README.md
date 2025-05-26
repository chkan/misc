
# Secure File Encryption with JSON Bundle Format

This Python script provides a complete solution to securely compress, encrypt, and package files into a single JSON structure using a hybrid encryption scheme (AES + RSA) and optional Base64 encoding.

## Features

- Gzip compression to reduce data size before encryption
- AES-256-CBC encryption for fast symmetric encryption
- RSA-2048 for secure AES key wrapping
- Base64 encoding for safe transport and storage in JSON
- Single output `.json` file containing all necessary information to decrypt
- Simple CLI interface for encryption and decryption

## JSON Output Structure

```
{
  "enc.key": "<RSA-encrypted AES key (Base64)>",
  "iv": "<Initialization Vector (Base64)>",
  "payload": "<AES-encrypted, gzip-compressed file content (Base64)>"
}
```

## CLI Usage

### Encrypt a file

```bash
python secure_encryptor_json.py encrypt myfile.txt
```

Outputs:
- `myfile.encrypted.json` – the encrypted bundle
- `myfile.rsa_priv.pem` – RSA private key (keep safe)
- `myfile.rsa_pub.pem` – RSA public key

### Decrypt a file

```bash
python secure_encryptor_json.py decrypt myfile.txt
```

Restores:
- `myfile.restored.txt` – original file content

## Cost-Benefit Analysis

| Aspect              | Benefits                                                               | Trade-offs                                                              |
|---------------------|------------------------------------------------------------------------|-------------------------------------------------------------------------|
| Single Output File  | Easier to manage and transmit                                          | Requires custom format handling                                         |
| Portability         | Encoded for safe use in APIs, databases, and transport layers          | Base64 adds ~33% size overhead                                          |
| Security            | Strong hybrid encryption (AES + RSA)                                   | Must securely store RSA private key                                     |
| Compression         | Reduces size before encryption, ideal for text                         | No effect on already-compressed files                                   |
| Simplicity          | Easy to debug, inspect, and extend                                     | JSON is not ideal for large binary blobs unless encoded                 |
| Extensibility       | Can add metadata, versions, etc.                                       | Requires version control for schema changes                             |

## Dependencies

Install with pip:

```bash
pip install cryptography pycryptodome
```

## License

MIT or similar open license. Use at your own discretion.

## Disclaimer

This example is intended for educational and internal use. For production use, ensure secure key handling, data validation, and auditing.
