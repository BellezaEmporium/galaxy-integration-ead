###
# welcome (back) to the infamous file decryption module. 
# this will permit to fully remove any API calls linked to the EA server, except subscription gathering.
#
# It now uses the Nucleus ID as an alternative choice. Either you need to use a Nucleus ID (for all files in that folder), either it uses a hardcoded string. It will all depend on the flag.
# So, if we recapitulate, we have : SHA3-256("allUsersGenericId" + file name + "l)%ge7fomILhfj*Qfi+,"),
# SHA3-256("allUsersGenericId" + file name + machine hash), AND, for the specific CONF-production workaround,
# SHA3-256(Nucleus ID + file + machine hash), in that nucleus ID folder, or SHA3-256(file name + machine hash)
# in that allUsers folder (yes, it can be very confusing).
#
# CATS2 - allUsersGenericIdCATS2l)%ge7fomILhfj*Qfi+, SHA3-256 
# NS - NucleusID + NS + machash SHA3-256 
# CONF-production - NucleusID + CONF-production + machash SHA3-256 
# IQ - allUsersGenericId + IQ + machash SHA3-256
# IS - allUsersGenericIdISl)%ge7fomILhfj*Qfi+, SHA3-256
# 
# Not all of them are useful, but they are all here for reference.
###
import os
import sys
import json
import cryptography

# SHA256 & AES-CBC decryption functions
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.modes import CBC
from hashlib import sha256, sha1, sha3_256

# SHA3-256 encryption of the folder names
allUsersGenericIdSalt = sha3_256(b"allUsersGenericId").hexdigest()
nucleusIdSalt = sha3_256().hexdigest() # TODO link w/ plugin.py to get the PID (nucleus ID) and inject it here

# Hardware information gathering for machash


# Decryption function for the files
def decrypt_file(file, key):
    # Read the encrypted file
    with open(file, 'rb') as f:
        iv = f.read(16)  # Read the initialization vector (IV)
        ciphertext = f.read()  # Read the rest of the file (ciphertext)

    # Create a cipher object using the key and IV
    cipher = Cipher(
        algorithms.AES(key),
        modes.CBC(iv)
    )

    # Decrypt the ciphertext
    decryptor = cipher.decryptor()
    plaintext = decryptor.update(ciphertext) + decryptor.finalize()

    return plaintext