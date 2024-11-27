import base64
import pickle

# Base64-encoded pickle string
encoded_pickle = """
gAWVoAAAAAAAAAB9lCiMDG9iamVjdF9uYW1lc5RdlCiMCWF2b19lbnRyeZSMDmF2b19lbnRyeV9w
YXJ0lIwIYXZvX3RyYW6UZYwSbG9hZF9mcm9tX2RhdGV0aW1llIwZMjAyMy0wMS0wMVQwMDowMDow
MCswMDowMJSMEGxvYWRfdG9fZGF0ZXRpbWWUjBkyMDI0LTAyLTAxVDAwOjAwOjAwKzAwOjAwlHUu
"""

# Decode the Base64 string into binary
binary_data = base64.b64decode(encoded_pickle)

# Unpickle the binary data
decoded_object = pickle.loads(binary_data)

# Print the decoded object
print(decoded_object)