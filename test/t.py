from typing import Union, Dict, Optional
import rlp
from eth_utils import keccak
from hexbytes import HexBytes

class Node:
    def __init__(self):
        self.value = None
        self.children: Dict[int, Optional['Node']] = {}

def encode_node(node: Node) -> bytes:
    """
    Encodes a node according to the Merkle Patricia Trie specification.
    """
    if node.value is not None:
        # Leaf node
        return rlp.encode([
            encode_path(get_path(node), True),
            node.value
        ])
    
    # Branch node
    encoded_children = []
    for i in range(16):
        if i in node.children and node.children[i]:
            child_hash = keccak(encode_node(node.children[i]))
            encoded_children.append(child_hash)
        else:
            encoded_children.append(b'')
    
    # Add the value (if any) as the 17th element
    encoded_children.append(node.value if node.value else b'')
    return rlp.encode(encoded_children)

def encode_path(path: bytes, is_leaf: bool) -> bytes:
    """
    Encodes a path with a prefix based on whether it's a leaf or extension node.
    """
    nibbles = bytes_to_nibbles(path)
    prefix = [2 if is_leaf else 0] if len(nibbles) % 2 == 0 else [3 if is_leaf else 1]
    return bytes(prefix + nibbles)

def bytes_to_nibbles(b: bytes) -> list:
    """
    Converts bytes to nibbles (half-bytes).
    """
    nibbles = []
    for byte in b:
        nibbles.extend([byte >> 4, byte & 0x0F])
    return nibbles

def get_path(node: Node) -> bytes:
    """
    Gets the path to a node by concatenating the keys of its ancestors.
    """
    path = []
    current = node
    while current:
        if hasattr(current, 'key'):
            path.append(current.key)
        current = current.parent if hasattr(current, 'parent') else None
    return bytes(reversed(path))

def calculate_root(key_values: Dict[Union[str, bytes], Union[str, bytes]]) -> HexBytes:
    """
    Calculates the Merkle Patricia Trie root hash from a dictionary of key-value pairs.
    
    Args:
        key_values: Dictionary where both keys and values can be strings or bytes
    
    Returns:
        Root hash as HexBytes
    """
    root = Node()
    
    # Convert all inputs to bytes
    normalized_pairs = {
        k.encode() if isinstance(k, str) else k:
        v.encode() if isinstance(v, str) else v
        for k, v in key_values.items()
    }
    
    # Insert all key-value pairs into the trie
    for key, value in normalized_pairs.items():
        current = root
        path = bytes_to_nibbles(key)
        
        # Traverse/create the path
        for nibble in path[:-1]:
            if nibble not in current.children:
                current.children[nibble] = Node()
            current = current.children[nibble]
        
        # Set the value at the leaf
        last_nibble = path[-1]
        if last_nibble not in current.children:
            current.children[last_nibble] = Node()
        current.children[last_nibble].value = value
    
    # Calculate and return the root hash
    return HexBytes(keccak(encode_node(root)))


key_values = {
    "hello": "world",
    "key": "value",
    "test": "data"
}

root_hash = calculate_root(key_values)
print(root_hash.hex())
