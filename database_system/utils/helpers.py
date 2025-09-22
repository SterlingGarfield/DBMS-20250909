import struct
from .constants import *

def serialize_int(value: int) -> bytes:
    """序列化整数"""
    try:
        result = struct.pack('>i', value)
        return result
    except:
        return b'\x00\x00\x00\x00'

def deserialize_int(data: bytes) -> int:
    """反序列化整数"""
    try:
        result = struct.unpack('>i', data)[0]
        return result
    except:
        return 0

def serialize_string(value: str, max_length: int) -> bytes:
    """序列化字符串"""
    try:
        encoded = value.encode('utf-8')
        if len(encoded) > max_length:
            encoded = encoded[:max_length]
        return encoded.ljust(max_length, b'\x00')
    except:
        return b'\x00' * max_length

def deserialize_string(data: bytes) -> str:
    """反序列化字符串"""
    try:
        # 去除尾部空字节
        end = len(data)
        while end > 0 and data[end-1] == 0:
            end -= 1
        return data[:end].decode('utf-8', errors='ignore')
    except:
        return ""

def serialize_bool(value):
    """序列化布尔值"""
    return b'\x01' if value else b'\x00'

def deserialize_bool(data):
    """反序列化布尔值"""
    return data != b'\x00'

def get_type_size(data_type, length=0):
    """获取数据类型的大小"""
    if data_type == INT_TYPE:
        return 4
    elif data_type == FLOAT_TYPE:
        return 8
    elif data_type == BOOL_TYPE:
        return 1
    elif data_type == STRING_TYPE:
        return length
    return 0