validity_map = {
    # Integer Types - Strict Sizing
    'int2': ['int16'],                # smallint, 2 bytes (-32768 to +32767)
    'int4': ['int32'],                # integer, 4 bytes (-2147483648 to +2147483647)
    'int8': ['int64'],                # bigint, 8 bytes (-9223372036854775808 to +9223372036854775807)
    
    # Floating Point Types - Strict Sizing
    'float4': ['float32'],            # real, 4 bytes, 6 decimal digits precision
    'float8': ['float64'],            # double precision, 8 bytes, 15 decimal digits precision
    'numeric': ['float64'],           # arbitrary precision, mapped to float64
    
    # Rest of types remain the same...
    'varchar': ['object', 'string'],
    'bpchar': ['object', 'string'],
    'text': ['object', 'string'],
    'bool': ['bool', 'boolean'],
    'date': ['datetime64[ns]'],
    'time': ['object'],
    'timetz': ['object'],
    'timestamp': ['datetime64[ns]'],
    'timestamptz': ['datetime64[ns, UTC]'],
    'json': ['object'],
    'jsonb': ['object'],
    'uuid': ['object'],
    
    # Geometric Types
    'point': ['object'],
    'line': ['object'],
    'lseg': ['object'],
    'box': ['object'],
    'path': ['object'],
    'polygon': ['object'],
    'circle': ['object'],
    
    # Arrays - strict type checking could be added for array contents
    '_int2': ['object'],
    '_int4': ['object'],
    '_int8': ['object'],
    '_float4': ['object'],
    '_float8': ['object'],
    '_numeric': ['object'],
    '_bool': ['object'],
    '_varchar': ['object'],
    '_text': ['object'],
    '_timestamp': ['object'],
    '_timestamptz': ['object'],
    '_json': ['object'],
    '_jsonb': ['object'],
    '_uuid': ['object'],
    '_point': ['object'],
    '_line': ['object'],
    '_lseg': ['object'],
    '_box': ['object'],
    '_path': ['object'],
    '_polygon': ['object'],
    '_circle': ['object'],
}