## this map represents the min an max range for each type of float ##
## that pandas uses
float_downcast_map = {
    'float32': {  # Maps to PostgreSQL REAL (float4)
        'min': -3.4028235e+38,
        'max': 3.4028235e+38,
        'precision': 6  # ~6 decimal digits of precision
    },
    'float64': {  # Maps to PostgreSQL DOUBLE PRECISION (float8)
        'min': -1.7976931348623157e+308,
        'max': 1.7976931348623157e+308,
        'precision': 15  # ~15 decimal digits of precision
    }
}