## this map represents the min an max range for each type of int ##
## that pandas uses
int_downcast_map = {
    'int16': {
        'min': -32768,
        'max': 32767
    },
    'int32': {
        'min': -2147483648,
        'max': 2147483647
    },
    'int64': {
        'min': -9223372036854775808,
        'max': 9223372036854775807
    }
}