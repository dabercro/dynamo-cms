# Namespace-specific rules for e.g. object name conversions

from exceptions import ObjectError

def customize_dataset(Dataset):
    # Enumerator for dataset type.
    # Starting from 1 to play better with MySQL
    Dataset._data_types = ['unknown', 'align', 'calib', 'cosmic', 'data', 'lumi', 'mc', 'raw', 'test']
    for name, val in zip(Dataset._data_types, range(1, len(_data_types) + 1)):
        # e.g. Dataset.TYPE_UNKNOWN = 1
        setattr(Dataset, 'TYPE_' + name.upper(), val)

    Dataset._name_pattern = '/[^/]+/[^/]+/[^/]+'

def Block_to_internal_name(name_str):
    # block name format: [8]-[4]-[4]-[4]-[12] where [n] is an n-digit hex.
    return long(name_str.replace('-', ''), 16)

def Block_to_real_name(name):
    full_string = hex(name).replace('0x', '')[:-1] # last character is 'L'
    if len(full_string) < 32:
        full_string = '0' * (32 - len(full_string)) + full_string

    return full_string[:8] + '-' + full_string[8:12] + '-' + full_string[12:16] + '-' + full_string[16:20] + '-' + full_string[20:]        

def Block_to_full_name(dataset_name, block_real_name):
    return dataset_name + '#' + block_real_name

def Block_from_full_name(full_name):
    # return dataset name, block internal name

    delim = full_name.find('#')
    if delim == -1:
        raise ObjectError('Invalid block name %s' % full_name)

    return full_name[:delim], Block.to_internal_name(full_name[delim + 1:])

def customize_block(Block):
    Block.to_internal_name = Block_to_internal_name
    Block.to_real_name = Block_to_real_name
    Block.to_full_name = Block_to_full_name
    Block.from_full_name = Block_from_full_name

def customize_blockreplica(BlockReplica):
    BlockReplica._use_file_ids = False
