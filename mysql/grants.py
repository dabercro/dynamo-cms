import os
import sys
import json

source_path = sys.argv[1]
target_path = sys.argv[2]

thisdir = os.path.dirname(os.path.realpath(__file__))

with open(source_path) as source:
    full_json = json.loads(source.read())

with open(thisdir + '/grants.json') as source:
    insertion = json.loads(source.read())

for user, block in insertion.items():
    if 'passwd' in block:
        full_json[user]['passwd'] = block['passwd']
    if 'hosts' in block:
        full_json[user]['hosts'] = list(set(full_json[user]['hosts'] + block['hosts']))
    if 'grants' in block:
        full_json[user]['grants'].extend(block['grants'])

with open(target_path, 'w') as target:
    target.write('{\n')
    user_blocks = []
    for user, block in full_json.items():
        user_blocks.append('  "%s": {\n' % user)
        user_blocks[-1] += '    "passwd": "%s",\n' % block['passwd']
        user_blocks[-1] += '    "hosts": %s,\n' % json.dumps(block['hosts'])
        user_blocks[-1] += '    "grants": [\n'
        user_blocks[-1] += ',\n'.join('      %s' % json.dumps(line) for line in block['grants'])
        user_blocks[-1] += '\n    ]\n'
        user_blocks[-1] += '  }'

    target.write(',\n'.join(user_blocks))
    target.write('\n}\n')
