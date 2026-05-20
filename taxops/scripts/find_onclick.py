import re, pathlib, sys
for p in pathlib.Path('templates').rglob('*.html'):
    txt = p.read_text(encoding='utf-8')
    calls = set(re.findall(r'on(?:click|change|input|submit|keydown|keyup)=["\'](\w+)\s*\(', txt))
    if calls:
        print(p.name + ': ' + str(sorted(calls)))
