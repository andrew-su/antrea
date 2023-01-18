import yaml
with open("osstpmgt.yaml") as f:
    data = yaml.safe_load(f)
def convert(data):
    if isinstance(data, bytes):  return data.decode('ascii')
    if isinstance(data, dict):   return dict(map(convert, data.items()))
    if isinstance(data, tuple):  return map(convert, data)
    return data
d = convert(data)
with open("osstpmgt.yaml", "w") as f:
    yaml.dump(d, f)
