def rreplace(string: str, find: str, replace: str, n_occurences: int) -> str:
    """
    Given a `string`, `find` and `replace` the first `n_occurences`
    found from the right of the string.
    """
    temp = string.rsplit(find, n_occurences)
    return replace.join(temp)

def transform_col(sr):
    sr.replace('', '0', inplace=True)
    sr = sr.str.strip()
    sr = sr.apply(lambda x: rreplace(x, '.', '', x.count('.')))
    return sr

def replace_double_quote(str):
    str = rreplace(str, "'", '"', str.count("'"))
    return str

def subtring_from_comma(str):
    try:
        i = str.index(',')
        str = str[0:i]
    except:
        pass
    return str