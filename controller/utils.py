import altair as alt

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

def get_line_chart(data, x, y, measure_delta, cate = None, sorting = False):
    hover = alt.selection_single(
        fields=[x],
        nearest=True,
        on="mouseover",
        empty="none",
    )
    if cate != None:
        if sorting == False:
            lines = alt.Chart(data).mark_line().encode(
                        x = x + ':O',
                        y = y + ':Q',
                        color = cate + ':N',
                        strokeDash = cate + ':N')
        else:
            lines = alt.Chart(data).mark_line().encode(
                        x = alt.X(x + ':O', sort = data[x].tolist()),
                        y = y + ':Q',
                        color = cate + ':N',
                        strokeDash = cate + ':N')
        tooltip=[
                alt.Tooltip(y, title=y),
                alt.Tooltip(x, title="Cycle"),
                alt.Tooltip(cate, title=cate),
                alt.Tooltip(measure_delta[y], title="Thay đổi")
            ]
    else:
        if sorting == False:
            lines = alt.Chart(data).mark_line().encode(
                        x = x + ':O',
                        y = y + ':Q')
        else:
            lines = alt.Chart(data).mark_line().encode(
                        x = alt.X(x + ':O', sort = data[x].tolist()),
                        y = y + ':Q')
        tooltip=[
                alt.Tooltip(y, title=y),
                alt.Tooltip(x, title=x),
                alt.Tooltip(measure_delta[y], title="Thay đổi")
            ]
    points = lines.transform_filter(hover).mark_circle(size=65)
    tooltips = (
        alt.Chart(data)
        .mark_rule()
        .encode(
            x = alt.X(x + ':O', sort = data[x].tolist()),
            y=y,
            opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
            tooltip=tooltip
        )
        .add_selection(hover)
    )
    return (lines + points + tooltips).interactive()
        
def get_fig4_chart(data, metric_type):
    hover = alt.selection_single(
        fields=["Cycle"],
        nearest=True,
        on="mouseover",
        empty="none",
    )
    lines = alt.Chart(data).mark_line().encode(
                x = 'Cycle:O',
                y = metric_type + ':Q',
                color = 'Tên món:N',
                strokeDash='Tên món:N')
    points = lines.transform_filter(hover).mark_circle(size=65)
    measure_delta = {'Tổng SL bán': '% Tổng SL bán', 'Max SL bán': '% Max SL bán', 'Min SL bán': '% Min SL bán',
                'Avg SL bán': '% Avg SL bán', 'Median SL bán': '% Median SL bán', 'Mode SL bán': '% Mode SL bán'}
    tooltips = (
        alt.Chart(data)
        .mark_rule()
        .encode(
            x="Cycle",
            y=metric_type,
            opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
            tooltip=[
                alt.Tooltip(metric_type, title=f"{metric_type}"),
                alt.Tooltip('Cycle', title="Cycle"),
                alt.Tooltip('Tên món', title="Tên món"),
                alt.Tooltip(measure_delta[metric_type], title="Thay đổi")
            ],
        )
        .add_selection(hover)
    )
    return (lines + points + tooltips).interactive()