import streamlit as st
import altair as alt
import os, sys
path = os.path.abspath('./controller')
sys.path.append(path)
from processing_data import *

st.set_page_config(
    page_title="Rau Cu Nam report",
    page_icon="✅",
    layout="wide",
)

st.title("Rau Cu Nam report")

placeholder = st.empty()

label_colors = {'condition': [], 'value': 'white'}  # The default value if no condition is met
    
with placeholder.container():
    st.markdown("### Tổng quan doanh thu, chi phí")
    hover = alt.selection_single(fields=["Sub-cate"],nearest=True,on="mouseover",empty="none")

    domain = ['GRAB', 'BAEMIN', 'CHI-PHI', 'CK-BAEMIN', 'CK-GRAB', 'CK-SP-FOOD', 'SP-FOOD', 'TAI-QUAN']
    range_ = ['#01A14B', '#4AC7C3', 'grey', '#4AC7C3', '#01A14B', '#E24A2C', '#E24A2C', 'grey']
    sub_cate_order = ['GRAB', 'SP-FOOD', 'BAEMIN', 'TAI-QUAN', 'CK-GRAB', 'CK-SP-FOOD', 'CK-BAEMIN', 'CHI-PHI']

    gp_st_finane = group_stacked_bar_chart_finance()

    fig = alt.Chart(gp_st_finane).mark_bar().encode(x=alt.X('Main-cate:N', title=None, axis=alt.Axis(labelColor=label_colors), 
                                                            sort=['DOANH-THU', 'CHI-PHI']), 
                                                    y=alt.Y('sum(giatri):Q', axis=alt.Axis(grid=True, title=None, values=list(range(0, 30000000, 1000000))), stack='zero'),
                                                    color=alt.Color('Sub-cate:N', scale=alt.Scale(domain=domain, range=range_), sort = sub_cate_order, legend=alt.Legend(title="Species by color")),
                                                    order = alt.Order('color_Sub-cate_sort_index:Q'),
                                                    tooltip=[alt.Tooltip("Sub-cate", title="Cate"),
                                                            alt.Tooltip("format-giatri", title="Value"),
                                                            alt.Tooltip("format-perctg", title="Pct")
                                                            ])

    text = alt.Chart(gp_st_finane).mark_text(opacity=0.5, color='white', align = 'center', baseline = 'bottom', dx = 0, dy=0).encode(
        x=alt.X('Main-cate:N', sort=['DOANH-THU', 'CHI-PHI']),
        y=alt.Y('total:Q', axis = alt.Axis(values=list(range(0, 30000000, 1000000)))),
        detail='Sub-cate:N',
        text=alt.Text('total:Q', format=',.0f')
    )
    
    st.altair_chart(alt.layer(fig, text).facet(column = alt.Column('NGAY:O', title=None, header=alt.Header(labelColor='white', labelOrient='top'))).configure_view(
    strokeWidth=0), use_container_width=False)
    
    fig_3 = alt.Chart(final_df).mark_line().encode(x = 'NGAY:O', y = 'giatri:Q', color = 'Sub-cate:N', strokeDash='Sub-cate:N')
    st.text(" ")
    st.text(" ")
    st.text(" ")
    st.altair_chart(fig_3, use_container_width=True)

