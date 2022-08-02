import streamlit as st
import altair as alt
import os, sys
import numpy as np
path = os.path.abspath('./controller')
sys.path.append(path)
from processing_data import *
from get_data import *

st.set_page_config(
    page_title="Rau Cu Nam report",
    page_icon="✅",
    layout="wide",
)

st.title("XEM BÁO CÁO THEO KỲ")

placeholder = st.empty()

label_colors = {'condition': [], 'value': 'white'}  # The default value if no condition is met
    
with placeholder.container():
    st.markdown("### Chọn kỳ báo cáo")
    finance_data_name_list = get_finance_data_name_list()
    order_data_name_list = get_order_data_name_list()
    with st.form(key='form-chon-ky-bao-cao'):
        col1, col2 = st.columns(2)
        with col1:
            sld_finance_report = st.selectbox("Kỳ báo cáo tài chính", finance_data_name_list, index = len(finance_data_name_list)-1)
        with col2:
            sld_order_report = st.selectbox("Kỳ báo cáo đơn hàng", order_data_name_list, index = len(order_data_name_list)-1)
        submitted = st.form_submit_button('Thực hiện')
    df = finalize_one_df_finance(sld_finance_report)
    st.markdown("### Tổng quan doanh thu, chi phí")
    hover = alt.selection_single(fields=["Sub-cate"],nearest=True,on="mouseover",empty="none")

    domain = ['GRAB', 'BAEMIN', 'CHI-PHI', 'CK-BAEMIN', 'CK-GRAB', 'CK-SP-FOOD', 'SP-FOOD', 'TAI-QUAN']
    range_ = ['#01A14B', '#4AC7C3', 'grey', '#4AC7C3', '#01A14B', '#E24A2C', '#E24A2C', 'grey']
    sub_cate_order = ['GRAB', 'SP-FOOD', 'BAEMIN', 'TAI-QUAN', 'CK-GRAB', 'CK-SP-FOOD', 'CK-BAEMIN', 'CHI-PHI']

    revenue_cost_overal = revenue_cost_overal(df)

    fig = alt.Chart(revenue_cost_overal).mark_bar().encode(x=alt.X('Main-cate:N', title=None, axis=alt.Axis(labelColor=label_colors), 
                                                            sort=['DOANH-THU', 'CHI-PHI']), 
                                                    y=alt.Y('sum(giatri):Q', axis=alt.Axis(grid=True, title=None, values=list(range(0, 30000000, 1000000))), stack='zero'),
                                                    color=alt.Color('Sub-cate:N', scale=alt.Scale(domain=domain, range=range_), sort = sub_cate_order, legend=alt.Legend(title="Species by color")),
                                                    order = alt.Order('color_Sub-cate_sort_index:Q'),
                                                    tooltip=[alt.Tooltip("Sub-cate", title="Cate"),
                                                            alt.Tooltip("format-giatri", title="Value"),
                                                            alt.Tooltip("format-perctg", title="Pct")
                                                            ])

    text = alt.Chart(revenue_cost_overal).mark_text(opacity=0.5, color='white', align = 'center', baseline = 'bottom', dx = 0, dy=0).encode(
        x=alt.X('Main-cate:N', sort=['DOANH-THU', 'CHI-PHI']),
        y=alt.Y('total:Q', axis = alt.Axis(values=list(range(0, 30000000, 1000000)))),
        detail='Sub-cate:N',
        text=alt.Text('total:Q', format=',.0f')
    )
    
    st.altair_chart(alt.layer(fig, text).facet(column = alt.Column('NGAY:O', title=None, header=alt.Header(labelColor='white', labelOrient='top'))).configure_view(
    strokeWidth=0), use_container_width=False)
    
    st.markdown("### Tỷ trọng doanh thu")
    date_from, date_to, dthu_type = get_default_params_prfs(df)
    
    with st.form(key='form-ty-trong-dthu'):
        col3, col4 = st.columns(2) 
        with col3:
            ds = st.date_input("Ngày bắt đầu", date_from, date_from, date_to)
        with col4:
            de = st.date_input("Ngày kết thúc", date_to, date_from, date_to)
        col5, col6 = st.columns(2)
        with col5:
            options = st.multiselect('Chọn nguồn doanh thu', dthu_type)
        submitted = st.form_submit_button('Thực hiện')
    percent_revenue_from_source = percent_revenue_from_source(df, ds, de, options)
    fig_2 = alt.Chart(percent_revenue_from_source).mark_line().encode(x = 'Ngày:O', y = 'Tỷ-lệ-%:Q', color = 'Nguồn-doanh-thu:N', strokeDash='Nguồn-doanh-thu:N')
    st.text(" ")
    st.text(" ")
    st.text(" ")
    if de > ds:
        st.altair_chart(fig_2, use_container_width=True)
    get_statistic_prfs = get_statistic_prfs(percent_revenue_from_source, options)
    col7, col8 = st.columns(2)
    with col7:
        st.markdown(f'Ngày bắt đầu: {ds.strftime("%d/%m/%Y")}')
    with col8:
        st.markdown(f'Ngày kết thúc: {de.strftime("%d/%m/%Y")}')
    st.table(get_statistic_prfs.style.format({'Max': '{:,.2f}', 'Min': '{:,.2f}',
                                           'Avg': '{:,.2f}'}))
        
    st.markdown("### Món bán chạy")
    
    st.markdown("### Món ăn bán mỗi ngày")

    st.markdown("### Số lượng đơn bán mỗi ngày")
