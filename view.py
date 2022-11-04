import streamlit as st
import altair as alt
import os, sys
import numpy as np
path = os.path.abspath('.')
sys.path.append(path)
from controller.processing_data import *
from controller.get_data import *

with open('config.json', "r", encoding='utf-8') as f:
    data = json.loads(f.read())

st.set_page_config(
    page_title="Rau Cu Nam report",
    page_icon="✅",
    layout="wide",
)

tab1, tab2 = st.tabs(["TRONG KỲ", "XUYÊN KỲ"])

with tab1:
    st.title("XEM BÁO CÁO THEO KỲ")

    placeholder = st.empty()

    label_colors = {'condition': [], 'value': 'white'}  # The default value if no condition is met
        
    with placeholder.container():
        st.markdown("### Chọn kỳ báo cáo")
        finance_data_name_list = get_finance_data_term_list()
        order_data_name_list = get_order_data_term_list()
        with st.form(key='form-chon-ky-bao-cao'):
            col1, col2 = st.columns(2)
            with col1:
                sld_finance_report = st.selectbox("Kỳ báo cáo tài chính", finance_data_name_list, index = len(finance_data_name_list)-1)
            with col2:
                sld_order_report = st.selectbox("Kỳ báo cáo đơn hàng", order_data_name_list, index = len(order_data_name_list)-1)
            submitted = st.form_submit_button('Thực hiện')
        df_finance = finalize_one_df_finance_by_term(sld_finance_report)
        df_order = finalize_one_df_order_by_term(sld_order_report)
        st.markdown("### Tổng quan doanh thu, chi phí")
        hover = alt.selection_single(fields=["Sub-cate"],nearest=True,on="mouseover",empty="none")

        domain = ['GRAB', 'BAEMIN', 'CHI-PHI', 'CK-BAEMIN', 'CK-GRAB', 'CK-SP-FOOD', 'SP-FOOD', 'TAI-QUAN']
        range_ = ['#01A14B', '#4AC7C3', 'grey', '#4AC7C3', '#01A14B', '#E24A2C', '#E24A2C', 'grey']
        sub_cate_order = ['GRAB', 'SP-FOOD', 'BAEMIN', 'TAI-QUAN', 'CK-GRAB', 'CK-SP-FOOD', 'CK-BAEMIN', 'CHI-PHI']

        revenue_cost_overal = revenue_cost_overal(df_finance)

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
        date_from, date_to, dthu_type = get_default_params_prfs(df_finance)
        
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
        percent_revenue_from_source = percent_revenue_from_source(df_finance, ds, de, options)
        fig_2 = alt.Chart(percent_revenue_from_source).mark_line().encode(x = 'Ngày:O', y = 'Tỷ-lệ-%:Q', color = 'Nguồn-doanh-thu:N', strokeDash='Nguồn-doanh-thu:N')
        st.text(" ")
        st.text(" ")
        st.text(" ")
        if de > ds:
            st.altair_chart(fig_2, use_container_width=True)
        get_statistic_prfs = get_statistic_prfs(percent_revenue_from_source, options)
        st.table(get_statistic_prfs.style.format({'Max (Tỷ lệ %)': '{:,.2f}', 'Min (Tỷ lệ %)': '{:,.2f}',
                                            'Avg (Tỷ lệ %)': '{:,.2f}'}))
        
        st.markdown("### Số lượng (đơn hàng) bán mỗi ngày")
        order_sale_every_day = order_sale_every_day(df_order)
        fig_4 = alt.Chart(order_sale_every_day).mark_line().encode(
        x = 'Ngày:O',
        y = 'SL hóa đơn:Q')
        st.altair_chart(fig_4, use_container_width=True)
        
        get_statistic_osed = get_statistic_osed(order_sale_every_day)
        st.table(get_statistic_osed.style.format({'Max (SL hóa đơn)': '{:,.0f}', 'Min (SL hóa đơn)': '{:,.0f}',
                                            'Avg (SL hóa đơn)': '{:,.2f}', 'Median (SL hóa đơn)': '{:,.0f}', 'Mode (SL hóa đơn)': '{:,.0f}'}))
        
        st.markdown("### Món bán chạy")
        date_from, date_to = get_default_params_bsd(df_order)
        with st.form(key='form-mon-ban-chay'):
            col7, col8 = st.columns(2)
            with col7:
                ds = st.date_input("Ngày bắt đầu", date_from, date_from, date_to)
            with col8:
                de = st.date_input("Ngày kết thúc", date_to, date_from, date_to)
            submitted = st.form_submit_button('Thực hiện')
        
        df_order_top, top_slider = top_slider(df_order, ds, de)
        col9, col10 = st.columns(2)
        with col9:
            top_quantity = st.slider("Top SL:", 1, top_slider, 10)
        with col10:
            top_revenue = st.slider("Top Doanh thu:", 1, top_slider, 10)
        top_dish_quantity, top_dish_revenue = top_seller_dish(df_order_top, top_quantity, top_revenue)
        col11, col12 = st.columns(2)
        with col11:
            st.write('Top món ăn theo số lượng bán')
            st.table(top_dish_quantity.style.format({'SL bán': '{:,.0f}', 'Đơn giá': '{:,.0f}',
                                            'Doanh thu': '{:,.0f}'}))
        with col12:
            st.write('Top món ăn theo doanh thu')
            st.table(top_dish_revenue.style.format({'SL bán': '{:,.0f}', 'Đơn giá': '{:,.0f}',
                                            'Doanh thu': '{:,.0f}'}))
        
        st.markdown("### Số lượng (món ăn) bán mỗi ngày")
        dish_list_res = dish_list(df_order)
        sltd_list = []
        with st.form(key='form-mon-ban-moi-ngay'):
            cols = st.columns(5)
            for i, col in enumerate(cols):
                sltd = col.selectbox('Chọn món', dish_list_res, key=i, index=len(dish_list_res)-1)
                sltd_list.append(sltd)
            submitted = st.form_submit_button('Thực hiện')
        dish_sale_every_day = dish_sale_every_day(df_order, sltd_list)
        
        if dish_sale_every_day[0] != 1:
            fig_3 = alt.Chart(dish_sale_every_day[1]).mark_line().encode(
            x = 'Ngày:O',
            y = 'SL bán:Q',
            color = 'Tên món:N',
            strokeDash='Tên món:N')
            st.altair_chart(fig_3, use_container_width=True)
        
        get_statistic_dsed = get_statistic_dsed(df_order, sltd_list)
        st.table(get_statistic_dsed.style.format({'Max (SL bán)': '{:,.0f}', 'Min (SL bán)': '{:,.0f}',
                                            'Avg (SL bán)': '{:,.2f}', 'Median (SL bán)': '{:,.0f}', 'Mode (SL bán)': '{:,.0f}'}))

with tab2:
    st.title("XEM BÁO CÁO XUYÊN KỲ")

    placeholder = st.empty()

    label_colors = {'condition': [], 'value': 'white'}  # The default value if no condition is met
        
    with placeholder.container():
        st.markdown("### Tỷ trọng doanh thu")
        finance_term_list = get_finance_data_term_list()
        
        with st.form(key='form-chon-cycle-ty-trong-dthu'):
            col1, col2 = st.columns(2)
            with col1:
                term = st.multiselect('Chọn kỳ', finance_term_list)
            submitted = st.form_submit_button('Thực hiện')
        
        with st.form(key='form-chon-nguon-ty-trong-dthu'):
            col3, col4 = st.columns(2)
            with col3:
                options = st.multiselect('Chọn nguồn doanh thu', dthu_type)
            submitted = st.form_submit_button('Thực hiện')
        
        st.markdown("### Món ăn")
        order_term_list = get_order_data_term_list()
        dish_list = get_order_data_dish_list()
        sltd_list = []
        metric_type_list = ['...', 'Tổng SL bán', 'Max SL bán', 'Min SL bán', 'Avg SL bán', 'Median SL bán', 
                            'Mode SL bán']
        
        with st.form(key='form-chon-cycle-mon-an'):
            col1, col2 = st.columns(2)
            with col1:
                term = st.multiselect('Chọn kỳ', order_term_list)
            cols = st.columns(5)
            for i, col in enumerate(cols):
                sltd = col.selectbox('Chọn món', dish_list_res, key=i+5, index=len(dish_list_res)-1)
                sltd_list.append(sltd)
            col3, col4 = st.columns(2)
            with col3:
                metric_type = st.selectbox("Loại thống kê", metric_type_list)
            submitted = st.form_submit_button('Thực hiện')
        
        if not term:
            term = [get_order_data_term_list()[-1]]
            
        if metric_type == '...':
            metric_type = 'Tổng SL bán'
        
        final_sltd_list = []
        for i in sltd_list:
            if i != '...':
                final_sltd_list.append(i)
        if len(final_sltd_list) == 0:
            final_sltd_list.append('Bún Thái')
            
        # list_df_order = finalize_list_df_order_by_term(term)
        # dish_list_res = dish_list(list_df_order)
        
        # with st.form(key='form-chon-mon-an'):
        #     cols = st.columns(5)
        #     for i, col in enumerate(cols):
        #         sltd = col.selectbox('Chọn món', dish_list_res, key=i+5, index=len(dish_list_res)-1)
        #         sltd_list.append(sltd)
        #     col3, col4 = st.columns(2)
        #     with col3:
        #         metric_type = st.selectbox("Loại thống kê", metric_type_list)
        #     submitted = st.form_submit_button('Thực hiện')

        # list_df_order_grouping_cycle = finalize_list_df_order_grouping_cycle(list_df_order, sltd_list)
        list_df_order_grouping_cycle = get_statistic_dish_by_cycle_data_from_db(term, final_sltd_list)
        list_df_order_grouping_cycle = markup_statistic_dish_by_cycle(list_df_order_grouping_cycle)
        
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
                        alt.Tooltip(metric_type, title="Value"),
                        alt.Tooltip('Cycle', title="Cycle"),
                        alt.Tooltip('Tên món', title="Tên món"),
                        alt.Tooltip(measure_delta[metric_type], title="Thay đổi")
                    ],
                )
                .add_selection(hover)
            )
            return (lines + points + tooltips).interactive()
        
        if not term or len(term) == 1:
            list_df_order_grouping_cycle = sort_df(list_df_order_grouping_cycle, metric_type)
            list_df_order_grouping_cycle = list_df_order_grouping_cycle[['Cycle', 'Tên món', metric_type]]
            st.table(list_df_order_grouping_cycle.style.format({metric_type: '{:,.0f}'}))
        else:
            # list_df_order_grouping_cycle = calculate_delta_measure_dish_by_cycle(list_df_order_grouping_cycle)
            # print(list_df_order_grouping_cycle[['Min SL bán', '% Min SL bán']].head(20))
            
            # if metric_type == 'Total SL bán':
            #     fig_4 = get_fig4_chart(list_df_order_grouping_cycle, 'Tổng SL bán')
            # if metric_type == 'Max SL bán':
            #     fig_4 = get_fig4_chart(list_df_order_grouping_cycle, 'Max SL bán')
            # if metric_type == 'Min SL bán':
            #     fig_4 = get_fig4_chart(list_df_order_grouping_cycle, 'Min SL bán')
            # if metric_type == 'Avg SL bán':
            #     fig_4 = get_fig4_chart(list_df_order_grouping_cycle, 'Avg SL bán')
            # if metric_type == 'Median SL bán':
            #     fig_4 = get_fig4_chart(list_df_order_grouping_cycle, 'Median SL bán')
            # if metric_type == 'Mode SL bán':
            #     fig_4 = get_fig4_chart(list_df_order_grouping_cycle, 'Mode SL bán')
            fig_4 = get_fig4_chart(list_df_order_grouping_cycle, metric_type)
            st.altair_chart(fig_4, use_container_width=True)