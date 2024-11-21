import optparse
from functools import partial

import sqlprocessor as sp
from db import getDbConnection

from tkinter_util import *

import pandas as pd
import datetime
import time

class StockStatusCommandPane(tk.Frame):
    def __init__(self, parent, dbPane):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane

        self.processFrame = add_label_frame(parent, "Stock Status Plots Data Generation", 400, 100)

        add_buttons(self.processFrame,
                    data=[["Generate data", partial(self.process, 4), "blue", "Generate stock status plots data"]],
                    row=0, column=0, direction="horizontal")

        parent.add(self.processFrame)

        for child in self.processFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

    def process(self, entity_layer_id):
        if not self.dbPane.isConnectionTestedSuccessfully():
            popup_message("Connection not yet tested",
                          "The DB Connection has not been tested successfully.\n" +
                          "Once the DB Connection has been tested successfully, you can click the Generate button again.")
            return

        dbOpts = self.dbPane.getDbOptions()
        #dbSession = getDbConnection(optparse.Values(dbOpts)).getSession()
        rawConn = getDbConnection(optparse.Values(dbOpts)).getSession().connection().connection
        cursor = rawConn.cursor()

        start_time = time.time() 
        cursor.execute("select eez_id,name as eez_name from web.eez;")
        df_eez = pd.DataFrame(cursor.fetchall())
        df_eez.columns = ["eez_id","eez_name"]
        print("Generating stock status plots csv files")

        for index, row in df_eez.iterrows():
            stock_status_func = f"""
                with base1 as (
                    select 
                        f.year, 
                        f.taxon_key, 
                        sum(f.catch_sum)
                    from web.v_fact_data f
                    where f.marine_layer_id in (1,2) and f.main_area_id = {row["eez_id"]} and 
                    f.taxon_key not in (100000, 100011, 100025, 100039, 100047, 100058, 100077, 100139, 100239, 100339)
                    group by f.year, f.taxon_key
                ),
                fact(year, taxon_key, year_taxon_catch_sum) as (
                    select * from base1
                ),
                factwithsum(year, taxon, catch_sum) as (
                    select 
                        dim.year, 
                        dim.taxon_key, 
                        f.year_taxon_catch_sum::numeric(20)
                    from (select dt.time_business_key as year, taxon.taxon_key from web.v_dim_time dt, (select distinct f2.taxon_key from fact as f2) as taxon) as dim
                    left join fact f on (f.year = dim.year and f.taxon_key = dim.taxon_key)
                ),
                catch as (
                    select 
                        s.*, 
                        first_value(s.year) over (peak_window) as peak_year, 
                        first_value(s.catch_sum) over (peak_window) as peak_catch_sum,
                           (case 
                            when coalesce(s.catch_sum, 0) = 0 
                            then 0 
                            else sum(case when coalesce(s.catch_sum, 0) = 0 then 0 else 1 end) over (partition by s.taxon order by s.year::int rows between current row and 4 following) 
                            end)::int as positive_catch_count  
                    from factwithsum s
                    window peak_window as (partition by s.taxon order by case when coalesce(s.catch_sum, 0) = 0 then 0 else s.catch_sum end desc)
                ),
                taxon_filter(taxon, final_year) as (
                    --Only taxons having at least 1000 tonnes of catch, at least 10 years of reported landings and at least 5 years of consecutive catches are considered 
                    select 
                        c.taxon, 
                        max(c.year)
                    from catch c
                    where coalesce(c.catch_sum, 0) > 0 
                    group by c.taxon
                    having sum(c.catch_sum) >= 1000 and (max(c.year)::int - min(c.year)::int + 1) >= 10 and max(c.positive_catch_count) >= 5
                ), 
                --get_collated_data_for_stock_status
                base2 as (
                    select 
                        c.year, 
                        tf.taxon, 
                        c.catch_sum, 
                        c.peak_year, 
                        c.peak_catch_sum, 
                        tf.final_year,
                        first_value(c.year) over (post_peak_window) as post_peak_min_year,
                        first_value(c.catch_sum) over (post_peak_window) as post_peak_min_catch_sum
                    from catch c
                    join taxon_filter tf on (tf.taxon = c.taxon)
                    where coalesce(c.catch_sum, 0) != 0
                    window post_peak_window as (partition by tf.taxon order by case when c.year > c.peak_year then c.catch_sum else null end nulls last) 
                    order by tf.taxon, c.year
                ),
                categorized as (
                    select 
                        t.year, 
                        t.taxon, 
                        t.catch_sum,
                        (case 
                            when (t.peak_year = t.final_year) or ((t.year <= t.peak_year) and (t.catch_sum <= (t.peak_catch_sum * .50))) then 4
                            when t.catch_sum > (t.peak_catch_sum * .50) then 3
                            when (t.catch_sum > (t.peak_catch_sum * .10)) and (t.catch_sum < (t.peak_catch_sum * .50)) and (t.post_peak_min_catch_sum < (t.peak_catch_sum * 0.1)) and (t.year > t.      post_peak_min_year) then 5
                            when (t.catch_sum > (t.peak_catch_sum * .10)) and (t.catch_sum < (t.peak_catch_sum * .50)) and (t.year > t.peak_year) then 2
                            when (t.catch_sum <= (t.peak_catch_sum * .10)) and (t.year > t.peak_year) then 1
                            else 6
                         end) as category_id
                  from base2 t
                ),
                catch_sum_tally(category_id, year, category_catch_sum) as (
                    select 
                        t.category_id, 
                        t.year, 
                        sum(t.catch_sum)  
                    from categorized t
                    where t.category_id != 6
                    group by t.category_id, t.year
                ),
                stock_count(year, year_taxon_count, year_catch_sum) as (
                    select 
                        t.year, 
                        count(distinct taxon)::int, 
                        sum(t.catch_sum) 
                    from categorized t
                    where t.category_id != 6
                    group by t.year
                ),
                year_window(begin_year, end_year) as ( 
                    select 
                        min(t.year), 
                        max(t.year) 
                    from categorized t
                    where t.category_id != 6
                ),                
                sum_data(taxa_count, should_be_displayed) as (
                    select 
                    taxa_count,
                    case when 1 = 1 then case when t.cellCount > 30 and t.taxa_count > 10 then true else false end else true end 
                    from (select (select count(distinct taxon)::int from categorized) as taxa_count,
                           (select sum(a.number_of_cells) from web.area a where a.main_area_id = {row["eez_id"]} and a.marine_layer_id = 1) as cellCount) as t
                ),                                                                                                                 
                category_lookup as (
                    select 
                        u.category, 
                        u.ordinality as category_id
                    from unnest(web.f_stock_status_category_heading()) with ordinality as u(category)
                ),
                css as (
                    select 
                        c.category_id, 
                        v.year, 
                        v.value
                    from (select c.category_id,
                            (select (array_agg(t.time_business_key), array_agg(coalesce(val, 0.0)))::t_stock_status_year_value as yv_tuple
                              from web.time t
                              left join (select cst.year,
                                            (case 
                                                when cst.year in (yw.begin_year, yw.end_year) 
                                                then cst.category_catch_sum 
                                                else avg(cst.category_catch_sum) over(order by cst.year rows between 1 preceding and 1 following) 
                                                end) as val
                                            from catch_sum_tally cst
                                            join stock_count sc on (sc.year = cst.year)
                                            where cst.category_id = c.category_id) as v on (v.year = t.time_business_key)        
                            )
                            from category_lookup c
                            join year_window yw on (true)
                            join sum_data sd on (sd.should_be_displayed)
                            where exists (select 1 from catch_sum_tally cs where cs.category_id = c.category_id limit 1)) as c
                    join lateral unnest((c.yv_tuple).year, (c.yv_tuple).value) as v(year, value) on (true)
                ),
                css_sum_tally as (
                    select 
                        c.year, 
                        sum(c.value) as category_catch_sum
                    from css c
                    group by c.year
                ), 
                base3 as (
                    select 
                        'css' as data_set, 
                        c.category_id::int, 
                        c.year::int, 
                        case 
                            when cst.category_catch_sum is distinct from 0.0 
                            then (c.value*100.0/cst.category_catch_sum) else 0.0 
                        end as value
                    from css c
                    join css_sum_tally cst on (cst.year = c.year)
                    union all
                    select 
                        'nss' as data_set, 
                        c.category_id::int, 
                        v.year::int, 
                        v.value
                    from (select c.category_id,
                         (select (array_agg(t.time_business_key order by t.time_business_key), array_agg(coalesce(val, 0.0) order by t.time_business_key))::t_stock_status_year_value
                            from web.time t
                            left join (select cz.year, (count(*)*100.0/sc.year_taxon_count) val
                                        from categorized cz
                                        join stock_count sc on (sc.year = cz.year) 
                                        where cz.category_id = c.category_id
                                        group by cz.year, sc.year_taxon_count) as v on (v.year = t.time_business_key)        
                            ) as yv_tuple          
                    from category_lookup c
                    join sum_data sd on (sd.should_be_displayed)
                    where exists (select 1 from catch_sum_tally cs where cs.category_id = c.category_id limit 1)
                    order by c.category_id) as c 
                    join lateral unnest((c.yv_tuple).year, (c.yv_tuple).value) as v(year, value) on (true)
                )
                SELECT
                    data_set, year,
                    MAX("value"::numeric(8,2)) FILTER (WHERE category_id = '1') as "Collapsed",
                    MAX("value"::numeric(8,2)) FILTER (WHERE category_id = '2') as "Over-exploited",
                    MAX("value"::numeric(8,2)) FILTER (WHERE category_id = '3') as "Exploited",
                    MAX("value"::numeric(8,2)) FILTER (WHERE category_id = '4') as "Developing",
                    MAX("value"::numeric(8,2)) FILTER (WHERE category_id = '5') as "Rebuilding"
                FROM base3
                GROUP BY data_set, year
                order by data_set, year
            """
            cursor.execute(stock_status_func)
            df_stock_status = pd.DataFrame(cursor.fetchall(),columns=('data_set','year','collapsed','over-exploited','exploited','developing','rebuilding'))
            if (df_stock_status.shape[0]) != 0:
                print("For", row["eez_name"], "EEZ with row count",df_stock_status.shape[0])
                df_stock_status.to_csv(f'stock_status\{row["eez_name"]}_stock-status.csv',index=False)
            else:
                print("Skipping", row["eez_name"], "EEZ with row count",df_stock_status.shape[0])

        print("Saved in stock_status folder")
        end_time = time.time() # get end time
        total_time = end_time - start_time # calculate the time
        total_time_str = str(datetime.timedelta(seconds=total_time))
        print(f"Total running time: {total_time_str}") # print time
# ===============================================================================================
# ----- MAIN
if __name__ == "__main__":
    Application("Stock Status Data Generator", StockStatusCommandPane).run()
