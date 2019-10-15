import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine


#readng the raw csvs 
raw_employees = pd.read_csv('/Users/shirleyxueyinghe/programming/barcelona_gse/databases/warehousing-operational-databases/homework/extracts/employees.csv')

raw_orders = pd.read_csv('/Users/shirleyxueyinghe/programming/barcelona_gse/databases/warehousing-operational-databases/homework/extracts/orders.csv')
raw_orders.rename(columns={'sales_rep_employee_number': 'employee_number'}, inplace=True)

raw_products = pd.read_csv('/Users/shirleyxueyinghe/programming/barcelona_gse/databases/warehousing-operational-databases/homework/extracts/products.csv')

#changing name of some columns that overlap e.g. employee_city and customer_city
overlap_cols = ['city', 'state', 'country']

def change_prefix(columns):
    for col in overlap_cols:
        raw_employees.rename(columns={col : 'employee_'+ col}, inplace=True)
        raw_orders.rename(columns={col: 'order_' + col}, inplace=True)
    return raw_employees
    return raw_orders

change_prefix(overlap_cols) 



#merging all csvs into one big table
full_joined_table = raw_orders.join(raw_products.set_index('product_code'), on=['product_code'], how='outer')\
    .join(raw_employees.set_index('employee_number'), on='employee_number', how='outer')\
    
    

full_joined_table['order_date'] = pd.to_datetime(full_joined_table['order_date'])

# Making new columns I want in star table and adding to the big merged table
full_joined_table['profit'] = full_joined_table['quantity_ordered'] * (full_joined_table['price_each'] - full_joined_table['buy_price'])
full_joined_table['total_sale_value'] = full_joined_table['quantity_ordered'] * full_joined_table['price_each']

full_joined_table['day_id'] = pd.to_datetime(full_joined_table['order_date']).dt.dayofweek
full_joined_table['quarter_id'] = pd.to_datetime(full_joined_table['order_date']).dt.quarter


# Building Dimension Tables
def build_dimension(star, name, index, columns):
    dim_index = name
    dim = star[columns].dropna(how='all').drop_duplicates().reset_index().rename_axis(dim_index).drop('index', axis=1).reset_index()
    return dim


customer_dim = build_dimension(full_joined_table, 'customer_id', 'customer_number', ['customer_number', 'customer_name', 'contact_last_name', 'contact_first_name', 'credit_limit'])

product_dim = build_dimension(full_joined_table, 'product_id','product_code', ['product_code', 'product_name', 'product_scale', 'product_description', 'html_description'] )

employee_dim = build_dimension(full_joined_table, 'employee_id', 'employee_number', ['employee_number', 'last_name', 'first_name', 'reports_to', 'job_title', 'employee_city', 'employee_state', 'employee_country', 'office_code'])

city_dim = build_dimension(full_joined_table, 'city_id', 'order_city', ['order_city', 'order_state', 'order_country'])


product_line_dim = full_joined_table[['product_line', 'product_code']].groupby('product_line').count().reset_index()
product_line_dim.rename(columns={'product_code':'product_line_id'}, inplace=True)
product_line_dim['product_line_id']= [0,1,2,3,4,5,6]

day_of_week_dim = build_dimension(full_joined_table, 'day_of_week', 'day_id', ['day_id', 'order_date'])
day_of_week_dim.drop('day_of_week', axis=1, inplace=True)     
day_of_week_dim['order_date'] = pd.to_datetime(day_of_week_dim['order_date'])
day_of_week_dim['day'] = day_of_week_dim['order_date'].dt.day_name()

    
    
quarter_dim = build_dimension(full_joined_table, 'quarter', 'quarter_id', ['quarter_id', 'order_date'])
quarter_dim.drop('quarter', axis=1, inplace=True)
quarter_dim['order_date'] = pd.to_datetime(quarter_dim['order_date'])

#making final star table by merging new columns I created into large merged table,
#then filtering and only taking the columns I want in the end
full_joined_table = full_joined_table.merge(day_of_week_dim, on=['order_date','day_id'], how='outer')
full_joined_table = full_joined_table.merge(quarter_dim, on=['order_date', 'quarter_id'], how='outer')
full_joined_table = full_joined_table.merge(product_line_dim, on=['product_line'], how='outer')
full_joined_table = full_joined_table.merge(customer_dim, on=['customer_number'], how='outer')
full_joined_table = full_joined_table.merge(product_dim, on=['product_code'], how='outer')
full_joined_table = full_joined_table.merge(city_dim, on='employee_city', how='outer')
full_joined_table = full_joined_table.merge(employee_dim, on='employee_number', how='outer')


cols_to_keep = ['customer_id',\
                'order_number',\
                'order_date',\
                'employee_id',\
                 'profit',\
                'total_sale_value',\
                'day_id',\
                'quarter_id',\
                'product_line_id', \
                'city_id', \
                'product_id'
               ]

final_star = full_joined_table[cols_to_keep].reset_index()
final_star['product_line_id'] = final_star['product_line_id']


# Checking for nulls and dropping
nulls_to_drop = list(final_star[final_star['profit'].isnull()].index)
nulls_to_drop

final_star.drop(nulls_to_drop, inplace=True)

# Pushing Data to Postgres


conn = psycopg2.connect("host=localhost dbname=shirley_homework user=postgres")
cur = conn.cursor()
engine = create_engine('postgresql://postgres: @localhost:5432/shirley_homework')

final_star.to_sql(name ='star', con=engine, index=False, if_exists='replace')
customer_dim.to_sql(name ='customer_dim', con=engine, index=False, if_exists='replace', index_label='customer_id')
product_dim.to_sql(name ='product_dim', con=engine, index=False, if_exists='replace', index_label='product_id')
product_line_dim.to_sql(name='product_line_dim', con=engine, index=False, if_exists='replace', index_label='product_line_id')
employee_dim.to_sql(name ='employee_dim', con=engine, index=False, if_exists='replace', index_label='employee_id')
city_dim.to_sql(name ='city_dim', con=engine, index=False, if_exists='replace', index_label='city_id')
day_of_week_dim.to_sql(name='day_of_week_dim', con=engine, index=False, if_exists='replace', index_label='day_id')
quarter_dim.to_sql(name='quarter_dim', con=engine, index=False, if_exists='replace', index_label='quarter_id')























