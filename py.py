import logging
import pandas as pd


def data_quality_check(column_name):
    x = '000000'
    if column_name == 'STATE' or column_name == 'TERRITORY':
        x = 'Not Available'

    # check if column values is null or NaN
    if pd.isnull(df[column_name]).any():
        df[column_name].fillna(x, inplace=True)
        logging.warning('Value is Null')


months_list = []
years_list = []
quarter_list = []


def date_conversion(column_name):
    date = df[column_name].str.split(' ', expand=True)  # split from '0:00'
    mdy = date[0].str.split('/', expand=True)  # split into 3 columns -> month, day, year

    months_list.extend(mdy[0])  # add months to list
    years_list.extend(mdy[2])  # add years to list

    return months_list, years_list


def find_quarter(month_list):
    quarter = -1

    # check month and set quarter for every value in the list
    for i in range(0, len(month_list)):
        if 1 <= int(month_list[i]) <= 3:
            quarter = 1
        else:
            if 4 <= int(month_list[i]) <= 6:
                quarter = 2
            else:
                if 7 <= int(month_list[i]) <= 9:
                    quarter = 3
                else:
                    if 10 <= int(month_list[i]) <= 12:
                        quarter = 4
        quarter_list.append(quarter)

    return quarter_list


def check_date(column_name, lst):
    msg = ''

    # set msg for logging
    if column_name == 'QUATER_ID':
        msg = 'Quarter error'
    else:
        if column_name == 'MONTH_ID':
            msg = 'Month error'
        else:
            if column_name == 'YEAR_ID':
                msg = 'Year error'

    # check value in every column and print error if it's not the same as the one in the conversion
    for i, row in df.iterrows():
        if int(lst[i]) != row[column_name]:
            logging.error(msg)


def most_successful_to_csv(d_frame, c_name, gby):
    res_df = d_frame.groupby(gby)[['SALES']].sum().groupby(['COUNTRY', 'YEAR_ID']).agg({'SALES': 'max'})
    # res_df = d_frame.groupby(['COUNTRY', 'YEAR_ID']).agg({'SALES': 'max'})
    res_df.to_csv(c_name)


def total_sales(d_frame, c_name, gby, s, ok):
    # discount/surcharge
    d_s = d_frame['ORDER_QTY'] * (d_frame['MSRP'] - d_frame['UNIT_PRICE'])

    res_df = d_frame.groupby(gby)[s].sum()
    if ok == 1:
        res_df['DISCOUNT/SURCHARGE'] = d_s
    res_df.to_csv(c_name)


if __name__ == "__main__":
    df = pd.read_csv('sales.csv')

    # fill invalid data with defaults
    data_quality_check('POSTAL_CODE')
    data_quality_check('STATE')
    data_quality_check('TERRITORY')

    # date conversion correctness
    m_list, y_list = date_conversion('ORDER_DATE')
    q_list = find_quarter(m_list)
    check_date('QUATER_ID', q_list)
    check_date('MONTH_ID', m_list)
    check_date('YEAR_ID', y_list)

    # total sales, total quantity, total discount/surcharge for every country, state, status
    total_sales(df, 'total_sales_per_country.csv', ['COUNTRY', 'STATE', 'STATUS'], ['SALES', 'ORDER_QTY'], 1)

    # discount/surcharge
    disc_surc = df['ORDER_QTY'] * (df['MSRP'] - df['UNIT_PRICE'])

    # highest selling product_line in a country
    result_df = df.groupby(['COUNTRY', 'PRODUCT_LINE'])[['SALES', 'ORDER_QTY', 'PRODUCT_LINE']].sum().groupby(['COUNTRY']).agg({'SALES': 'max', 'ORDER_QTY': 'max'})
    result_df['DISCOUNT/SURCHARGE'] = disc_surc
    csv_name = 'highest_selling_product_line.csv'
    result_df.to_csv(csv_name)

    # most successful month for each year and country
    most_successful_to_csv(df, 'most_successful_month.csv', ['COUNTRY', 'YEAR_ID', 'MONTH_ID'])

    # most successful quarter for each year and country
    most_successful_to_csv(df, 'most_successful_quarter.csv', ['COUNTRY', 'YEAR_ID', 'QUATER_ID'])

    # total sales, total quantity, total discount/surcharge for every deal_size
    total_sales(df, 'total_sales_per_deal_size.csv', ['DEAL_SIZE'], ['SALES', 'ORDER_QTY'], 1)

    # total sales per month for each country and territory
    total_sales(df, 'total_sales_per_month_country_territory.csv', ['MONTH_ID', 'COUNTRY', 'TERRITORY'], ['SALES'], 0)

    # cumulative total sales per month
    total_sales(df, 'total_sales_per_month.csv', ['MONTH_ID'], ['SALES'], 0)
