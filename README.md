# folly

folly is a technical analysis program, so named for outsmarting the market is surely 
a fools game.  folly will download stock updates from a number of online sources,
facilitate running technical analysis, compare stocks against each other and industries,
and more.  The program facilitates queries whereby you can describe characteristics
of curves of data values and how they might relate to others, and more.
See further below for usage. 


Usage: folly
                --debug                 Prints extraneous informaton not generally of use              

                --history_start=date    Sets the date that all stock history and data stores should start from
                --update_all            Updates all stock history and data stores to the latest date.
                --update_stocks         Updates only stock historical data, not datastores
                --update_stores         Updates all of the datastores using the stock_history we already have
                --update_right          Like update_all but only updates since most current
                                        data.  If no data does since our earliest data.
                --update_stocks_right   Like update_all but only updates since most current
                                        data.  If no data does since our earliest data.
                --update_today          Updates stocks with only the latest data including stock fundamentals.
                                        
                --desc_all              Displays summary information for available functions, datastores, industries, 
                                        stocks, etc.

                --DELETE_DATA           Deletes ALL data
                --delete_stock_data     Deletes all data from calc_store, stock_history, and stock
                --delete_store_data     Deletes all data from calc_store only
                --trim_left=date        Deletes all data from calc_store, stock_history and stock tables older 
                                        than supplied date
                --trim_right=date       Deletes all data from calc_store, stock_history and stock tables newer 
                                        than supplied date

                --data_integrity        NOT YET IMPLEMENTED (NYI) - Run tests to see that our data makes sense

                --fx=fxn_name           Run a named given calculation
                --fx_arg=fx_argument    Provide a given calculation arguments.  This is optional to the --fx argument
                --fx_data_t             The source data table to run fx on.  The default is stock_history, but table sources
                                        can include industry_history, or any calc_store tables, etc.  The default assumption
                                        is that the fx is run on the stock_history, close, and stock data type.  Using a derived
                                        data source (a calc_store) as a table source implies some knowledge of the data.
                --fx_data_c             The data table column to run fx on.  The default is close but it can be any named column.
                --fx_data_type          Stock, industry, etc.  If the fx is run on something besides stock it must be specified 
                                        as the data_id of values in the table is keyed from the data type source.
                ** 
                Some --fx* examples:
                folly --fx=moving_ave --fx_arg=30 --fx_data_c=percentage_change_volume
                    moving_ave fx run with an arg of 30 on the percentage_change_volume column of the default table (stock_history) 

                folly --fx=moving_ave --fx_arg=30 --fx_data_t=industry_history --fx_data_c=volume --fx_data_type=industry
                    moving_ave fx run with an arg of 30 on the volume column of the industry_history table where the data type is industry

                folly --fx=moving_ave --fx_arg=30 --fx_data_t=calc_store_67 --fx_data_c=value --fx_data_type=industry
                    moving_ave fx run with an arg of 30 on the value column of the calc_store_67 table where the data type is industry
                ** 

                --store=name            Provides a data store name for a given calculation.  This is optional for 
                                        the --fx argument.  Doing so causes the store to be written
                --store_list            Provides a listing of the current data stores
                --store_delete=store_id Deletes data stores identified by store_id

                --query                 Run query against data.  Parameter names should end with a numeric value so that related 
                                        parameters are grouped together and multiple groupings are possible (see examples below).

                    sq=                 NYI Name of saved query to include as a filter.
                    save=               NYI Save this query with provided name, the saved query may contain subqueries
                    window=             Time window measured in days for the query
                    
                    stock=              Specify whether to run query against a single stock, where value is stock
                    date=               Specify a specific date to run query on (in the form of month/day/year).

                    ds=                 Name of datastore
                    dsv=                Datastore value to search for.  An exact value to search for.  To define
                                        a range use dsvlt and dsvgt together.
                    dsvlt=              Less than this datastore value
                    dsvgt=              Greater than this datastore value

                    dsr=                Datastore search to use for a relative value search (see dsrv).  This
                                        requires a ds. In other words, the query is for dsr relative to ds by the specifier
                                        of dsrv.
                    dsrv=               Datastore relative value to search for.  This is a percentage value ranging
                                        from 1 to 100.
                    dsrvlt=             Less than this datastore relative value
                    dsrvgt=             Greater than this datastore relative value

                    TDrt=               The TD search is a (T)able(Direct) search where the behaviour is that of the dsr type
                                        of search except that the table, column, and corresponding data key (matching the data_id
                                        value of a calc_store_* table) must be specificied.  A ds is of course also required.
                                        TDt is the table.
                    TDrc=               The column of the TDt we are using.
                    TDrkey=             The name of the column in the TDt that is keyed with the data_id in a calc_store_* table.
                    TDrv=               The relative value to search for.  This is a percentage value.
                    TDrvlt=             Less than this relative value.
                    TDrvgt=             Greater than this relative value.

                    slopev=             A specific slope value to search for.  The slope query set requires a ds and optionally 
                                        a dsr to determine slope against.
                    slopelt=            A slope less than supplied value. 
                    slopegt=            A slope greater than supplied value.
                    slope_window=       A timeframe (in days) in which to measure the slope from the start and end points (with
                                        the end point being the matching data point).
                    slope_offset=       An optional offset from the end of the window to measure the slope of, measured as a
                                        percentage of the window size.  For example, if an offset of 20 is asked for and the
                                        window size is 100 the slope is calculated for the values defining ((DP - 100) -> (DP - 20)), 
                                        where DP is our matching data point.  Using this technique you can start more accurately
                                        describing curves as you can construct curves and characteristics of their segments to test for.


                    ** 
                    Some --query examples:
                    folly --query 
                         ds1=MovAve_50  dsr1=MovAve_30 dsrvlt1=90
                         Provides all dates for all stocks where the 30 day moving average is less than 90% of the 50 day moving average.
                         This is a single group of parameters.

                    folly --query  
                         ds1=Stock_Price_MovAve_90 slopegt1=.01 slopelt1=3 slope_window1=9 slope_offset1=10
                         Provides all dates for all stocks where the slope of the ds1 values are greater than .01 and less then 3, with the 
                         measured window of days being 9 and the percentage of days offset being 10.
                    
                    folly --query 
                         ds1=RSI_100  slopegt1=.1 slope_window1=10 
                         ds2=MovAve_50 slopegt2=.01 slope_window2=30 
                         ds3=ADX_15 dsvgt3=25 
                         ds4=RSI_100 dsvlt4=70 
                         ds5=RSI_100 slopegt5=0 slope_window5=2 
                         ds6=MovAve_30 dsr6=PSAR_100 dsrvlt6=100 
                         Provides all dates for all stocks where the RSI_100 ds has a slope greater than .1 with in a slope window of 10 days
                         and where the slope of the ds MovAve_50 has a slope greater than .01 with a window of 30 days
                         and where the value of ADX_15 is greater than 25
                         and where the value of RSI_100 is less than 70
                         and where the slope of RSI_100 is positive with a window of 2 days
                         and where the value of PSAR_100 is less than 100 percent of the MovAve_30 value

                    folly --query 
                         ds1=RSI_100 slopegt1=\"\-.1\" slope_window1=10 
                         ds2=RSI_100 slopelt2=0 slope_window2=2 
                         ds3=RSI_100 dsgt3=30 
                         ds4=MovAve_30 dsr4=PSAR_100 dsrvgt4=100
                         Provides all dates for all stocks where the RSI_100 ds has greater than -.1 10 day slope
                         and the 2 day slope for the same ds is negative 
                         and the RSI_100 ds values are greater than 30
                         and the PSAR_100 ds values is greater than the MovAve_30 ds values 

                    folly --query 
                         ds1=RSI_100  slopelt1=\"\-.1\" slope_window1=10  
                         ds2=MovAve_50 slopelt2=\"\-.01\" slope_window2=30 
                         ds3=ADX_100 dsvgt3=25 
                         ds4=RSI_100 dsvgt4=30 
                         ds5=MovAve_30 dsr5=PSAR_100 dsrvgt5=100 

                    folly --query 
                         ds1=RSI_100  slopegt1=\"\-.1\" slope_window1=10 
                         ds2=MovAve_50 slopelt2=\"\-.01\" slope_window2=30 
                         ds3=ADX_15 dsvgt3=25 
                         ds4=RSI_100 dsvgt4=30 
                         ds5=RSI_100 slopelt5=0 slope_window5=2 
                         ds6=MovAve_30 dsr6=PSAR_100 dsrvgt6=100 

                    folly --query  
                         ds1=MovAve_30 slopegt1=.5 slope_window1=15 stock=amzn date=1/16/2013
                    ** 

                    Notes:
                    The above is a filter, I want to be able to test/rate what comes through the filter.
                    Put the passers into a temporary table for processing/rating, dates and values go into table

                    Provide set of test parameters (predefined functions) to use for a given numbered query, for
                         example - rate of change/acccelaration, distance from X, etc., intersection of Y, whether
                         big or small number is desirous, weighting, normalizing (for example, make values relative
                         to 1?), concavity of curve, etc

                    Not all filter passers need ratings, only the very valuable ones
                    Test Parms:

               --dump_schema            Dump the database schema to a file titled folly.Schema.current_date.time.sql
               --dump_db                Dump the entire database to a file titled folly.DB.current_date.time.sql

