# perl -wall

# Who we are
package Finance::devFolly;
require 5.005;

require Exporter;
use strict;

# What all we use
use DBI;
use Date::Parse;
use Date::Format;
use Finance::YahooQuoteSimonized;
use Finance::TickerSymbols;
use Finance::QuoteHist;
use Math::Business::ParabolicSAR;
use Math::Business::DMI;
use Math::Business::RSI;

# 
use vars qw($VERSION @EXPORT @ISA $debug $dbh);

$VERSION = '.1';

# Our functions
@ISA = qw(Exporter);
@EXPORT = qw(&change_history_start &update_stores &describe_all &delete_all &delete_stock_dates &delete_all_stores &trim_left &trim_right 
          &get_stock_id &all_stocks &stock_history_write &store_exist &store_write &store_list &store_delete &oldest_stock_date &newest_stock_date &yahoo_extended_mopup
          &oldest_calculation &newest_calculation &get_industry_stocks &get_industry_name &update_market_dates &integrity_check &delete_pink
          &ma &moving_ave &mytime &invoke_calc &check_for_active_session &get_today &get_fun &process_yq &check_pct &check_number &get_float &get_exchange
          &run_query $debug &dump_schema &dump_db &convert_cs &does_table_exist &get_percentage_delta &create_stock_percentage &update_industries &test_exchange);

# Our DB vars
my $db = "momo";
my $db_user = "momo";
my $db_pass = "secret";

# Open DB connection
$dbh = DBI->connect("DBI:mysql:$db", "$db_user", "$db_pass");

#############################
# General Data Subs
#############################
# These are general functions for dealing with stock data
# and derived data stores.

##
# Converts epoch time to month/day/year format
# Expects:
#    Epochtime
# Returns:
#    A good time      
##
sub mytime{
        my $ep_time = $_[0];
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($ep_time);
        $year = $year + 1900;
        $mon = $mon + 1;
        my $goodtime = "$mon/$mday/$year";
        return ($goodtime)
}


##
# Gets the csd_id and calls the generic_fx for calculations for every stock
# It writes a new datastore if the store name is provided and it doesn't already
# exist.  Otherwise it just runs calculation fx
# Expects:
#    A data function, function arguments, data source table, data source column, 
#    optionally a store name if writing store.  The defaults for data source and
#    column are stock_history and close.
# Returns:
#   Nothing, but will call generic_fx for every stock
##
sub invoke_calc {

     my $sub_fx = $_[0];
     my $sub_fx_arg = $_[1];
     my $sub_fx_data_t = $_[2];
     my $sub_fx_data_c = $_[3];
     my $sub_fx_data_type = $_[4];
     my $sub_store_name = $_[5];
     my $csd_id;

     if (defined $sub_store_name)
     {
          # We pass store name, function, and args to see if the store exists.
          # If it does exist the function will complain and exit, if not it will
          # write the $csd_id (calculation store descriptor id).
          # We need the $csd_id to write calculation values
          $csd_id = &store_exist($sub_store_name,$sub_fx,$sub_fx_arg,$sub_fx_data_t,$sub_fx_data_c);
     }

     if($sub_fx_data_type eq "stock")
     {
          # Get an array with all of our stocks
          my @stocks = &all_stocks;

          # For all of our data run the generic_fx function
          foreach(@stocks)
          {
               if(defined $debug)
               {	
                    print "Sub invoke_calc: data_source_id is $_\n";
               } 

               # Existence of $csd_id argument to generic_fx  means we are writing to a datastore 
               &generic_fx($_,$sub_fx,$sub_fx_arg,$sub_fx_data_t,$sub_fx_data_c,$csd_id);
          }
     }
     elsif($sub_fx_data_type eq "industry")
     {
          my @industries;

          # Get an array with all of our industries
          my @industries = &get_industry_id_list;

          # For all of our data run the generic_fx function
          foreach(@industries)
          {
               if(defined $debug)
               {	
                    print "Sub invoke_calc: data_source_id is $_\n";
               } 

               # Existence of $csd_id argument to generic_fx  means we are writing to a datastore 
               &generic_fx($_,$sub_fx,$sub_fx_arg,$sub_fx_data_t,$sub_fx_data_c,$csd_id);
          }

     }
}

##
# Updates historical stock data and derived datastores.  Provided a
# date it will scan the left and right side of the existing data and
# attempt to fill in whats missing.  Depending on the given argument
# to the function it may only do some subset of that.  
# Expects:
#    A new start ($new_start_date), and a job (one of update_stocks,
#    update_right, update_stocks_right). 
#    These are the ways in which it is currently invoked:
#    &change_history_start($history_start);
#    &change_history_start($oldest_stock_date);
#    &change_history_start($oldest_stock_date,"update_stocks");
#    &change_history_start($oldest_stock_date,"update_right");
#    &change_history_start($oldest_stock_date,"update_stocks_right");
# Returns:
#    Nothing
##
#############################
sub change_history_start {


# Before requesting yahoo extended, First check if a normal weekday, if yes then do a "pre-query" using
# historical method and see if you get a response for a couple of stocks on given date, and if yes then
# request full dump of fundamentals


     # If no start date given just add to the right side for all data otherwise
     # add to both right and left
        
     # Our new date to work with
     my $new_start_date = $_[0];
     # Our specific task
     my $arg = $_[1];

     # Initializing vars
     my $update_stocks_only = undef;
     my $right_only = undef;

     # Let's see what our assignment is
     if($arg eq "update_stocks")
     {
          $update_stocks_only = 1;
     }
     if($arg eq "update_right")
     {
          $right_only = 1;
     }
     if($arg eq "update_stocks_right")
     {
          $update_stocks_only = 1;
          $right_only = 1;
     }

     # Convert our new start date to epoch time
     my $new_epoch_start_date = str2time($new_start_date);

     # A status indicator describing whether a successful write was done to stock history
     my $written_to;

     # Used to mark a stock as new if new
     my $new_stock = 0;

     # Date vars of existing stocks
     my $epoch_newest_date;
     my $epoch_newest_date_plus_one;

     # What this silly little time trick does is give us our epoch time for the beginning of the day
     # I do this in case any of the times I receive are not at the exact beginning.  Having the exact
     # epochtime for a given day is important for numerous other calculations
     my $now = str2time(&mytime(time));

     # Now for each $industry (get list of industries), so that we can get our list of stocks
     foreach my $industry ( industries_list()) {
          print "#########################################\n";
          print "Processing updates for $industry stocks\n";
          print "#########################################\n";

          # Our industry id in the DB
          my $industry_id;

          # Let's see if we already have this industry registered
          my $query = "select id from industry where name=?";
          my $query_handle = $dbh->prepare($query);
          $query_handle->execute($industry);
          while (my @data = $query_handle->fetchrow_array())
          {
               # We already know about this industry
               $industry_id = $data[0];
               if(defined $debug)
               { 
                    print "Sub change_history_start: industry is $industry_id\n";
               }
          }
          if(not defined $industry_id)
          {
               # This is a new industry to us so let's register it
               print "Registering Industry: $industry\n";
               $query = "insert into industry (name) values (?)";
               my $query_handle = $dbh->prepare($query);
               $query_handle->execute($industry);
               # and assign $industry_id 
               my @row = $dbh->selectrow_array('select last_insert_id()');
               $industry_id =  $row[0];
          }

#Mark#2
          # Now we are iterating through this industry getting the stocks that are part of it
          foreach my $symbol ( industry_list( $industry ) ) {
               # Let's just forget about the pink sheet stuff
               unless(($symbol =~ /.*\.(PK|OB)$/i))
               {
                    if(&get_exchange($symbol,"major_us"))
                    {
                         # Let's simplify this stuff.  Providing a stock and a range these are our options:
                         # 1. New stock
                         # 2. An old stock with no recorded data
                         # 3. An old stock with new left hand data
                         # 4. An old stock with new right hand data
                         # 5. An old stock with new right and left hand data

                         # Declare vars
                         my $we_have_data = undef;
                         # Dates to use for querying yahoo
                         my $yahoo_start_date = undef;
                         my $yahoo_end_date = undef;
                         # Date ranges of what we currently have
                         my $epoch_oldest_stock_date = undef; 
                         my $epoch_newest_stock_date = undef;

                         # Let's see if we already have this stock registered
                         my $query = "select id from stock where symbol=?";
                         my $query_handle = $dbh->prepare($query);
                         $query_handle->execute($symbol);
                         my $stock_id = undef;
                         while (my @data = $query_handle->fetchrow_array())
                         {
                              # We already know about this stock
                              $stock_id = $data[0];
                         }

                         if(not defined $stock_id){

                              # This takes care of case #1

                              # New stock
                              print "Registering Stock: $symbol\n";
                              # Let's make a note that this stock is new for the
                              # purpose of getting data for the right-hand side (below)
                              $new_stock = 1;
                              # This stock does not exist in DB yet so register it
                              $query = "insert into stock (symbol,industry_id,status) values (?,?,?)";
                              my $query_handle = $dbh->prepare($query);
                              $query_handle->execute($symbol,$industry_id,"active");

                              # Get our new stock_id
                              my @row = $dbh->selectrow_array('select last_insert_id()');
                              $stock_id =  $row[0];
                              $yahoo_start_date = $new_start_date;
                              # Our end date is today since the stock is new to the db
                              $yahoo_end_date = &mytime($now);

                         }else{
                              # Let's see if we have data for this stock
                              my $query = "select * from stock_history where data_id=? limit 1";
                              my $query_handle = $dbh->prepare($query);
                              $query_handle->execute($stock_id);
                              while (my @data = $query_handle->fetchrow_array())
                              {
                                   $we_have_data = $data[0];
                                   # We already stock data (not all stocks do)
                              }

                              if(defined $we_have_data)
                              {
                                   # If we are here then we have data.  Get the oldest date.
                                   $epoch_oldest_stock_date = &oldest_stock_date($stock_id,"stock_history"); 
          ### Why are we subtraacting a day?
                                   $epoch_oldest_stock_date = $epoch_oldest_stock_date - 86400; 
                                   $epoch_newest_stock_date = &newest_stock_date($stock_id,"stock_history"); 

                              }
                              else
                              {
                                   # This takes care of case #2
                                   # This stock is registered but without data
                                   $yahoo_start_date = $new_start_date;
                                   $yahoo_end_date = &mytime($now);
                              }
                         }
                         # If our new start range starts after what we already have say so
                         if(((defined $we_have_data) and ($new_epoch_start_date >= $epoch_oldest_stock_date)) and (not defined $right_only))
                         {
                              print "There is already data for $symbol older than the start date provided\n";
                         }elsif((defined $we_have_data) and ($new_epoch_start_date <= $epoch_oldest_stock_date)){
                              # This is the left hand gap (the new date -> the current oldest date) 
                              $yahoo_start_date = &mytime($new_epoch_start_date);
                              $yahoo_end_date = &mytime($epoch_oldest_stock_date);
                         }

                         # If we have good start/end dates and 
                         # 1. we are either not called to do right side only, or
                         # 2. we are a new stock, or 
                         # 3. we currently have no data
                         if(((defined $yahoo_start_date) and (defined $yahoo_end_date)) and ((not defined $right_only) or (defined $new_stock) or (not defined $we_have_data)))
                         {
                              # At this point we have our Yahoo $start_date and $end_date
                              if(defined $debug)
                              {
                                        print "\n";
                                        print "In change_history_start: $industry - $symbol\n";
                                        print "In change_history_start: Left Hand Side\n";
                                        print "In change_history_start: Dates: $yahoo_start_date - $yahoo_end_date , Stock_id = $stock_id\n";
                                        print "\n";
                              }
                              print "Updating (left-hand) stock history table for $symbol ($stock_id) ($yahoo_start_date - $yahoo_end_date)\n";

                              # Let's get the status of the write.  If a success we will also update the data stores (down below)
                              $written_to = &stock_history_write($symbol, $stock_id, $yahoo_start_date, $yahoo_end_date);
                         }

                         # Right-hand side data -> We already have data
                         if(($new_stock ne 1) and (defined $we_have_data))
                         {
                              # Get date of most current data point for stock
                              $epoch_newest_date = &newest_stock_date($stock_id,"stock_history");
                              # We are adding a day here so that we can test if it is older than today by a day
                              $epoch_newest_date_plus_one = $epoch_newest_date + 86400;

                              # If most current data point on record is more than 1 day old do a write
                              if($epoch_newest_date_plus_one < $now)
                              {
                                   # Setting out yahoo start date to be one day later than our newest date 
                                   my $yahoo_start_date = &mytime($epoch_newest_date_plus_one);
                                   my $yahoo_end_date = &mytime($now);

                                   if(defined $debug)
                                   {
                                           print "\n";
                                           print "In change_history_start: $industry - $symbol\n";
                                           print "In change_history_start: Right Hand Side\n";
                                           print "In change_history_start: Dates: $yahoo_start_date - $yahoo_end_date, Stock_id = $stock_id\n";
                                           print "\n";
                                   }

                                   # I think this should never happen
                                   unless($yahoo_start_date eq $yahoo_end_date)
                                   {
                                        print "Updating (right-hand) stock history table for $symbol ($yahoo_start_date - $yahoo_end_date)\n";
                                        $written_to = &stock_history_write($symbol, $stock_id, $yahoo_start_date, $yahoo_end_date);
                                   }
                              }
                         }

                         # If we successfully wrote stock history and we only doing stock history updates then do stores too
                         if(($written_to eq "success") and (not defined $update_stocks_only))
                         {
                              &update_stores($stock_id,"stock_history");
                                 
                              # Let's undefine this for the next go around
                              undef $written_to;
                         }

                         # Let's undefine this for the next go around
                         undef $new_stock;
                    }
               }
          } 
     }

     # If we doing right things only then get_today
     if((not defined $update_stocks_only) and (defined $right_only))
     {
          # This function gets today's extended stock info (no history is available) for all stocks
          # It deletes todays record if there is any and writes the extended
          &get_today;
     }

     # Finally, let us update our list of valid market dates
     &update_market_dates;

     &create_stock_percentage("price");
     &create_stock_percentage("volume");
     # Update our industry data
     print "Updating Industry Historical Data\n";
     &update_industries(undef,undef);

     print "Updating Industry Data Stores\n";
     # Get an array with all of our industries
     my @industries = &get_industry_id_list;

     # For all of our data run the generic_fx function
     foreach my $indu_id (@industries)
     {
          &update_stores($indu_id,"industry_history");
     }

}

##
# This updates all calculation stores for a given stock or industry.  It gets a list of all stores, their invoking
# functions and arguments, and then calls the generic_fx for the stock and functions.
# Expects:
#    A data id - which is commonly a stock id
# Returns:
#    Nothing 
##
sub update_stores
{
     # Our data_id
     my $data_id = $_[0];

     # Our data type (stock, industry, etc)
     my $data_type = $_[1];

     # A listing of our datastores, their fx names, and their fx arguments
     my %stores;
     %stores = &store_list($data_type,\%stores);
     my @sortorder = sort keys %stores;
     foreach my $store_id (@sortorder)
     {
          my @a = split(/:::/,$stores{$store_id});
          #noomer
          # Here $a[1] is the name of our function
          ## $stock_id is that
          # $data_id is that
          # $a[2] is our function args
          # $a[3] is our data table source 
          # $a[4] is our data table column source
          # $store_id is $csd_id

          if(defined $debug)
          {
               print "Sub update_stores: sub &$a[1]($data_id,$a[2],$store_id)\n";
               print "Sub update_stores: For data_id: $data_id\n In function update_stores: $store_id: $a[0] (fx=$a[1] fx_args=$a[2])\n"; 
          }
          my $fx = $a[1];
          my $fx_args = $a[2];
          my $table = $a[3];
          my $column = $a[4];
          print "DataID: $data_id - Checking and updating records for datastore $a[0]\n";
          &generic_fx($data_id,$fx,$fx_args,$table,$column,$store_id);
     }

}

##
# Prints descriptive info about the different types of data we currently have.
# Describe functions (showing fx, fx_args), data_stores (showing date range), stocks (number of, 
# date ranges), industries (number of stocks in), DOWN THE ROAD SHOW SECTOR 
# Expects:
#    Nothing
# Returns:
#    Nothing, just prints directly from function 
# v1 2
##
sub describe_all
{
     # The various things to report on:          
     # Total number of industries
     # Total number of data stores
     # Total number of functions
     # Total number of stocks
     # Functions, Function args
     # Data_stores, date range
     # Industries, number of stocks in
     # Stock, sector it is in, date range

     print "#############################\n";
     print "#    Folly Summary Data     #\n";
     print "#############################\n\n";

     # Industry info
     my $total_industries;
     my $query = "select count(*) from industry";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          $total_industries = $data[0]; 
     }
     print "Total industries: $total_industries\n";

     # Data store info
     my $total_stores;
     $query = "select count(*) from calc_store_descriptor";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          $total_stores = $data[0]; 
     }
     print "Total data stores: $total_stores\n";

     # Function info
     my $total_fxs;
     $query = "select count(distinct fx_name) from calc_store_descriptor;";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          $total_fxs = $data[0]; 
     }
     print "Total registered functions: $total_fxs\n";

     # Stock info
     my $total_stocks;
     $query = "select count(*) from stock";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          $total_stocks = $data[0]; 
     }
     print "Total stocks: $total_stocks\n";


     # List functions
     print "\n#############################\n";
     print "# Functions \n";
     print "#############################\n\n";

     my $fx_name;
     my $i = 0;
     $query = "select distinct fx_name from calc_store_descriptor;";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          $i++;
          $fx_name = $data[0]; 
          print "$i. $fx_name\n";

     }
     # List data stores, providing fx, fx args, and date range that we have data
     print "\n#############################\n";
     print "# Datastores\n";
     print "#############################\n\n";

     $i = 0;
     $query = "select * from calc_store_descriptor";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          $i++;

          my $oldest_calc = &mytime(&oldest_calculation($data[0]));
          my $newest_calc = &mytime(&newest_calculation($data[0]));
          print "$i. $data[1] - Function: $data[2], Function args: $data[3] Date range: $oldest_calc -> $newest_calc \n";
     }

     # List industry count, including number of stocks in each
     print "\n#############################\n";
     print "# Industries\n";
     print "#############################\n\n";

     $i = 0;
     $query = "select * from industry order by name";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          my $number_of_stocks = &get_industry_stocks($data[0]);
          $i++;
          print "$i. $data[1](industry id:$data[0]) - $number_of_stocks stocks\n"; 
     }

     # List stock, industry it is in, date range
     print "\n#############################\n";
     print "# Stocks\n";
     print "#############################\n\n";

     $i = 0;
     $query = "select * from stock order by industry_id desc";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          my $oldest_stock_history = &mytime(&oldest_stock_date($data[0],"stock_history"));
          my $newest_stock_history = &mytime(&newest_stock_date($data[0],"stock_history"));
          my $industry = &get_industry_name($data[2]);
          $i++;
          print "$i. $data[1]\t(stock id: $data[0], Industry: $industry)\tDate range: $oldest_stock_history -> $newest_stock_history\n"; 
     }

}

##
# Delets all stocks, datastore, industry info, the works... 
# Expects:
#    Nothing 
# Returns:
#    Nothing 
##
sub delete_all 
{

# When we have sector data include that as well
     print "Deleting all stock, calculation, and industry data - This may take a while...\n";

     &delete_stock_datas;

     my $query = "delete from industry";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
}


##
# Deletes all stock data and calculations
# Expects:
#     Nothing
# Returns:
#     Nothing
#    v1 3
##
sub delete_stock_datas
{
# MIGHT BE GOOD TO ALLOW FOR THESE FUNCTIONS TO BE ABLE TO DELETE SINGLE STOCK

#Mark#3
# When we have sector data include that as well
     # Delete stores
     my $total_stock_calcs;
     my $query = "select count(*) from calc_store";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          $total_stock_calcs = $data[0]; 
     }
     print "Deleting Stock Calculations: $total_stock_calcs records\n";
     $query = "delete from calc_store";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 

     # Delete stock histories
     my $total_stock_history;
     $query = "select count(*) from stock_history";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          $total_stock_history = $data[0]; 
     }
     print "Deleting Historical Stock Data: $total_stock_history records\n";
     $query = "delete from stock_history";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 

     # Delete actual stock records
     my $total_stocks;
     $query = "select count(*) from stock";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          $total_stocks = $data[0]; 
     }
     print "Deleting Stock Entries: $total_stocks records\n";
     $query = "delete from stock";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 

}

##
# Deletes all data stores, first from calc_store then from calc_store_descriptor
# Expects:
#    Nothing
# Returns:
#    Nothing 
#    v1 3
##
sub delete_all_stores
{
     my $total_stock_calcs;
     my $query = "select count(*) from calc_store";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while (my @data = $query_handle->fetchrow_array())
     {
          $total_stock_calcs = $data[0]; 
     }
     print "Deleting Stock Calculations: $total_stock_calcs records\n";

     $query = "delete from calc_store";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 

     $query = "delete from calc_store_descriptor";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
}


##
# Reduce data set on the left side (old)
# Expects:
#    Our oldest date of stocks
# Returns:
#    Nothing      
#    v1 2
##
sub trim_left
{
#Mark#4
     # Our new left (oldest) limit to our data date range
     my $new_left_date = $_[0];

     # Convert to epoch_time
     my $new_epoch_left_date = str2time($new_left_date);

     # Delete from our calc_store where date is less than new time
     my $query = "delete from calc_store where date < ?";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($new_epoch_left_date); 
     my $rownumber = $query_handle->rows;
     print "Deleted $rownumber records from calculation stores\n";

     # Delete from our stock_history where date is less than new time
     $query = "delete from stock_history where date < ?";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute($new_epoch_left_date); 
     $rownumber = $query_handle->rows;
     print "Deleted $rownumber records from stock history\n";
}


##
# Reduce data set on right side (newer)
# Expects:
#    Our oldest date of stocks
# Returns:
#    Nothing 
#    v1 2
##
sub trim_right
{
#Mark#4
     # Our new right (newest) limit to our data date range
     my $new_right_date = $_[0];

     # Convert to epoch_time
     my $new_epoch_right_date = str2time($new_right_date);

     # Delete from our calc_store where date is greater than new time
     my $query = "delete from calc_store where date > ?";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($new_epoch_right_date); 
     my $rownumber = $query_handle->rows;
     print "Deleted $rownumber records from calculation stores\n";

     # Delete from our stock_history where date is greater than new time
     $query = "delete from stock_history where date > ?";
     $query_handle = $dbh->prepare($query);
     $query_handle->execute($new_epoch_right_date); 
     $rownumber = $query_handle->rows;
     print "Deleted $rownumber records from stock history\n";

}
        

##
# Provides a listing of the currently registered stocks, either in id or symbol format
# Excludes Pink sheet or over the counter stocks
# Expects:
#    An option specifying whether id or symbol should be provided 
# Returns:
#    An array with stocks in id or symbol format 
##
sub all_stocks
{
	my $option = $_[0];
     my @substocks;
     my $query;

     if ($option eq "byname")
     {
	     $query = "select symbol from stock where status='active' and symbol not like '%.PK' and symbol not like '%.OB'";
     }else{
	     $query = "select id from stock where status='active' and symbol not like '%.PK' and symbol not like '%.OB'";
     }
	my $query_handle = $dbh->prepare($query);
	$query_handle->execute();
	while (my @data = $query_handle->fetchrow_array())
       	{
		push(@substocks, $data[0]); 
	}
	return @substocks;
}

##
# Provided a stock symbol and id, start and end dates, this fx queries yahoo historical data and
# feeds it to the database.
# Expects:
#    Stock symbol
#    Stock ID
#    Start date
#    End date
# Returns:
#    The statement "success" or "none" 
##
sub stock_history_write
{
     # Our args
     my $sub_symbol = $_[0];
     my $sub_stock_id = $_[1];
     my $sub_start_date = $_[2];
     my $sub_end_date = $_[3];

     # Some vars
     my $row;
     my $success = 0;

     # Converting to epoch time
     my $epoch_start_date = str2time($sub_start_date);
     my $epoch_end_date = str2time($sub_end_date);

     # Create our new Finance object 'q'
     my $sub_q = Finance::QuoteHist->new
     (
     symbols    => $sub_symbol,
     start_date => $sub_start_date, # '6 months ago' or  '1 year ago', see Date::Manip
     end_date   => $sub_end_date,
     );

     # Work with our 'sub_q'
     foreach $row ($sub_q->quotes()) 
     {
          # Break up the data into chunks
          my ($symbol, $date, $open, $high, $low, $close, $volume) = @$row;
          # Change date to epoch time
          my $epoch_date = str2time($date);
          # Do the below time range check because sometimes we get out of range values that 
          # are surprising!
          if(($epoch_date >= $epoch_start_date) and ($epoch_date <= $epoch_end_date))
          {
               # Add to db 
               my $query = "insert into stock_history (data_id,date,open,high,low,close,volume) values (?,?,?,?,?,?,?)";
               my $query_handle = $dbh->prepare($query);
               $query_handle->execute($sub_stock_id,$epoch_date,$open,$high,$low,$close,$volume);
               if(defined $debug)
               {
                    print "Sub stock_history_write: Stock ID: $sub_stock_id, Date: $epoch_date, Open: $open, High: $high, Low: $low, Close: $close, Volume: $volume\n";
               }
          }
          $success = 1;
     }

     if($success eq 1)
     {
          return("success");
     }
     else
     {
          return("none");
     }
}

#############################
# Store Subs
#############################


##
# This accepts an arbitrary new store name and creates the calc_store_descriptor entry
# Expects:
#    A store name  
#    A fx name (this must already be defined in code)
#    fx argments
#    Data source table
#    Data source table column
# Returns:
#    The calc_store_descriptor_id of the newly created store 
##
sub store_exist
{
     # Provided store name
     my $sub_store_name = $_[0];
     # Provided fx name
     my $sub_fx_name = $_[1];
     # Provided fx arguments
     my $sub_fx_arg = $_[2];
     # Our data source table
     my $sub_fx_data_t = $_[3];
     # Our data source column 
     my $sub_fx_data_c = $_[4];

     # Var
     my $csd_id;

     # Look for something with all of our above args
     my $query = "select id from calc_store_descriptor where calc_store_name=? and fx_name=? and fx_arg=?";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($sub_store_name, $sub_fx_name, $sub_fx_arg);
     # If we get something then this data store is already present
     while (my @data = $query_handle->fetchrow_array())
     {
          $csd_id = $data[0];
     }
     # If we have it, complain and exit
     if(defined $csd_id)
     {
          print "The data store \"$sub_store_name\" (fx=$sub_fx_name, fx_args=$sub_fx_arg) already exists\n";	
          exit;
     }
     # It is not present so let's register this store descriptor
     #Mark#5
     else
     {
          my $query = "insert into calc_store_descriptor (calc_store_name, fx_name, fx_arg, source_t, source_c) values (?,?,?,?,?)";

          my $query_handle = $dbh->prepare($query);
          $query_handle->execute($sub_store_name,$sub_fx_name,$sub_fx_arg,$sub_fx_data_t,$sub_fx_data_c);
          # Our last last_insert_id is our $csd_id
          my @row = $dbh->selectrow_array('select last_insert_id()');
          my $csd_id =  $row[0];

          print "making table calc_store_$csd_id\n";
          my $make_table_query = "create table calc_store_$csd_id 
                    (id int not null auto_increment, 
                    data_id int(11), 
                    date int(11), 
                    value decimal(10,2),
                    index `data_id_index`(data_id),
                    index `date_index`(date),
                    primary key (id, date, value))";
          print $make_table_query;
           my $query_make_handle = $dbh->prepare($make_table_query);
           $query_make_handle->execute();
          return($csd_id);
     }
}


##
# Simply writes a single day of data into calc_store
# Expects:
#    csd_id
#    stock_id 
#    date
#    value
# Returns:
#    Nothing 
##
# Let's write derived data to our data store
sub store_write
{
     # Provided calc_store_descriptor ID
     my $csd_id = $_[0];
     # Provided a data id
     my $data_id = $_[1];
     # Provided a date
     my $date = $_[2];
     # Provided a value to store
     my $value = $_[3];

     if(defined $debug)
     {
          print "FX store_write: StoreID = $csd_id, $data_id, $date, $value\n";
     }

     # Write value into data store
     my $data_convert_query = "insert into calc_store_$csd_id (data_id, date, value) values ($data_id, $date, $value)";
     my $query = "insert into calc_store_$csd_id (data_id, date, value) values (?,?,?)";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($data_id, $date, $value);
     #my $subquery = "insert into calc_store (calc_store_descriptor_id,stock_id,date,value) values (?,?,?,?)";
     #my $subquery_handle = $dbh->prepare($subquery);
     #$subquery_handle->execute($sub_csd_id,$sub_stock_id,$sub_date,$sub_value);
}

##
# Provide a hash of stores, keyed by the csd_id
# Expects:
#    A data source type and a hash of return values
# Returns:
#    A hash of calculation stores (including necessary children, ie. stores that derive data from provided source type)
# Provide a hash of stores, keyed by the csd_id
sub store_list
{
     # Our data source 
     my $data_type = $_[0];

     # Our list of entries to respond with
     my %list = %{$_[1]};

     my $query;
     # Get our current store data
     if($data_type eq "all")
     {
          $query = "select * from calc_store_descriptor";
     }
     else
     {
          $query = "select * from calc_store_descriptor where source_t=?";
     }

     my $query_handle = $dbh->prepare($query);
     if($data_type eq "all")
     {
          $query_handle->execute();
     }
     else
     {
          $query_handle->execute($data_type);
     }
     while (my @data = $query_handle->fetchrow_array())
     {
          # Build hash
          # These are in order: id, name, fx_name, fx_arg, source, column
          #my @data_array = "$data[1]","$data[2]","$data[3]","$data[4]","$data[5]";
          #print "$data[1],$data[2],$data[3],$data[4],$data[5]\n";
          $list{$data[0]} = "$data[1]:::$data[2]:::$data[3]:::$data[4]:::$data[5]"; 
          #$list{$data[0]} = \@data_array; 
          # Now lets invoke this function again with the name of this record's data source 
          my $cs_id = "calc_store_$data[0]";
          %list = &store_list($cs_id,\%list);
     }
     return %list;
	
}


##
# Delete a data store (calc_store_descriptor and calc_store)
# Expects:
#    A calculation store id    
# Returns:
#    Nothing   
##
sub store_delete
{
     # Provided $calc_store_descriptor id
     my $sub_csd_id = $_[0];

     # Let's test to see that this csd exists
     my $query = "select id from calc_store_descriptor where id=?";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($sub_csd_id);
     my $rownumber = $query_handle->rows;
     if($rownumber ne 0)
     {
          # If we have anything here then we do exist
          # Start deleting
          # Delete items in calc_store tied to our calc_store_descriptor
          my $query = "drop table calc_store_$sub_csd_id";
          my $query_handle = $dbh->prepare($query);
          $query_handle->execute();

          # Now let's delete the actual calc_store_descriptor
          $query = "delete from calc_store_descriptor where id=?";
          $query_handle = $dbh->prepare($query);
          $query_handle->execute($sub_csd_id);
     }
     else
     {
          # We got nothing back so this csd doesn't exist
          print "No such data store exists\n";
     }
}

#############################
# Formula Helper Subs
#############################

##
# Gets oldest stock history for all stocks or a single
# Expects:
#    Optionally a stock_id (or data_id) 
#    Table
# Returns:
#    Oldest stock date using epochtime 
##
sub oldest_stock_date
{
     # Our optionally supplied stock
     my $data_id = $_[0];
     # Our optionally supplied table source 
     my $table = $_[1];

     # Initializing vars
     my $oldest_data_date; 
     my $query;

     # If we have a stock do this
     if((defined $data_id) and (defined $table))
     {
          $query = "select min(date) from $table where data_id=?";
     }
     # Otherwise this
     else
     {
          $query = "select min(date) from stock_history";
     }
     my $query_handle = $dbh->prepare($query);

     # Again if we have a stock...
     if(defined $data_id)
     {
          $query_handle->execute($data_id);
     }
     else
     {
          $query_handle->execute();
     }

     # Getting the oldest date
     while (my @data = $query_handle->fetchrow_array())
     {
          $oldest_data_date = $data[0]; 
     }
     return $oldest_data_date;
}


##
# Gets our newest stock date
# Expects:
#    Optionally a stock_id (or data_id) 
# Returns:
#    The most recent entry in epochtime 
##
sub newest_stock_date
{
     # Our provided stock
     my $data_id = $_[0];
     # Our optionally supplied table source 
     my $table = $_[1];

     # Vars
     my $newest_data_date; 
     my $query;

     # If we have a stock do this
     if((defined $data_id) and (defined $table))
     {
          $query = "select max(date) from $table where data_id=?";
     }
     else
     {
          $query = "select max(date) from stock_history";
     }

     my $query_handle = $dbh->prepare($query);

     # Again if we have a stock...
     if(defined $data_id)
     {
          $query_handle->execute($data_id);
     }
     else
     {
          $query_handle->execute();
     }

     while (my @data = $query_handle->fetchrow_array())
     {
          $newest_data_date = $data[0]; 
     }

     return $newest_data_date;
}


##
# Getting our oldest bit of calculation for a specific csd, and optionally a single stock
# Expects:
#    A calculation store descriptor id (csd_id)
#    Optionally a data id 
# Returns:
#    The oldest calculation date in epochtime
##
sub oldest_calculation
{
     # Our csd_id
     my $csd_id = $_[0];
     # Our optional data_id
     my $data_id = $_[1];

     # Vars
     my $query;
     my $oldest_calculation;

     # If we are doing this for a single stock do this
     if(defined $data_id)
     {
     #Mark#6
          #$query = "select min(date) from calc_store where calc_store_descriptor_id=? and stock_id=?;";
          #$query = "select date from calc_store_ where calc_store_descriptor_id=? and stock_id=? order by date asc limit 1";
          $query = "select date from calc_store_$csd_id where data_id=? order by date asc limit 1";
     }
     else
     # Do it for every calc_store
     {
          #$query = "select min(date) from calc_store where calc_store_descriptor_id=?";
          #$query = "select date from calc_store where calc_store_descriptor_id=? order by date asc limit 1";
          $query = "select date from calc_store_$csd_id order by date asc limit 1";
     }
     my $query_handle = $dbh->prepare($query);

     # If for a single stock...
     if(defined $data_id)
     {
          $query_handle->execute($data_id);
     }
     else
     {
          $query_handle->execute();
     }

     # Get our oldest date
     while (my @data = $query_handle->fetchrow_array())
     {
          $oldest_calculation = $data[0]; 
     }
     return $oldest_calculation; 
}

##
# Getting our newest bit of calculation for a specific csd, and optionally a single stock
# Expects:
#    A csd id
#    Optionally a data id
# Returns:
#    The newest calculation date in epochtime 
##
sub newest_calculation
{

     # Our csd id
     my $csd_id = $_[0];
     # Our optional stock id
     my $data_id = $_[1];

     # Vars
     my $query;
     my $newest_calculation;

     # If provided a stock id do this
     if(defined $data_id)
     {
          $query = "select max(date) from calc_store_$csd_id where data_id=?;";
     }
     else
     # No stock so 
     {
          $query = "select max(date) from calc_store_$csd_id;";
     }
     my $query_handle = $dbh->prepare($query);

     # If provided a stock id do this...
     if(defined $data_id)
     {
          $query_handle->execute($data_id);
     }
     else
     {
          $query_handle->execute();
     }

     # Getting our newest date
     while (my @data = $query_handle->fetchrow_array())
     {
          $newest_calculation = $data[0]; 
     }
     return $newest_calculation; 
}


##
# Gets a list of stock IDs in a given industry 
# Expects:
#    industry_id  
# Returns:
#    Array of stock_ids of the industry members 
##
sub get_industry_stocks
{
     # Our provided industry_id
     my $industry_id = $_[0];

     # Vars
     my @industry_stocks;

     # Get our industry members
     # Elaborate on active status
     my $query = "select id from stock where industry_id=? and status='active'";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($industry_id);
     while (my @data = $query_handle->fetchrow_array())
     {
          # Build our list
          push(@industry_stocks, $data[0]);
     }
     # Give our list
     return @industry_stocks;
}


##
# Get the industry name provided an industry id 
# Expects:
#    industry id 
# Returns:
#    industry name 
##
sub get_industry_name
{
     # Our provided industry_id
     my $industry_id = $_[0];

     # Vars
     my $name;

     # Getting the name
     my $query = "select name from industry where id=?";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($industry_id);
     while (my @data = $query_handle->fetchrow_array())
     {
          $name = $data[0];
     }
     # Giving the name
     return $name;
}


##
# Provides the stock id, given the symbol 
# Expects:
#    Stock symbol 
# Returns:
#    Stock ID 
##
sub get_stock_id
{
     # Our provided symbol (upper cased)
     my $stock = uc($_[0]);

     # Vars
     my $stock_id; 

     # Get the stock id
     my $query = "select id from stock where symbol=?";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($stock);
     while (my @data = $query_handle->fetchrow_array())
     {
          $stock_id = $data[0];
     }
     # Give the stock id
     return $stock_id;
}

##
# Provides the stock symbol, given the id 
# Expects:
#    Stock id
# Returns:
#    Stock symbol
##
sub get_stock_symbol
{
     # Our provided symbol (upper cased)
     my $stock_id = $_[0];

     # Vars
     my $stock_symbol; 

     # Get the stock symbol
     my $query = "select symbol from stock where id=?";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($stock_id);
     while (my @data = $query_handle->fetchrow_array())
     {
          $stock_symbol = $data[0];
     }
     # Give the stock symbol
     return $stock_symbol;
}

#############################
# Formula Subs
#############################

# Calculation subs should only write to stores if a csd_id is given
# We should just be able to pass the basic parameters to formula subs and they should
# then figure out whether they need to add data on the left or right side of the data
# already stored.

##
# This is a wrapper function that sets up a stock to have a function
# run against it and optionally writes it to a datastore.  If it doesn't
# write it to the datastore it prints 
# Expects:
#    A data id 
#    A fx
#    Number of days (window)
#    Data table source 
#    Data table column source 
#    Optionally a csd_id if it is to be written 
# Returns:
#    Nothing but hands off to another function to write to datastore
##
sub generic_fx 
{

#Mark#7
### Make sure subsequent updates work

     # Our provided data id
     my $data_id = $_[0];

     # Our provided function
     my $fx = $_[1];

     # Our provided days (window)
     my $days = $_[2];

     # Data table source
     my $table = $_[3];

     # Data table column source
     my $column = "date,$_[4]";

     # Our optionally provided calculation store descriptor id
     # If this is provided then we are writing to it
     my $csd_id = $_[5];
     #my $csd_id = $_[3];

     # Vars
     my $oldest_calculation_date; 
     my $newest_calculation_date; 

     my $oldest_data_history;
     my $newest_data_history;

     my @stock_closes;
     my %queries;

     ## Getting our oldest and newest epochtimes for this stocky
     # Getting our oldest and newest epochtimes for this data
     #$oldest_stock_history = &oldest_stock_date($stock_id);
     $oldest_data_history = &oldest_stock_date($data_id,$table);
     #$newest_stock_history = &newest_stock_date($stock_id);
     $newest_data_history = &newest_stock_date($data_id,$table);
     
     if (defined $debug)
     {
          print "sub generic_fx - Data ID: $data_id -> oldest_data_history = $oldest_data_history\n";
          print "sub generic_fx - Data ID: $data_id -> newest_data_history = $newest_data_history\n";
     }

     # If we have provided a calculation store id do all of this
     # The below section builds creates a couple of quiries with proper date ranges to be acted
     # on.  They get added to the %queries hash
     if(defined $csd_id)
     {
          # If this is defined we are going to write to a datastore and so need to know if 
          # calculations we need left, right, or both data

          # These are only needed if writing to the data store...
          $oldest_calculation_date = &oldest_calculation($csd_id, $data_id); 
          $newest_calculation_date = &newest_calculation($csd_id, $data_id); 

          if (defined $debug)
          {
               print  "sub generic_fx - Data_ID: $data_id, CSD: $csd_id -> oldest_calculation_date = $oldest_calculation_date\n";
               print  "sub generic_fx - Data_ID: $data_id, CSD: $csd_id -> newest_calculation_date = $newest_calculation_date\n";
          }

          # If we have data history that is older than our calculation data (must be cognizant of $days variable above)
          # This means that we have likely loaded new data to the left hand side of our history

          # The get_epoch_minus_days function gets the oldest calculation date, taking into consideration the days window
          # required for our current requirement.

          #if((defined $oldest_calculation_date) and ($oldest_stock_history < ($oldest_calculation_date - ($days * 86400))))
          if((defined $oldest_calculation_date) and ($oldest_data_history < &get_epoch_minus_days($oldest_calculation_date,$days,$data_id)))
          {
               if (defined $debug)
               {
                    print "sub generic_fx - Sub ma: $oldest_data_history < $oldest_calculation_date - $days\n";
               }
               # Here ? is $oldest_calculation_date 
               # query case #1
               #my $q = "select distinct * from $table where data_id=? and date < ? order by date asc";
               my $q = "select distinct $column from $table where data_id=? and date < ? order by date asc";
               $queries{1} = $q;
          }

          # If we have data history that is newer than our calculation data
          # This means that have new data on the right hand side of our history (an update occurred)
          # Note: even if $newest_calculation_date is zero (or equivalent) it is still valid
          if((defined $newest_data_history) and ($newest_data_history > $newest_calculation_date))
          {
               # Here ? is $newest_calculation_date minus $days*86400 (at invocation below)
               # query case #2
               #my $q = "select distinct * from $table where data_id=? and date > ? order by date asc";
               my $q = "select distinct $column from $table where data_id=? and date > ? order by date asc";
               $queries{2} = $q;

          }
     }

     if(not defined $csd_id)
     {
          # We don't have $csd_id defined so really just printing output
          # query case #3
          #my $q = "select distinct * from $table where data_id=? order by date asc";
          my $q = "select distinct $column from $table where data_id=? order by date asc";
          print "$q $data_id\n";
          $queries{3} = $q;

     }


     # Set up actual formulas after the above preamble?

     my @sortorder = sort keys %queries;
     foreach my $q_number (@sortorder)
     # Cycle through @queries array and do the following for each $q defined above          
     {
          # Since each query has unique arguments we need to keep track which one 
          # we are currently processing

          my $query = $queries{$q_number}; 

          if (defined $debug)
          {
               print "Sub generic_fx query: $query\n";
          }

          # Setting up query      
          my $query_handle = $dbh->prepare($query);

          # Handle the query cases defined above depending on the current query_counter value
          # Older data range
          if ($q_number eq 1)
          {
               if (defined $debug)
               {
                    print "Sub generic_fx: first\n";
                    print "Sub generic_fx: $query\n";
                    print "Sub generic_fx: $data_id, $oldest_calculation_date\n";
               }
               $query_handle->execute($data_id, $oldest_calculation_date);
          # Newer data range
          }elsif ($q_number eq 2){
               if (defined $debug)
               {
                    print "Sub generic_fx: second\n";
                    print "Sub generic_fx: $query\n";
                    print "Sub generic_fx: $data_id, &get_epoch_minus_days($newest_calculation_date,$days,$data_id)\n";
               }

               #Mark#8 
               #$query_handle->execute($stock_id, ($newest_calculation_date - ($days * 86400)));
               $query_handle->execute($data_id, &get_epoch_minus_days($newest_calculation_date,$days-1,$data_id));
          # All data
          }elsif ($q_number eq 3){
               if (defined $debug)
               {
                    print "Sub generic_fx: third\n";
                    print "Sub generic_fx: $query\n";
               }
               # This is our non-datastore query
               $query_handle->execute($data_id);
          }

          # Day counter for iteration - after counter >= days calculations can be written
          my $counter = 0;

          # Vars
          my $result = undef;

          # Used to determine if we have enough days for calculations and whether we need 
          # to start popping @stock_closes array
          my $subtract_me = undef;

          # More vars
          my %stock_data;
          my @stock_data_array;

          # Read our results
          while (my @data = $query_handle->fetchrow_array())
          {
               # Increment counter that started at 0;	
               $counter++;

               # The date of our values 
               #my $date = $data[2];
               my $date = $data[0];
                   
               # Append reference to data result array 
               push(@stock_data_array, \@data); 


               # If $subtract_me is set that means that we already have enough data
               # for our moving average ($days) and that we need to remove a value
               # from our @stock_closes array
               if($subtract_me eq 1)
               {
                    shift(@stock_data_array); 
               }
                   
               # If we have data for at least the number of days that we are getting the ma, do this
               # (what I said above)
               if($counter >= $days)
               {
                    if ($fx eq "moving_ave")
                    {
                         #$result = &moving_ave(\@stock_data_array,"6");
                         $result = &moving_ave(\@stock_data_array);
                    }
                    elsif ($fx eq "p_sar")
                    {
                         $result = &p_sar(\@stock_data_array);
                         if ($result eq "0.00")
                         {
                              my $sym = &get_stock_symbol($data_id);
                              my $da = &mytime($date);
                              print "$result: $da: $sym\n";
                         }
                    }
                    elsif ($fx eq "adx")
                    {
                         $result = &adx(\@stock_data_array);
                    }
                    elsif ($fx eq "rsi")
                    {
                         $result = &rsi(\@stock_data_array);
                    }
                    #v1 remove
                    #elsif ($fx eq "volume_ave")
                    #{
                    #     $result = &moving_ave(\@stock_data_array,"8");
                    #}
                    #v1 remove
                    #elsif ($fx eq "pe_ave")
                    #{
                    #     $result = &moving_ave(\@stock_data_array,"19");
                    #}
                    #v1 remove
                    #elsif ($fx eq "float_ave")
                    #{
                    #     $result = &moving_ave(\@stock_data_array,"24");
                    #}
                    #v1 remove
                    #elsif ($fx eq "market_cap_ave")
                    #{
                    ##     $result = &moving_ave(\@stock_data_array,"25");
                    #}

                    # We are setting this to signify that we have enough values for our computations
                    # and that we need to subtract one from the @stock_closes array
                    $subtract_me = 1; 

                    # Default action - Print
                    # Maybe I should put a test in here to see if a $csd_id is present and not print this?
                    # Nah, I like the feedback
                    #if(defined $debug)
                    #{ 
                    print "Calculating value for data ID: $data_id: Days=$days FX=$fx: $result \n";
                    #}

                    # If $csd_id is defined then we are storing our calculated value in the DB	
                    if(defined $csd_id)
                    {
                         if (defined $debug)
                         {
                              print "Sub generic_fx: Writing csd_id: $csd_id, data: $data_id, date: $date, $fx: $result\n";
                         }
                         # Here we write a single line of calculation to the calculation store
                         &store_write($csd_id,$data_id,$date,$result);
                    }
               }
          }
     }
}
##
# Produces an average of some set of values. 
# Expects:
#    An array reference.  This array in turn is full of other array references that should 
#    have data resulting from a "select * from stock_history where ...".
# Returns:
#     The average closing value
##
sub moving_ave
{
#Mark#9
     # Lets dereference our array reference (of array refences) 
     my @stock_data_array = @{$_[0]};
#     my $place = $_[1];

     # Each element is a day
     my $days = scalar @stock_data_array;

     # Vars
     my $close;
     # Reset total value for the ma calculation for this day
     my $total = undef;

     # Iterate through our array of data that contains values for the $days number of days
     my $i = 0;
     while ($i < $days)
     {
          # Our clsing price for number $i day
          #$close = $stock_data_array[$i][6];
          #$close = $stock_data_array[$i][$place];
          $close = $stock_data_array[$i][1];

          # Let's add up the numbers
          $total = $total + $close;
          
          # Let's get the next day
          $i++;
     }
     
     # Average the values
     my $moving_average = $total/$days;	

     # Round the resultant
     $moving_average = sprintf("%.2f", $moving_average);

     # Return the resultant
     return($moving_average); 
}


##
# Takes a list of open, high, low, close values and calculates the parabolic sar
# Expects:
#    An array reference.  This array in turn is full of other array references that should 
#    have data resulting from a "select * from stock_history where ...".
# Returns:
#    The parabolic sar value for last date in the array
##
sub p_sar
{

     # Lets dereference our array reference (of array refences) 
     my @stock_data_array = @{$_[0]};

     # See http://search.cpan.org/~jettero/stockmonkey-2.9014/Business/ParabolicSAR.pm
     # "Wilder himself felt the SAR was particularly vulnerable to "whipsaws" and 
     # recommended only using the SAR when the ADX is above 30 -- that is, when there 
     # is a strong trend going.
     # Creating a new ParabolicSAR object
     my $sar = new Math::Business::ParabolicSAR;

     # alpha is accelaration factor.  The initial is .02 and every step thereafter increases .2
     $sar->set_alpha(0.02, 0.2);

     # Building our array of datapoints

#Mark#10
     # Initializing counter
     my $i = 0;
     # The number of days of data 
     my $days = scalar @stock_data_array;
     # Initializing var
     my @data_points; 
     # For every day 
     while ($i < $days)
     {
          # For every day ($i) load in array open, high, low, close
          #if(defined $stock_data_array[$i][3] and defined $stock_data_array[$i][4] and defined $stock_data_array[$i][5] and defined $stock_data_array[$i][6] and ($stock_data_array[$i][4] >=  $stock_data_array[$i][5]))
          if(defined $stock_data_array[$i][1] and defined $stock_data_array[$i][2] and defined $stock_data_array[$i][3] and defined $stock_data_array[$i][4] and ($stock_data_array[$i][2] >=  $stock_data_array[$i][4]))
          {
               $data_points[$i][0] = $stock_data_array[$i][1];
               $data_points[$i][1] = $stock_data_array[$i][2];
               $data_points[$i][2] = $stock_data_array[$i][3];
               $data_points[$i][3] = $stock_data_array[$i][4];
          }
          # Increment
          $i++;
     }

     # Provide data points
     $sar->insert( @data_points );

     # Get value
     my $sar_value = $sar->query;
     $sar_value = sprintf("%.2f", $sar_value);

     # Print value if debug is turned on
     if(defined $debug){print "Sub p_sar: $sar_value \n";}

     # The following debug was used to find a bug
     if(defined $debug)
     {
          if ($sar_value eq "0.00")
          {
               $i = 0;
               while ($i < $days)
               {
                    # For every day ($i) load in array open, high, low, close
                    # open, high, low, close [3,4,5,6]
                    if(defined $stock_data_array[$i][1] and defined $stock_data_array[$i][2] and defined $stock_data_array[$i][3] and defined $stock_data_array[$i][4] and ($stock_data_array[$i][2] >=  $stock_data_array[$i][4]))
                    {
                         print "Sub p_sar: $data_points[$i][0]\n";
                         print "Sub p_sar: $data_points[$i][1]\n";
                         print "Sub p_sar: $data_points[$i][2]\n";
                         print "Sub p_sar: $data_points[$i][3]\n";
                    }
                    # Increment
                    $i++;
              }
          }
     }

     # Return our calculated sar
     return($sar_value);
}

##
# Takes a list of high, low, close values and calculates the ADX (trend strength)
# Expects:
#    An array reference.  This array in turn is full of other array references that should 
#    have data resulting from a "select * from stock_history where ...".
# Returns:
#    The ADX 
##
sub adx
{
     # Load in our data
     my @stock_data_array = @{$_[0]};
     my @data_points;

     # Create the new dmi object
     # http://search.cpan.org/~jettero/stockmonkey-2.9014/Business/DMI.pm
     my $dmi = new Math::Business::DMI;

     # This is a data smoothing function parameter
     $dmi->set_days(14);

     # Let' start counting at 0
     my $i = 0;
     # Get our number of days
     my $days = scalar @stock_data_array;

     # Build @data_points array of arrays
     while ($i < $days)
     {
          #high low close
          $data_points[$i][0] = $stock_data_array[$i][1];
          $data_points[$i][1] = $stock_data_array[$i][2];
          $data_points[$i][2] = $stock_data_array[$i][3];

          # Another day...
          $i++;
     }

     # Do the thing
     $dmi->insert( @data_points );

     # Get the thing
     my $adx = $dmi->query;     # ADX

     # This function could be extended like this to there things...
     #my $pdi = $dmi->query_pdi; # +DI
     #my $mdi = $dmi->query_mdi; # -DI

     # Let's make sure we get a valid value
     if( defined $adx ) {
          return($adx * 100);
     } else {
         return("ADX: n/a");
     }
}

##
# Takes a list of closing values and calculates the RSI (Relative Strength Index)
# Expects:
#    An array reference.  This array in turn is full of other array references that should 
#    have data resulting from a "select * from stock_history where ...".
# Returns:
#    The RSI value
##
sub rsi
{
     # Our incoming data
     my @stock_data_array = @{$_[0]};

     # http://search.cpan.org/~jettero/stockmonkey-2.9014/Business/RSI.pm
     # Our new object
     my $rsi = new Math::Business::RSI;

     # Our smoother
     $rsi->set_alpha(14); # issues a set days of 2*14-1

     # Start counting
     my $i = 0;

     # Our number of days 
     my $days = scalar @stock_data_array;

     # Start building the closing_values array
     my @closing_values; 
     while ($i < $days)
     {
          #push(@closing_values, $stock_data_array[$i][6]);
          push(@closing_values, $stock_data_array[$i][1]);
          $i++;
     }

     # choose one:
     $rsi->insert( @closing_values );

     # Let's make sure we get a valid value
     if( defined(my $q = $rsi->query) ) {
          return($q); 
     } else {
          return("RSI: n/a.");
     }
}

##
# This gets the extended quote data for today (yahoo doesn't provide historical extended data).
# A couple of helper functions are used: get_fun and process_yq.
# Expects:
#    Nothing   
# Returns:
#    Nothing  
##
sub get_today
{
     # Vars
     my %our_stocks;
     my @yquotes;

     # These are the values we are queryeing for.  See http://www.gummy-stuff.org/Yahoo-data.htm 
     # for a more complete list of options and how they are translated to the get string (which is used
     # by the Simonized perl module)
     my @quote_query = ("Open","Day High","Day Low","Last Trade (Price Only)","Volume",
     "Previous Close","Average Daily Volume","52-week Range","Pct Chg From 52-wk Low",
     "Pct Chg From 52-wk High","50-day Moving Avg","Pct Chg From 50-day Moving Avg",
     "200-day Moving Avg","Pct Chg From 200-day Moving Avg","Earnings/Share","P/E Ratio",
     "Short Ratio","Dividend Pay Date","Ex-Dividend Date","Dividend Yield",
     "Market Capitalization","1yr Target Price","PEG Ratio","Book Value","Stock Exchange",
     "Last Trade Date","Float Shares"); 

     # Let's get all of our stocks
     my $query = "select id,symbol from stock where status='active'";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute();
     while (my @data = $query_handle->fetchrow_array())
     {
          # Building an array of our stock_id to symbol
          my $id = $data[0];
          my $symbol = $data[1] ;
          $our_stocks{$id} = $symbol;

          # We need to deal with the tail end of the list (what lies beyond the last 175)
          #Mark#11
          if( keys( %our_stocks ) eq  "175")
          {
               # Feeding the quote string and the stock hash to get_fun 
               # which is our fx that communicates directly with yahoo
               @yquotes = &get_fun(\%our_stocks, \@quote_query);
               # Take our results reference and stock hash and passes it to
               # be processed 
               #Mark#12
               &process_yq(\@yquotes,\%our_stocks,$dbh);
               undef %our_stocks;
          }
     }
     #Mark#11
     # Get the stragglers 
     if(%our_stocks)
     {
          @yquotes = &get_fun(\%our_stocks, \@quote_query);
          #Mark#12
          &process_yq(\@yquotes,\%our_stocks, $dbh);
     }

     # Clean up any bad data we may just have imported
     &yahoo_extended_mopup;

     # Update the known good dates
     &update_market_dates;
}


##
# This function does the communication with yahoo to get our extended quote
# Expects:
#    A hash of stock id mapped to symbols 
#    A quote option list in an array
# Returns:
#    An array with our results 
##
sub get_fun
{
     # Our passed hash reference with our stocks
     my %our_stocks = %{$_[0]};
     # Our passed array reference with our quote options
     my @quote_query = @{$_[1]};
     my @stocks = undef;

     # Iterate
     foreach my $stock_id(sort keys %our_stocks)
     {
          # Build stocks array to be passed to query function
          push(@stocks, $our_stocks{$stock_id});
     }

     # Toggle on
     useExtendedQueryFormat();     # switch to extended query format
     # Execute 
     my @quotes = getcustomquote([@stocks], [@quote_query]);# using custom format

     # Give back
     return @quotes;
                         
}

##
# The big bertha that procresses the results.  I think this was mostly worked on while
# on our trip to Hawaii 2011!
# Expects:
#    A quote array reference containing extended query results   
# Returns:
#    Nothing, but processes and writes to stock histories
##
sub process_yq
{
     # Our response array 
     my @yquotes = @{$_[0]};

#Mark#13

     # This is needed for float shares
     my $yqref = \@yquotes;

     # Our stock hash
     my %our_stocks = %{$_[1]};

     #Mark#12
     my $dbh = $_[2];

     # Start counter     
     my $i = 0;
     # Start!  
#Mark#14
     foreach my $stock_id(sort keys %our_stocks)
     {
          # Our current stock
          my $stock = $our_stocks{$stock_id};

          # All of our different query results
          my $open = $yquotes[$i][0];
          my $high = $yquotes[$i][1];
          my $low = $yquotes[$i][2];
          my $close = $yquotes[$i][3];
          my $volume = $yquotes[$i][4];
          my $previous_close = $yquotes[$i][5];
          my $average_daily_volume = $yquotes[$i][6];
          my $week_range_52 = &check_pct($yquotes[$i][7]);
          my $pct_change_from_52_wk_low = &check_pct($yquotes[$i][8]);
          my $pct_change_from_52_wk_hi = &check_pct($yquotes[$i][9]);
          my $ma_50 = $yquotes[$i][10];
          my $pct_change_from_50_ma = &check_pct($yquotes[$i][11]);
          my $ma_200 = $yquotes[$i][12];
          my $pct_change_from_200_ma = &check_pct($yquotes[$i][13]);
          my $earnings_to_share = $yquotes[$i][14];
          my $p2e_ratio = $yquotes[$i][15];
          my $short_ratio = $yquotes[$i][16];
          # If we have a result convert it to epochtime
          unless ($yquotes[$i][17] eq "N/A"){ $yquotes[$i][17] = str2time($yquotes[$i][17])} 
          my $dividend_pay_date = $yquotes[$i][17];
          # If we have a result convert it to epochtime
          unless ($yquotes[$i][18] eq "N/A"){ $yquotes[$i][18] = str2time($yquotes[$i][18])} 
          my $ex_dividend_pay_date = $yquotes[$i][18];
          my $dividend_yield = $yquotes[$i][19];
          # Convert response with a B or M to a real number
          my $market_cap = &check_number($yquotes[$i][20]);
          my $target_price_1yr = $yquotes[$i][21];
          my $PEG = $yquotes[$i][22];
          my $book_value = $yquotes[$i][23];
          my $stock_exchange = $yquotes[$i][24];
          # Convert date
          my $epoch_date = str2time($yquotes[$i][25]);
          # Get float - must use this function to deal with inconsistency in data
          my $float_shares = &get_float($yqref,$i,26);

          # Check to see that we have some valid values
          if (($epoch_date gt 0) and not ($open eq 0 and $high eq 0 and $low eq 0 and $close ne 0))
          {
               # Brute forcing this!
               # First delete from stock_history for this stock if we already have data for this date
               my $query1 = "delete from stock_history where data_id=? and date=?";
               # Then build a new BIG ASS INSERT
               my $query2 = "insert into stock_history (data_id,date,open,high,low,close,volume,previous_close,average_daily_volume,52_week_range,pct_change_from_52_wk_low,pct_change_from_52_wk_hi,50_ma,pct_change_from_50_ma,200_ma,pct_change_from_200_ma,earnings_to_share,p2e_ratio,short_ratio,dividend_pay_date,ex_dividend_pay_date,dividend_yield,float_shares,market_cap,1yr_target_price,PEG,book_value,stock_exchange) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
               # Get ready to Do it
               my $query_handle1 = $dbh->prepare($query1);
               # Get ready to Do it again!
               my $query_handle2 = $dbh->prepare($query2);
               # Do it
               $query_handle1->execute($stock_id,$epoch_date);
               # Do it again!
               $query_handle2->execute($stock_id,$epoch_date,$open,$high,$low,$close,$volume,$previous_close,$average_daily_volume,$week_range_52,$pct_change_from_52_wk_low,$pct_change_from_52_wk_hi,$ma_50,$pct_change_from_50_ma,$ma_200,$pct_change_from_200_ma,$earnings_to_share,$p2e_ratio,$short_ratio,$dividend_pay_date,$ex_dividend_pay_date,$dividend_yield,$float_shares,$market_cap,$target_price_1yr,$PEG,$book_value,$stock_exchange);

          }
          # Get ready to do it all again! 
          $i++;
     }
}

##
# Converts a number in the form of 10(M|B|K) to a real number with the necessary zeroes
# Expects:
#    A number with a M or B or K suffix   
# Returns:
#    A real number 
##
#Mark#15
sub check_number
{
     # Our funny number
     my $number = $_[0];

     # A suffix
     my $suffix;

     # If the funny number is defined
     unless ($number eq "N/A")
     {
          # Get the suffix
          $suffix = chop $number;
          # If Thousands 
          if($suffix eq "K")
          {
               $number = $number * 1000;
          }
          # If Millions
          elsif ($suffix eq "M")
          {
               $number = $number * 1000000;
          }
          # If Billions
          elsif($suffix eq "B")
          {
               $number = $number * 1000000000;
          }
          else
          {
               $number = $number.$suffix;
          }

     }
     return($number);
}


##
# Strips the + of a positive percentage but leaves the - for a negative one
# Expects:
#    A positive or negative number   
# Returns:
#    A number or a negative number  
##
sub check_pct
{

     # Our passed percentage
     my $pct = $_[0];

     # If we have a +/-number
     unless ($pct eq "N/A")
     {
          # Get rid of the %
          chop $pct;

          # Determine whether a negator or positive number
          my $poneg = substr($pct,0,1);
          $pct = substr($pct,1);

          # If a positive just return the stripped number
          if ($poneg eq "+")
          {
               return($pct);
          }
          # Else, if negative return number with the negative
          else
          {
               return($poneg.$pct);
          }
     }
     return($pct); 
}


##
# This function is needed because we are unsure of the exact format as sometime
# the delimiter changes.  This is why it is requested last.
# Expects:
#    An array ref   
#    The $i counter which specifies the stock
#    The position where the float data is
# Returns:
#    The float number 
##
sub get_float
{
     # This is how the function is called
     #my $float_shares = &get_float(\@yquotes,$i,26);

     # The quote, representing a single line from the db
     my @quote_array = @{$_[0]};
     # The stock specified by the position of $i
     my $stock = $_[1];
     # The place in the line where the float number starts
     my $quote_start = $_[2];

     # What we are after
     my $float_number;

     # Iterating until we get to the proper position
     while ($quote_array[$stock][$quote_start])
     {
          $float_number = $float_number.$quote_array[$stock][$quote_start];
          $quote_start++;
     }
     # Getting rid of spaces
     $float_number =~ s/^\s+//;

     # Give it back
     return($float_number); 
}


##
# Checks whether the provided epoch time is on a day the markets are open
# Mark#13
# Expects:
#    Nothing
# Returns:
#    1 for active session (or possibly active), 0 otherwise 
##
sub check_for_active_session
{

     use POSIX qw(tzset);
     $ENV{TZ} = 'America/New_York';
     tzset;

     # Let's do a quick test first to see if it lands on a weekend
     my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();

     print "$hour $min\n";
     # If sat or sun, not a market day 
     if(($wday eq 6) or ($wday eq 0 ))
     {
          return("0");
     }

     # If $hour is 9 (and if 9, minues > 30) to 16 
     if((($hour == 9) and ($min > 30)) or ($hour > 10) and ($hour < 16))
     {

     }

     print "$hour $min\n";

     # Let's normalize our date
     #my $good_date = &mytime();
     my $good_date = str2time(&mytime(time));

     # Set up the query
     my $q = Finance::QuoteHist->new
     (
          symbols    => [qw(^GSPC)],
          start_date => "$good_date", # or '1 year ago', see Date::Manip
          end_date   => "$good_date",
     );

     # Our response vars
     my ($symbol, $date, $open, $high, $low, $close, $volume,$row);

     # Our response
     foreach $row ($q->quotes()) {
          ($symbol, $date, $open, $high, $low, $close, $volume) = @$row;
     }

     # If we get a valid response we are on a market day
     if(defined $symbol)
     {
          return(1);
     }
     else
     {
          return(0);
     }
}

##
# Updates the market_dates table.  If dates are not provided
# then the necessary dates are gathered and the function calls
# itself
# Expects:
#    Nothing, or a combination of start_date and end_dates 
# Returns:
#    Nothing 
##
sub update_market_dates
{

     my $start_date = $_[0];
     my $end_date  = $_[1];
 
     #if we don't have two date vars passed then get our dates
     # and call ourselves again
     unless(defined $start_date and defined $end_date)
     {
          my $oldest_stock_date = &oldest_stock_date;
          my $newest_stock_date = &newest_stock_date;

          my $oldest_market_date = &get_market_dates("oldest");
          my $newest_market_date = &get_market_dates("newest");

          #print "$oldest_stock_date < $oldest_market_date\n";
          #print "$newest_stock_date > $newest_market_date\n";
          #exit;

          # First let us delete any market_dates that are older or newer than
          # our stock history
          if ($oldest_stock_date > $oldest_market_date)
          {
               my $query = "delete from market_dates where date < ?";
               my $query_handle = $dbh->prepare($query);
               $query_handle->execute($oldest_stock_date);
          }
          if ($newest_stock_date < $newest_market_date)
          {
               my $query = "delete from market_dates where date > ?";
               my $query_handle = $dbh->prepare($query);
               $query_handle->execute($newest_stock_date);
          }
          # Initial data load
          if ($newest_market_date == 0)
          {
               # Just get our stock date range
               &update_market_dates($oldest_stock_date, $newest_stock_date);
          }
          # Left hand side
          elsif ($oldest_stock_date < $oldest_market_date)
          {
               my $oldest_market_date_minus_one = $oldest_market_date - 86400;
               # Call ourselves with dates set
               &update_market_dates($oldest_stock_date, $oldest_market_date_minus_one);
          }
          # Right hand side
          elsif ($newest_stock_date > $newest_market_date) 
          {
               my $newest_market_date_plus_one = $newest_market_date + 86400;
               # Call ourselves with dates set
               &update_market_dates($newest_market_date_plus_one, $newest_stock_date);
          }
          return;
     }

     # Let's reformat our dates
     my $string_start_date = &mytime($start_date);
     my $string_end_date = &mytime($end_date);

     # Create our new Finance object 'q'
     my $q = Finance::QuoteHist->new
     (
     symbols    => [qw(^GSPC)],
     start_date => $string_start_date, # '6 months ago' or  '1 year ago', see Date::Manip
     end_date   => $string_end_date,
     );

     # Work with our 'q'
     foreach my $row ($q->quotes()) 
     {
          # Break up the data into chunks
          my ($symbol, $date, $open, $high, $low, $close, $volume) = @$row;
          # Change date to epoch time
          my $epoch_date = str2time($date);
          # Do the below time range check because sometimes we get out of range values that 
          # are surprising!
          if(($epoch_date >= $start_date) and ($epoch_date <= $end_date))
          {
               # Add to db 
               my $query = "insert into market_dates (date) values (?)";
               my $query_handle = $dbh->prepare($query);
               $query_handle->execute($epoch_date);
               if(defined $debug)
               {
                    print "Sub update_market_dates: Start - $string_start_date($start_date) -> $string_end_date($end_date), $date($epoch_date)\n";
               }
          }
     }
}


##
# Provides market dates in the form of an array or string depending on what 
# is being requested
# Expects:
#    One of _ALL_, oldest, newest 
# Returns:
#    An array of market dates, oldest_date, or newest_date
##
sub get_market_dates
{
     # What is being requested
     my $option = $_[0];

     # Lets get and return all of the date values
     if ($option eq "_ALL_")
     {
          my @market_dates;

          my $query = "select date from market_dates order by date asc";
          my $query_handle = $dbh->prepare($query);
          # Get our affected row count
          my $num_rows = $query_handle->execute();
          # If zero we have no data
          if($num_rows eq "0")
          {
               die("No market_dates data!\n");
          }
          # These be our stock dates
          while(my @data = $query_handle->fetchrow_array())
          {
               push(@market_dates, $data[0]);
          }
          # Return array of all dates
          return(@market_dates);
     }
     # Lets get and return the oldest value
     elsif ($option eq "oldest")
     {
          my $query = "select date from market_dates order by date asc limit 1";
          my $query_handle = $dbh->prepare($query);
          # Get our affected row count
          my $num_rows = $query_handle->execute();
          # If zero we have no data
          if($num_rows eq "0")
          {
               die("No market_dates data!\n");
          }

          while (my @market_dates = $query_handle->fetchrow_array())
          {
               my $oldest_date = $market_dates[0];
               # Return string of oldest date
               return($oldest_date);
          }
     }
     # Lets get and return the newest value
     elsif ($option eq "newest")
     {
          my $query = "select date from market_dates order by date desc limit 1";
          my $query_handle = $dbh->prepare($query);
          # Get our affected row count
          my $num_rows = $query_handle->execute();
          # If zero we have no data
          if($num_rows eq "0")
          {
               die("No market_dates data!\n");
          }

          while (my @market_dates = $query_handle->fetchrow_array())
          {
               my $newest_date = $market_dates[0];
               # Return string of newest date
               return($newest_date);
          }
     }
}

#Mark#16
##
#  
# Expects:
#    one of a named check (or keyword all), optionally a single stock
# Returns:
#     
##
sub integrity_check
{

     # JUST REPORT INITIALLY  
     # Be able to do this for single stock
     # Have default be report but eventually also provide a repair option

     # The current check we are doing, one of
     # stock_history
     my $check = $_[0];
 
     # Are we doing this for single stock?
     my $stock = $_[1];

     # Default is just to report
     my $option = $_[2];
     
     my $stock_id;
     my @stocks;
     #my @stock_dates;
     
     if($stock ne "_ALL_")
     {
          # Get our stock id from the symbol name
          $stock_id = &get_stock_id($stock);

          # If not all just load the single stock into the array so that we 
          # deal with it the same way
          push(@stocks, $stock_id); 

     }else{
          # Calling all_stocks with "id" as our desired data
          @stocks = &all_stocks("id");
     }

     # Get our valid market dates
     my @market_dates = &get_market_dates("_ALL_");

     # Let's make sure we have a complete stock history compared to known market_dates
     if($check eq "stock_history")
     {
          # For all of our stocks listed by id do this
          foreach my $id (@stocks)
          {

               # Get our dates for each stock in @stocks
               my $query = "select date from stock_history where data_id=? order by date asc";
               my $query_handle = $dbh->prepare($query);
               $query_handle->execute($id);

               my @stock_dates = undef;

               # These be our stock dates
               while(my @data = $query_handle->fetchrow_array())
               {
                    push(@stock_dates, $data[0]);
                    #print "s dates: $data[0]\n";
               }
                    
               # Get how many days since we've had a valid entry for this stock relative to our known market dates
               my $last_stock_entry_date = $stock_dates[-1];
               my $last_market_entry_date = $market_dates[-1];
               # Rounding for whole numbers
               my $entry_day_difference = ($last_market_entry_date - $last_stock_entry_date) / 86400;
               my $symbol = &get_stock_symbol($id); 

               my $oldest_stock_date = &oldest_stock_date($id,"stock_history");

               if($entry_day_difference > 30)
               {
                    my $last_date = &mytime($last_stock_entry_date);
                    print "Marking $id ($symbol) as inactive.  Last entry ($last_date) $entry_day_difference days older than our current market data \n";
                    my $inactive_mark = "update stock set status='inactive' where id=?";
                    my $query_handleIM = $dbh->prepare($inactive_mark);
                    $query_handleIM->execute($id);
               }
               else
               {
                    # Let's see what's in @market_dates but not in @stock_dates (missing stock_dates)
                    #    If difference option is specified an array of values in array0 not in array1 is returned
                    #my @intersection_difference = &intersection(\@stock_dates,\@market_dates,"difference");
                    my @intersection_difference = &intersection(\@market_dates,\@stock_dates,"difference");

                    foreach my $epoch_date (@intersection_difference)
                    {
                        my $date = &mytime($epoch_date);
                        if ($epoch_date >= $oldest_stock_date)
                        { 
                             print "Stock: $id ($symbol) is missing stock history data for $epoch_date ($date)\n";
                             if($option eq "repair")
                             {
                                   print "Attempting to get quote for $symbol for $date\n\n";
                                   &stock_history_write($symbol,$id,$date,$date);
                             }
                         }
                    }
               }
          }
     }
#     Check calculation data

#     Check for duplicates in stock history as well as datastore

#     Check for logical data constraints errors (having ds records without corresponding stock_id, have csd without cs, etc)

#     Check for valid market cap data

#     yahoo mop up
}

##
# Function deletes the pink sheet stocks.  Was called one time
# to clean up data.  There is no permanent invocation of it from folly.
# Expects:
#    Nothing
# Returns:
#    Nothing     
#    v1 - Not really used in v2 but making a note that this is 
#    currently only v1 ready.
##
sub delete_pink
{

     # Get our dates for each stock in @stocks
     my $query = "select id,symbol from stock where symbol like '%.PK' or symbol like '%.OB'";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute();

     # These be our stock dates
     while(my @data = $query_handle->fetchrow_array())
     {
          my $id = $data[0];
          print "Symbol: $data[1] ID: $data[0]\n";

          my $query2 = "delete from stock_history where data_id=?";
          my $query_handle2 = $dbh->prepare($query2);
          $query_handle2->execute($id);

          my $query3 = "delete from calc_store  where data_id=?";
          my $query_handle3 = $dbh->prepare($query3);
          $query_handle3->execute($id);

          my $query4 = "delete from stock  where id=?";
          my $query_handle4 = $dbh->prepare($query4);
          $query_handle4->execute($id);
     }
          
}

##
# This function is called for each stock being queried 
# Expects:
#     A query hash
# Returns:
#     
##
sub run_query
{
#Mark#17
#
#                --query                 Run query against data
#                    ds=                 Name of datastore to query
#                    dsv=                Datastore value(s) to search for.  Can be an exact value, a range, or a 
#                                        less than or greater than.  Examples: '1', '1-3', '>1', '<3'
#                    dsrv=               Datastore relative value to search for.  This is a percentage value ranging
#                                        from 1 to 100.
#                    save=               Save this query with provided name, the saved query may contain subqueries
#                    dir=                Up, down, or flat.  This is the direction of change.  This assumes that \"window\" is set.
#                    dirdelta=           This is a value indicating amount of change.  This assumes that \"window\" is set.
#                    window=             Time window measured in days for the query
#                    stock=             Specify whether to run query against a single stock, where value is stock

     # The reference to the %query hash is passed.  This contains all of the options passed via the --query option
     my $stockquery = $_[0];

     # A data store id to search in
     my $csd_id;

     # A data store id to do a relative search in
     my $csdr_id;

     # A place to store some query return values
     # This becomes a hash of a hash where it is keyed by $i which is the counter of query types for the query
     # and subsequently the number of iterations through a loop.  The contents of the hash in $new_dates{$i} is a result
     # keyed by a date where the query holds true.
     my %new_dates;

     # @dates is an array that is filled with dates where the query for a given iteration of $i is true.
     # It is filled at the end of the iteration.  True date values for the current $i iteration of the search are
     # contained in the %new_dates hash and where there is an intersection of these dates and those in @dates,
     # those dates are put into @dates_temp and then after processing @dates_temp becomes @dates 
     my @dates;
     my @dates_temp;
     
     # The query dates used in the first iteration $i
     my @q_dates;

     # DB stuff
     my $query;
     my $query_handle;

#Mark#18
     # Should this query be saved?
     my $save;

#Mark#19
#"Mark#19" stuff should probably be down below in the while loop?
     # The direction (up/down) that the queried attribute should be going
     my $dir;

#Mark#19
     # The change of direction (percentage?)
     my $dirdelta;

#Mark#19
     # The moving time window
     my $window;

     # We are doing this search on a single stock
     my $stock;

     # Initializing the $stock_id var
     my $stock_id;

     # $i should start with 1 as that is what the arguments are started with on the cli
     my $i = 1;


     # Start to process our cli query strings
     while (exists ${$stockquery}{"ds$i"})
     {

          # Initialize our vars
          my $ds;
          my $dsv;
          my $dsvlt;
          my $dsvgt;

          my $dsr;
          my $dsrv;
          my $dsrvlt;
          my $dsrvgt;

          my $TDrt;
          my $TDrc;
          my $TDrkey;
          my $TDrv;
          my $TDrvlt;
          my $TDrvgt;

          my $slopev;
          my $slopelt;
          my $slopegt;
          my $slope_window;
          my $slope_offset;

          # Do we need to zero out all of the values so that we aren't carrying forth values from i - 1?
          # It does not appear so since we are my'ing them right above but I will leave the above comment

          # If they exist in the hash assign them to the variable
          # This is easier for me to deal with though perhaps less efficient

          # Datastore
          if(defined ${$stockquery}{"ds$i"}){$ds = ${$stockquery}{"ds$i"}}
          if(defined $debug){print "Sub run_query - ds: $ds \n";}
          # Datastore value
          if(defined ${$stockquery}{"dsv$i"}){$dsv = ${$stockquery}{"dsv$i"}}
          # Datastore less than value
          if(defined ${$stockquery}{"dsvlt$i"}){$dsvlt = ${$stockquery}{"dsvlt$i"}}
          # Datastore greater than value
          if(defined ${$stockquery}{"dsvgt$i"}){$dsvgt = ${$stockquery}{"dsvgt$i"}}
          # A datastore with values to compare agains the above datastore (datastore relative - dsr)
          if(defined ${$stockquery}{"dsr$i"}){$dsr = ${$stockquery}{"dsr$i"}}
          # dsr value (exppressed as a percentage)
          if(defined ${$stockquery}{"dsrv$i"}){$dsrv = ${$stockquery}{"dsrv$i"}}
          # dsr less than value
          if(defined ${$stockquery}{"dsrvlt$i"}){$dsrvlt = ${$stockquery}{"dsrvlt$i"}}
          # dsr greater value
          if(defined ${$stockquery}{"dsrvgt$i"}){$dsrvgt = ${$stockquery}{"dsrvgt$i"}}

          # A table with data values to compare against the above datastore (datastore relative - TDt)
          if(defined ${$stockquery}{"TDrt$i"}){$TDrt = ${$stockquery}{"TDrt$i"}}
          # A table column with data values to compare against the above datastore (datastore relative - TDc)
          if(defined ${$stockquery}{"TDrc$i"}){$TDrc = ${$stockquery}{"TDrc$i"}}
          # This is the key (or column of the table) that matches up with the data_id value in the calc_data stores
          if(defined ${$stockquery}{"TDrkey$i"}){$TDrkey = ${$stockquery}{"TDrkey$i"}}
          # table value (exppressed as a percentage)
          if(defined ${$stockquery}{"TDrv$i"}){$TDrv = ${$stockquery}{"TDrv$i"}}
          # table value less than value
          if(defined ${$stockquery}{"TDrvlt$i"}){$TDrvlt = ${$stockquery}{"TDvlt$i"}}
          # table value greater value
          if(defined ${$stockquery}{"TDrvgt$i"}){$TDrvgt = ${$stockquery}{"TDrvgt$i"}}

          # Slope value (requires a ds and optionally a dsr)
          if(defined ${$stockquery}{"slopev$i"}){$slopev = ${$stockquery}{"slopev$i"}}
          # Slope less than value
          if(defined ${$stockquery}{"slopelt$i"}){$slopelt = ${$stockquery}{"slopelt$i"}}
          # Slope greater than value
          if(defined ${$stockquery}{"slopegt$i"}){$slopegt = ${$stockquery}{"slopegt$i"}}
          # Our time frame that we are looking at the slope
          if(defined ${$stockquery}{"slope_window$i"}){$slope_window = ${$stockquery}{"slope_window$i"}}
          # Our time frame offset relative to the end of the window
          if(defined ${$stockquery}{"slope_offset$i"}){$slope_offset = ${$stockquery}{"slope_offset$i"}}

          # Save this query with this name
          if(defined ${$stockquery}{"save$i"}){$save = ${$stockquery}{"save$i"}}

          # Direction 
          if(defined ${$stockquery}{"dir$i"}){$dir = ${$stockquery}{"dir$i"}}
          # Direction change (expressed as a percentage)
          if(defined ${$stockquery}{"dirdelta$i"}){$dirdelta = ${$stockquery}{"dirdelta$i"}}
          # Our moving time window
          if(defined ${$stockquery}{"window$i"}){$window = ${$stockquery}{"window$i"}}
          # The following check is done so that a stock need only be declared once (in stock1=asdf)
          # This is usefull for testing against a single stock
          #if(defined ${$stockquery}{"stock$i"})
          if(defined ${$stockquery}{"stock"})
          {
               $stock = ${$stockquery}{"stock"};
               if(defined $debug){print "Sub run_query - stock is $stock\n";}
          }
          #else 
          #{
          ##     $stock = ${$stockquery}{"stock1"};
          #     if(defined $debug){print "Sub run_query - stock1 is $stock\n";}
          #}

          # Get our stock id from the symbol name
          $stock_id = &get_stock_id($stock);

          # Print if debug
          if(defined $debug){print "Sub run_query - Stock:".${$stockquery}{"stock"}."\n";}

#Mark#20
# ADD A SIMILAR SECTION FOR A SPECIFIC DATE
# And then skip loading the dates array below


#Mark#25
# Here we need to fill a @q_dates array for the first $i iteration.  This would be the place
# to add functionality for specifying date ranges and getting the intersect of desired dates with
# actual marked dates so that we are querying for dates with data.

          # The following check is done so that a date need only be declared once (in date1=12/1/2012)
          # This is usefull for testing against a single date
          if(defined ${$stockquery}{"date"} and $i eq "1")
          {
               my $q_date = str2time(${$stockquery}{"date"});
               if(defined $debug){print "Sub run_query - date is $q_date\n";}
               # left off

               push(@q_dates, $q_date);
               my @market_dates = &get_market_dates("_ALL_");
               @q_dates = &intersection(\@market_dates,\@q_dates);
               if(scalar @q_dates eq "0")
               {
                    print "There is no valid stock data for the date you picked\n";
                    exit;
               }

          }
          #else 
          #{
          #     my $q_date = ${$stockquery}{"date1"};
          #     if(defined $debug){print "Sub run_query - date1 is $q_date\n";}
          #}
          #else 
          #{
          #     #$stock = ${$stockquery}{"stock1"};
          #     #if(defined $debug){print "Sub run_query - stock1 is $stock\n";}
          #     my $q_date = str2time(${$stockquery}{"date1"});
          #     if(defined $debug){print "Sub run_query - date1 is $q_date\n";}
          #     print "Sub run_query - date1 is $q_date\n";
          #     push(@dates, $q_date);
          #}


          ###############
          # Expand this to check for a saved query
          ###############
          # Getting the csd_id for the supplied datastore name
          if(defined $ds)
          {
               $csd_id = &get_csd_id($ds);
               if(defined $debug){print "Sub run_query - csd_id is $csd_id\n";}
          }
          # Getting the csd_id for the supplied relative datastore name
          if(defined $dsr)
          {
               $csdr_id = &get_csd_id($dsr);
               if(defined $debug){print "Sub run_query - csdr_id is $csdr_id\n";}
          }
          ###############
          # Expand this to check for a saved query
          ###############

          #######################
          # start dsv processing
          #######################

          # This acts like a filter

          # If we are querying against a datastore do this
          if(defined $dsv or defined $dsvlt or defined $dsvgt)
          {
               # Get our count of matches
               my $number_of_dates = scalar @dates;

               if(defined $debug){print "Sub run_query - DSV q: $dsv, $dsvlt, $dsvgt,$stock_id, $csd_id, Matches: $number_of_dates\n";}
               # Call the actual dsv query function
               # This returns a hash of values keyed by dates where the query is true.  
               # This might be overridden and/or used by other queries - so the order of processing matters

               # This means that we have had successful iterations with query results
               if ($number_of_dates gt 0)
               {
                    $new_dates{$i} = &query_dsv($dsv, $dsvlt, $dsvgt, $stock_id, $csd_id, \@dates);
               # We are going around the first iterations and have been provided dates to test
               }elsif($i eq "1" and scalar @q_dates gt 0){
                    $new_dates{$i} = &query_dsv($dsv, $dsvlt, $dsvgt, $stock_id, $csd_id,\@q_dates);
               }else{
                    # We have not matching query results yet and have no dates to work against so do it for all of our dates
                    my @market_dates = &get_market_dates("_ALL_");
                    $new_dates{$i} = &query_dsv($dsv, $dsvlt, $dsvgt, $stock_id, $csd_id,\@market_dates);
               }
          }
          #######################
          # end dsv processing
          #######################

          #######################
          # start dsrv processing
          #######################

          # This also acts like a filter
#?
# Put a test in place to make sure that dsvgt/lt values are not included a # search where dsrv is sought
# A dsvgt/lt criteria should have its own #
#?
          # If we are doing a relative querying against a datastore do this
          if(defined $dsrv or defined $dsrvlt or defined $dsrvgt)
          {
               # Get our count of matches
               my $number_of_dates = scalar @dates;

               if(defined $debug){print "Sub run_query - DSRV q: $dsrv, $dsrvlt, $dsrvgt,$stock_id, $csdr_id, Matches: $number_of_dates\n";}
               # Call the actual dsrv query function
               # This returns a hash of values keyed by dates where the query is true.  
               # This might be overridden and/or used by other queries - so the order of processing matters
               # It is similar to query_dsv except it is provided the csd_id and csdr_id

               if ($number_of_dates gt 0)
               {
                    $new_dates{$i} = &query_dsrv($dsrv, $dsrvlt, $dsrvgt, $stock_id, $csd_id, $csdr_id,\@dates);
               }elsif($i eq "1" and scalar @q_dates gt 0){
                    $new_dates{$i} = &query_dsrv($dsrv, $dsrvlt, $dsrvgt, $stock_id, $csd_id, $csdr_id,\@q_dates);
               }else{
                    # We have no dates to work against so do it for all of our dates
                    my @market_dates = &get_market_dates("_ALL_");
                    $new_dates{$i} = &query_dsrv($dsrv, $dsrvlt, $dsrvgt, $stock_id, $csd_id, $csdr_id,\@market_dates);
               }

          }
          #######################
          # end dsrv processing
          #######################

          #######################
          # start TDrv processing
          #######################

          # This also acts like a filter
#?
# Put a test in place to make sure that dsvgt/lt values are not included a # search where dsrv is sought
# A dsvgt/lt criteria should have its own #
#?
          # If we are doing a relative querying against a datastore do this
          if(defined $TDrv or defined $TDrvlt or defined $TDrvgt)
          {
               # Get our count of matches
               my $number_of_dates = scalar @dates;

               if(defined $debug){print "Sub run_query - TDRV q: $TDrt, $TDrc,$TDrkey,$TDrv, $TDrvlt, $TDrvgt,$stock_id, $csd_id, Matches: $number_of_dates\n";}
               # Call the actual TDrv query function
               # This returns a hash of values keyed by dates where the query is true.  
               # This might be overridden and/or used by other queries - so the order of processing matters
               # It is similar to query_dsv except it is provided the csd_id and $TDt 

                    #print "0badf\n";
               if ($number_of_dates gt 0)
               {
                    #print "1badf\n";
                    $new_dates{$i} = &query_TDrv($TDrt,$TDrc,$TDrkey,$TDrv,$TDrvlt,$TDrvgt,$stock_id,$csd_id,\@dates);
               }elsif($i eq "1" and scalar @q_dates gt 0){
                    #print "2badf\n";
                    $new_dates{$i} = &query_TDrv($TDrt,$TDrc,$TDrkey,$TDrv,$TDrvlt,$TDrvgt,$stock_id,$csd_id,\@q_dates);
               }else{
                    # We have no dates to work against so do it for all of our dates
                    my @market_dates = &get_market_dates("_ALL_");
                    #print "3badf\n";
                    $new_dates{$i} = &query_TDrv($TDrt,$TDrc,$TDrkey,$TDrv,$TDrvlt,$TDrvgt,$stock_id,$csd_id,\@market_dates);
               }

          }
          #######################
          # end TDrv processing
          #######################

          #######################
          # start slope processing
          #######################

          # This also acts like a filter
          if(defined $slope_window)
          {
               #my $number_of_slopes = scalar keys %{$new_dates{$i}};
               # Get our count of dates
               my $number_of_slopes = scalar @dates;
               # If we are doing a query against a datastore values slope do this
               if((defined $slopev or defined $slopelt or defined $slopegt) and $number_of_slopes gt 0)
               {
                    # Call the actual query_slope function
                    # This returns a hash of values keyed by dates where the query is true.  
                    # It is similar to query_dsv except it is provided the csd_id and csdr_id
                    # This might be overridden and/or used by other queries - so the order of processing matters
                    $new_dates{$i} = &query_slope($slopev, $slopelt, $slopegt, $stock_id, $csd_id,\@dates, $slope_window,$slope_offset);
               }elsif($i eq "1" and scalar @q_dates gt 0){
                    $new_dates{$i} = &query_slope($slopev, $slopelt, $slopegt, $stock_id, $csd_id,\@q_dates, $slope_window,$slope_offset);
               }elsif((defined $slopev or defined $slopelt or defined $slopegt))
               {
                    # We have no dates to work against so do it for all of our dates
                    my @market_dates = &get_market_dates("_ALL_");
                    $new_dates{$i} = &query_slope($slopev, $slopelt, $slopegt, $stock_id, $csd_id, \@market_dates, $slope_window,$slope_offset);
               }
               if(defined $debug){print "Sub run_query - Slope q: $slopev, $slopelt, $slopegt,$stock_id, $csd_id\n";}
          }
          #######################
          # end slope processing
          #######################

          # This is our returned result date/value hash
          my %hash = %{$new_dates{$i}};
          my $marker = 0;
          # Iterate through
          foreach my $date (sort keys %hash)
          {
               # If this is the first query we are iterating through
               if($i eq 1)               
               {
                    # Load into our result array of dates
                    push(@dates, $date); 

                    # If this is not our first time through see if this result
                    # is already in the array.  If it is push it into the temp
                    # array which will become the full array.  This way we are getting
                    # the overlap

               # Perl version of php in_array function
               }elsif(grep $_ eq $date, @dates){
                    push(@dates_temp, $date); 
               }
               $marker = 1;
          }

          # If this exists then make it the current @dates response array
          if(@dates_temp)
          {
               @dates = @dates_temp;

          # Clear up this one
               undef @dates_temp;
          }

          if($marker eq 0)
          {
               return(0);
          }

          # Increment out counter
          $i++;

          # Keeping track of $i
          if(defined $debug){print "Sub run_query - i is $i\n";}
          $csd_id = 0;

     } 

###
# Active code development begins here
###

# At this point we have our matching results based on our filter.
# We will now put them into the rating pool to determine their quality


     # Let us get the number of matches (they should equal the number of queries[i-1])
     my $match_count = scalar keys %new_dates;

                    my $counter = 0;
                    my $win_counter = 0;
                    my $loose_counter = 0;
                    my $tally;

     if ($match_count = ($i - 1))
     {
          # First, clearing out rating_pool from the last run
          $query = "delete from rating_pool";
          $query_handle = $dbh->prepare($query);
          $query_handle->execute();


          foreach my $I (sort keys %new_dates)
          {

               my @ndates = @dates;
               my %hash = %{$new_dates{$I}};
               if(defined ${$stockquery}{"dir$I"})
               {
                    print "Dir $I is ".${$stockquery}{"dir$I"}."\n";
               }
               foreach my $date (@ndates)
               {
                    if(defined $debug){print "Sub run_query - date is $date\n";}
                    $counter++;
                    if(defined $debug){print "Sub run_query - counter is $counter\n";}
                    #print "I is $I\n";

                    my $value = $hash{$date};
                    if (defined $value)
                    {
                         my $formdate = &mytime($date);
                         print "Date Matches Criteria #$I for $stock: Date: $formdate - $date Value: $value\n";
                         
                         if($match_count eq $I)
                         {
                              my $status = &is_winner($stock_id, $date,"20","20");
                              if($status eq 1)
                              {
                                   $win_counter++;
                                   $tally = ($win_counter/$counter) * 100;
                                   print "Status: Winner -> $stock on $formdate\n";
                                   print "Winners: $counter/$win_counter $tally%\n";
                              }else{
                                   $loose_counter++;
                                   $tally = ($loose_counter/$counter) * 100;
                                   print "Status: Looser -> $stock on $formdate\n";
                                   print "Loosers: $counter/$loose_counter $tally%\n";
                              }
                         }

                         #if (rate parameters exists for this $i)
                         $query = "insert into rating_pool (stock_id,date,value) values(?,?,?)";
                         $query_handle = $dbh->prepare($query);
                         $query_handle->execute($stock_id, $date, $value);
                    }
               }
          }
     }
}


##
# This runs a query for a given stock for exact, less than, or greater values
# This function is invoked by run_query function
# Expects:
#    One or more of dsv, dsvlt, dsvgt 
#    data_id which is often stock_id
#    datastore id (csd_id)
#    dates to run the query for (@query_dates)
# Returns:
#    A hash with the values keyed by date
##
sub query_dsv
{
     # My datastore value
     my $dsv = $_[0];
     # My datastore less than value
     my $dsvlt = $_[1];
     # My datastore greater than value
     my $dsvgt = $_[2];
     # The stock id of what we are looking at
     my $data_id = $_[3];
     # The data store id that we are looking at
     my $csd_id = $_[4];

     # Our dates
     my @query_dates = @{$_[5]};

     # Vars
     my $query;
     my $query_handle;
     my @dates;

     # Declare our hash for the response to go
     my %hash;

     #######################
     # start dsv processing
     #######################

     foreach my $query_date (@query_dates)
     {
          #Mark#21
          # If we have a datastore value and not a lt or gt value
          if(not defined $dsv and not defined $dsvlt and not defined $dsvgt)
          {
               #$query = "select date,value from calc_store where calc_store_descriptor_id=? and stock_id=? and date=?";
               $query = "select date,value from calc_store_$csd_id where data_id=? and date=?";
               $query_handle = $dbh->prepare($query);
               #$query_handle->execute($csd_id,$stock_id);
               $query_handle->execute($data_id,$query_date);
               if(defined $debug){print "Sub query_dsv - query #1\n";}
               # Process results
               while (my @data = $query_handle->fetchrow_array())
               {
                    my $date = $data[0];
                    my $value = $data[1];
                    $hash{$date} = $value;
               }
          }
          # If we have a datastore value and not a lt or gt value
          elsif(defined $dsv and not defined $dsvlt and not defined $dsvgt)
          {
               #$query = "select date,value from calc_store where calc_store_descriptor_id=? and stock_id=? and value=? and date=?";
               $query = "select date,value from calc_store_$csd_id where data_id=? and value=? and date=?";
               $query_handle = $dbh->prepare($query);
               #$query_handle->execute($csd_id,$stock_id,$dsv);
               $query_handle->execute($data_id,$dsv,$query_date);
               if(defined $debug){print "Sub query_dsv - query #2\n";}
               # Process results
               while (my @data = $query_handle->fetchrow_array())
               {
                    my $date = $data[0];
                    my $value = $data[1];
                    $hash{$date} = $value;
               }
          }
          # If we have a datastore value and not a lt or gt value
          elsif(defined $dsvlt and not defined $dsvgt)
          {

               #$query = "select date,value from calc_store where calc_store_descriptor_id=? and stock_id=? and value < ? and date=?";
               $query = "select date,value from calc_store_$csd_id where data_id=? and value < ? and date=?";
               $query_handle = $dbh->prepare($query);
               #$query_handle->execute($csd_id,$stock_id,$dsvlt);
               $query_handle->execute($data_id,$dsvlt,$query_date);
               if(defined $debug){print "Sub query_dsv - query #3\n";}
               # Process results
               while (my @data = $query_handle->fetchrow_array())
               {
                    my $date = $data[0];
                    my $value = $data[1];
                    $hash{$date} = $value;
               }
          }
          # If have a datastore gt value and not a lt value
          elsif(not defined $dsvlt and defined $dsvgt)
          {

               #$query = "select date,value from calc_store where calc_store_descriptor_id=? and stock_id=? and value > ? and date=?";
               $query = "select date,value from calc_store_$csd_id where data_id=? and value > ? and date=?";
               $query_handle = $dbh->prepare($query);
               #$query_handle->execute($csd_id,$stock_id,$dsvgt);
               $query_handle->execute($data_id,$dsvgt,$query_date);
               if(defined $debug){print "Sub query_dsv - query #4\n";}
               # Process results
               while (my @data = $query_handle->fetchrow_array())
               {
                    my $date = $data[0];
                    my $value = $data[1];
                    $hash{$date} = $value;
               }
          }
          # If have a datastore gt and a lt value
          elsif(defined $dsvlt and defined $dsvgt)
          {

               #$query = "select date,value,id from calc_store where calc_store_descriptor_id=? and stock_id=? and value > ? and value < ? and date=?";
               $query = "select date,value,id from calc_store_$csd_id where data_id=? and value > ? and value < ? and date=?";
               $query_handle = $dbh->prepare($query);
               #$query_handle->execute($csd_id,$stock_id,$dsvgt, $dsvlt);
               $query_handle->execute($data_id,$dsvgt, $dsvlt,$query_date);
               if(defined $debug){print "Sub query_dsv - query #5\n";}
               # Process results
               while (my @data = $query_handle->fetchrow_array())
               {
                    my $date = $data[0];
                    my $value = $data[1];
                    $hash{$date} = $value;
               }
          }
     }

     # Return the hash reference
     return(\%hash);
}


##
# This fx is much the same as query_dsv except that it has another csd_id provided as that is
# what is compared to
# This function is invoked by run_query function
# Expects:
#    One or more of dsv, dsvlt, dsvgt 
#    data_id - which is often stock_id
#    datastore id (csd_id)
#    A second datastore id (csdr_id)
#    dates to run the query for (@query_dates)
# Returns:
#    A hash with the values keyed by date
##
sub query_dsrv
{
     # Our relative values
     my $dsrv = $_[0];
     my $dsrvlt = $_[1];
     my $dsrvgt = $_[2];
     if (defined $debug){print "Sub query_dsrv Vars: $dsrv, $dsrvlt, $dsrvgt\n";}

     # Our relative values percentafied
     if(defined $dsrv){$dsrv = $dsrv / 100;}
     if(defined $dsrvlt){$dsrvlt = $dsrvlt / 100;}
     if(defined $dsrvgt){$dsrvgt = $dsrvgt / 100;}
     if (defined $debug){print "Sub query_dsrv Percentified Vars: $dsrv, $dsrvlt, $dsrvgt\n";}

     # The data we are working on
     my $data_id = $_[3];

     # Our CSD against which we are doing relative query
     my $csd_id = $_[4];

     # Our CSD whose values we are doing relative comparisons against CSD
     my $csdr_id = $_[5];

     # Our dates
     my @query_dates = @{$_[6]};

     # Vars
     # Our db query stuff
     my $query;
     my $query_handle;

     # Our return value hash
     my %hash;

     # Date values that match
     my @dates;

     # This is the gold:
     #select dsrTable.date, dsrTable.value, dsTable.value from calc_store as dsrTable, calc_store as dsTable where dsrTable.calc_store_descriptor_id=50 and dsTable.calc_store_descriptor_id=44 and dsrTable.stock_id=16182 and dsTable.stock_id=16182 and dsrTable.date = dsTable.date and dsrTable.value < (dsTable.value * 1.05) order by date asc;


     foreach my $query_date (@query_dates)
     {
          #######################
          # start dsrv processing
          #######################
          # As this function is currently called this will never be the case...
          # Perhaps it should.  Needs to be called without any of the exact criteria
          if(not defined $dsrv and not defined $dsrvlt and not defined $dsrvgt)
          {
               if (defined $debug){print "Sub query_dsrv: Q1 \n";}
               $query = "select date,value from calc_store_$csd_id where data_id=? and date=?";
               if(defined $debug){print "$query\n";}
               $query_handle = $dbh->prepare($query);
               $query_handle->execute($data_id,$query_date);
               while (my @data = $query_handle->fetchrow_array())
               {
                    # The date value
                    my $date = $data[0];
                        
                    # Our relative ds value
                    my $dsrvalue = $data[1];

                    # Our non-relative value (only used in debug below)
                    my $dsvalue = $data[2];

                    # Assign
                    $hash{$date} = $dsrvalue;

                    if(defined $debug)
                    {
                         print "Date: $date, DS Value $dsvalue, DSR Value $dsrvalue ->  dsrv:$dsrv, dsrvlt:$dsrvlt, dsrvgt:$dsrvgt \n";
                    }

               }
          }
          # If we have an exact dsrv
          elsif(defined $dsrv and not defined $dsrvlt and not defined $dsrvgt)
          {
               if (defined $debug){print "Sub query_dsrv: Q2 \n";}
               #$query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store as dsrTable, calc_store as dsTable where dsrTable.calc_store_descriptor_id=? and dsTable.calc_store_descriptor_id=? and dsrTable.stock_id=? and dsTable.stock_id=? and dsrTable.date=dsTable.date and dsrTable.value = (dsTable.value * ?) and dsrTable.date=?;";
               $query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store_$csdr_id as dsrTable, calc_store_$csd_id as dsTable where dsrTable.data_id=? and dsTable.data_id=? and dsrTable.date=dsTable.date and dsrTable.value = (dsTable.value * ?) and dsrTable.date=?;";
               if(defined $debug){print "$query\n";}
               $query_handle = $dbh->prepare($query);
               $query_handle->execute($data_id,$data_id,$dsrv,$query_date);
               while (my @data = $query_handle->fetchrow_array())
               {
                    # The date value
                    my $date = $data[0];

                    # Our relative ds value
                    my $dsrvalue = $data[1];

                    # Our non-relative value (only used in debug below)
                    my $dsvalue = $data[2];

                    # Assign
                    $hash{$date} = $dsrvalue;

                    if(defined $debug)
                    {
                         print "Date: $date, DS Value $dsvalue, DSR Value $dsrvalue ->  dsrv:$dsrv, dsrvlt:$dsrvlt, dsrvgt:$dsrvgt \n";
                    }

               }
          }
          # If we have a lt dsrv and not a gt dsrv
          elsif(defined $dsrvlt and not defined $dsrvgt)
          {
               if (defined $debug){print "Sub query_dsrv: Q3 \n";}
               #$query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store as dsrTable, calc_store as dsTable where dsrTable.calc_store_descriptor_id=? and dsTable.calc_store_descriptor_id=? and dsrTable.stock_id=? and dsTable.stock_id=? and dsrTable.date=dsTable.date and dsrTable.value < (dsTable.value * ?) and dsrTable.date=?;";
               $query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store_$csdr_id as dsrTable, calc_store_$csd_id as dsTable where dsrTable.data_id=? and dsTable.data_id=? and dsrTable.date=dsTable.date and dsrTable.value < (dsTable.value * ?) and dsrTable.date=?;";
               if(defined $debug){print "$query\n";}
               if(defined $debug){print "options:$csdr_id,$csd_id,$data_id,$dsrvlt\n";}
               $query_handle = $dbh->prepare($query);
               $query_handle->execute($data_id,$data_id,$dsrvlt,$query_date);
               while (my @data = $query_handle->fetchrow_array())
               {
                    # The date value
                    my $date = $data[0];
                        
                    # Our relative ds value
                    my $dsrvalue = $data[1];

                    # Our non-relative value (only used in debug below)
                    my $dsvalue = $data[2];

                    # Assign
                    $hash{$date} = $dsrvalue;

                    if(defined $debug)
                    {
                         print "Date: $date, DS Value $dsvalue, DSR Value $dsrvalue ->  dsrv:$dsrv, dsrvlt:$dsrvlt, dsrvgt:$dsrvgt \n";
                    }

               }
          }
          # If we have a gt dsrv and not a lt dsrv
          elsif(not defined $dsrvlt and defined $dsrvgt)
          {
               if (defined $debug){print "Sub query_dsrv: Q4 \n";}
               #$query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store as dsrTable, calc_store as dsTable where dsrTable.calc_store_descriptor_id=? and dsTable.calc_store_descriptor_id=? and dsrTable.stock_id=? and dsTable.stock_id=? and dsrTable.date=dsTable.date and dsrTable.value > (dsTable.value * ?) and dsrTable.date=?;";
               $query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store_$csdr_id as dsrTable, calc_store_$csd_id as dsTable where dsrTable.data_id=? and dsTable.data_id=? and dsrTable.date=dsTable.date and dsrTable.value > (dsTable.value * ?) and dsrTable.date=?;";
               if(defined $debug){
                    print "$query\n";
                    print "Query Args: $csdr_id,$csd_id,$data_id,$dsrvgt,$query_date\n";
                    }
               $query_handle = $dbh->prepare($query);
               $query_handle->execute($data_id,$data_id,$dsrvgt,$query_date);
               while (my @data = $query_handle->fetchrow_array())
               {
                    # The date value
                    my $date = $data[0];
                        
                    # Our relative ds value
                    my $dsrvalue = $data[1];

                    # Our non-relative value (only used in debug below)
                    my $dsvalue = $data[2];

                    # Assign
                    $hash{$date} = $dsrvalue;

                    if(defined $debug)
                    {
                         print "Date: $date, DS Value $dsvalue, DSR Value $dsrvalue ->  dsrv:$dsrv, dsrvlt:$dsrvlt, dsrvgt:$dsrvgt \n";
                    }

               }
          }
          # If we have a gt dsrv and a lt dsrv
          elsif(defined $dsrvlt and defined $dsrvgt)
          {
               if (defined $debug){print "Sub query_dsrv: Q5 \n";}
               #$query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store as dsrTable, calc_store as dsTable where dsrTable.calc_store_descriptor_id=? and dsTable.calc_store_descriptor_id=? and dsrTable.stock_id=? and dsTable.stock_id=? and dsrTable.date=dsTable.date and dsrTable.value > (dsTable.value * ?) and dsrTable.value < (dsTable.value * ?) and dsrTable.date=?;";
               $query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store_$csdr_id as dsrTable, calc_store_$csd_id as dsTable where dsrTable.data_id=? and dsTable.data_id=? and dsrTable.date=dsTable.date and dsrTable.value > (dsTable.value * ?) and dsrTable.value < (dsTable.value * ?) and dsrTable.date=?;";
               if(defined $debug){print "$query\n";}
               if(defined $debug){print "options:$csdr_id,$csd_id,$data_id,$dsrvgt, $dsrvlt\n";}
               $query_handle = $dbh->prepare($query);
               $query_handle->execute($data_id,$data_id,$dsrvgt, $dsrvlt,$query_date);
               while (my @data = $query_handle->fetchrow_array())
               {
                    # The date value
                    my $date = $data[0];
                        
                    # Our relative ds value
                    my $dsrvalue = $data[1];

                    # Our non-relative value (only used in debug below)
                    my $dsvalue = $data[2];

                    # Assign
                    $hash{$date} = $dsrvalue;

                    if(defined $debug)
                    {
                         print "Date: $date, DS Value $dsvalue, DSR Value $dsrvalue ->  dsrv:$dsrv, dsrvlt:$dsrvlt, dsrvgt:$dsrvgt \n";
                    }
               }
          }
     }
     # Return our matching values
     return(\%hash);
}


##
#   REDO DESCRIPTION
# This fx is much the same as query_dsvr except that it uses a direct table value to compare
# This function is invoked by run_query function
# Expects:
#    One or more of TDrv, TDrvlt, TDrvgt 
#    data_id - used to be stock_id
#    datastore id (csd_id)
#    A table and column
#    dates to run the query for (@query_dates)
# Returns:
#    A hash with the values keyed by date
##
sub query_TDrv
{

#dlete
my $csdr_id;
my $dsrv;
my $dsrvlt;
my $dsrvgt;
#dlete
     # Our relative value source
     my $TDrt = $_[0];
     my $TDrc = $_[1];
     my $TDrkey = $_[2];
     # Our relative values
     my $TDrv = $_[3];
     my $TDrvlt = $_[4];
     my $TDrvgt = $_[5];
     if (defined $debug){print "Sub query_TDrv Vars: $TDrv, $TDrvlt, $TDrvgt\n";}

     # Our relative values percentafied
     if(defined $TDrv){$TDrv = $TDrv / 100;}
     if(defined $TDrvlt){$TDrvlt = $TDrvlt / 100;}
     if(defined $TDrvgt){$TDrvgt = $TDrvgt / 100;}
     if (defined $debug){print "Sub query_TDrv Percentified Vars: $TDrv, $TDrvlt, $TDrvgt\n";}

     # The data we are working on - often a stock
     my $data_id = $_[6];

     # Our CSD against which we are doing relative query
     my $csd_id = $_[7];

     # Our dates
     my @query_dates = @{$_[8]};

     # Vars
     # Our db query stuff
     my $query;
     my $query_handle;

     # Our return value hash
     my %hash;

     # Date values that match
     my @dates;

     # This is the gold:
     #select dsrTable.date, dsrTable.value, dsTable.value from calc_store as dsrTable, calc_store as dsTable where dsrTable.calc_store_descriptor_id=50 and dsTable.calc_store_descriptor_id=44 and dsrTable.stock_id=16182 and dsTable.stock_id=16182 and dsrTable.date = dsTable.date and dsrTable.value < (dsTable.value * 1.05) order by date asc;


     foreach my $query_date (@query_dates)
     {
          #######################
          # start TDrv processing
          #######################
          # As this function is currently called this will never be the case...
          # Perhaps it should.  Needs to be called without any of the exact criteria
          if(not defined $TDrv and not defined $TDrvlt and not defined $TDrvgt)
          {
               if (defined $debug){print "Sub query_TDrv: Q1 \n";}
               #$query = "select date,value from calc_store where calc_store_descriptor_id=? and stock_id=? order by date asc";
               $query = "select date,value from calc_store_$csd_id where data_id=? and date=?";
               if(defined $debug){print "$query\n";}
               $query_handle = $dbh->prepare($query);
               $query_handle->execute($data_id,$query_date);
               while (my @data = $query_handle->fetchrow_array())
               {
                    # The date value
                    my $date = $data[0];
                        
                    # Our relative ds value
                    my $TDrvalue = $data[1];

                    # Our non-relative value (only used in debug below)
                    my $TDvalue = $data[2];

                    # Assign
                    $hash{$date} = $TDrvalue;

                    if(defined $debug)
                    {
                         print "Date: $date, TD Value $TDvalue, TDR Value $TDrvalue ->  TDrv:$TDrv, TDrvlt:$TDrvlt, TDrvgt:$TDrvgt \n";
                    }

               }
          }
          # If we have an exact TDrv
          elsif(defined $TDrv and not defined $TDrvlt and not defined $TDrvgt)
          {
               if (defined $debug){print "Sub query_TDrv: Q2 \n";}
               #$query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store as dsrTable, calc_store as dsTable where dsrTable.calc_store_descriptor_id=? and dsTable.calc_store_descriptor_id=? and dsrTable.stock_id=? and dsTable.stock_id=? and dsrTable.date=dsTable.date and dsrTable.value = (dsTable.value * ?) and dsrTable.date=?;";
               $query = "select dsrTable.date, dsrTable.$TDrc, dsTable.value from $TDrt as dsrTable, calc_store_$csd_id as dsTable where dsrTable.$TDrkey=? and dsTable.data_id=? and dsrTable.date=dsTable.date and dsrTable.$TDrc = (dsTable.value * ?) and dsrTable.date=?;";
               if(defined $debug){print "$query\n";}
               $query_handle = $dbh->prepare($query);
               #$query_handle->execute($csdr_id,$csd_id,$stock_id,$stock_id,$dsrv,$query_date);
               $query_handle->execute($data_id,$data_id,$TDrv,$query_date);
               while (my @data = $query_handle->fetchrow_array())
               {
                    # The date value
                    my $date = $data[0];

                    # Our relative ds value
                    my $dsrvalue = $data[1];

                    # Our non-relative value (only used in debug below)
                    my $dsvalue = $data[2];

                    # Assign
                    $hash{$date} = $dsrvalue;

                    if(defined $debug)
                    {
                         print "Date: $date, DS Value $dsvalue, DSR Value $dsrvalue ->  dsrv:$TDrv, dsrvlt:$TDrvlt, dsrvgt:$TDrvgt \n";
                    }

               }
          }
          # If we have a lt TDrv and not a gt TDrv
          elsif(defined $TDrvlt and not defined $TDrvgt)
          {
               if (defined $debug){print "Sub query_dsrv: Q3 \n";}
               #$query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store as dsrTable, calc_store as dsTable where dsrTable.calc_store_descriptor_id=? and dsTable.calc_store_descriptor_id=? and dsrTable.stock_id=? and dsTable.stock_id=? and dsrTable.date=dsTable.date and dsrTable.value < (dsTable.value * ?) and dsrTable.date=?;";
               $query = "select dsrTable.date, dsrTable.$TDrc, dsTable.value from $TDrt as dsrTable, calc_store_$csd_id as dsTable where dsrTable.$TDrkey=? and dsTable.data_id=? and dsrTable.date=dsTable.date and dsrTable.$TDrc < (dsTable.value * ?) and dsrTable.date=?;";
               if(defined $debug){print "$query\n";}
               #if(defined $debug){print "options:$csdr_id,$csd_id,$stock_id,$stock_id,$dsrvlt\n";}
               if(defined $debug){print "options:$$csd_id,$data_id,$TDrvlt\n";}
               $query_handle = $dbh->prepare($query);
               $query_handle->execute($data_id,$data_id,$TDrvlt,$query_date);
               while (my @data = $query_handle->fetchrow_array())
               {
                    # The date value
                    my $date = $data[0];
                        
                    # Our relative ds value
                    my $dsrvalue = $data[1];

                    # Our non-relative value (only used in debug below)
                    my $dsvalue = $data[2];

                    # Assign
                    $hash{$date} = $dsrvalue;

                    if(defined $debug)
                    {
                         print "Date: $date, DS Value $dsvalue, DSR Value $dsrvalue ->  dsrv:$TDrv, dsrvlt:$TDrvlt, dsrvgt:$TDrvgt \n";
                    }

               }
          }
          # If we have a gt TDrv and not a lt TDrv
          elsif(not defined $TDrvlt and defined $TDrvgt)
          {
               if (defined $debug){print "Sub query_dsrv: Q4 \n";}
               #$query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store as dsrTable, calc_store as dsTable where dsrTable.calc_store_descriptor_id=? and dsTable.calc_store_descriptor_id=? and dsrTable.stock_id=? and dsTable.stock_id=? and dsrTable.date=dsTable.date and dsrTable.value > (dsTable.value * ?) and dsrTable.date=?;";
               $query = "select dsrTable.date, dsrTable.$TDrc, dsTable.value from $TDrt as dsrTable, calc_store_$csd_id as dsTable where dsrTable.$TDrkey=? and dsTable.data_id=? and dsrTable.date=dsTable.date and dsrTable.$TDrc > (dsTable.value * ?) and dsrTable.date=?;";
               if(defined $debug){
                    print "$query\n";
                    print "Q4 Query AArgs: $data_id,$data_id,$TDrvgt,$query_date -- $query";
                    }
               $query_handle = $dbh->prepare($query);
               $query_handle->execute($data_id,$data_id,$TDrvgt,$query_date);
               while (my @data = $query_handle->fetchrow_array())
               {
                    # The date value
                    my $date = $data[0];
                        
                    # Our relative ds value
                    my $dsrvalue = $data[1];

                    # Our non-relative value (only used in debug below)
                    my $dsvalue = $data[2];

                    # Assign
                    $hash{$date} = $dsrvalue;

                    if(defined $debug)
                    {
                         print "Date Q4: $date, DS Value $dsvalue, DSR Value $dsrvalue ->  dsrv:$TDrv, dsrvlt:$TDrvlt, dsrvgt:$TDrvgt \n";
                    }

               }
          }
          # If we have a gt TDrv and a lt TDrv
          elsif(defined $TDrvlt and defined $TDrvgt)
          {
               if (defined $debug){print "Sub query_dsrv: Q5 \n";}
               #$query = "select dsrTable.date,dsrTable.value,dsTable.value from calc_store as dsrTable, calc_store as dsTable where dsrTable.calc_store_descriptor_id=? and dsTable.calc_store_descriptor_id=? and dsrTable.stock_id=? and dsTable.stock_id=? and dsrTable.date=dsTable.date and dsrTable.value > (dsTable.value * ?) and dsrTable.value < (dsTable.value * ?) and dsrTable.date=?;";
               $query = "select dsrTable.date, dsrTable.$TDrc, dsTable.value from $TDrt as dsrTable, calc_store_$csd_id as dsTable where dsrTable.$TDrkey=? and dsTable.data_id=? and dsrTable.date=dsTable.date and dsrTable.$TDrc > (dsTable.value * ?) and dsrTable.$TDrc < (dsTable.value * ?) and dsrTable.date=?;";
               if(defined $debug){print "$query\n";}
               if(defined $debug){print "options:$csd_id,$data_id,$TDrvgt, $TDrvlt\n";}
               $query_handle = $dbh->prepare($query);
               #$query_handle->execute($csdr_id,$csd_id,$stock_id,$stock_id,$dsrvgt, $dsrvlt,$query_date);
               $query_handle->execute($data_id,$data_id,$TDrvgt,$TDrvlt,$query_date);
               while (my @data = $query_handle->fetchrow_array())
               {
                    # The date value
                    my $date = $data[0];
                        
                    # Our relative ds value
                    my $dsrvalue = $data[1];

                    # Our non-relative value (only used in debug below)
                    my $dsvalue = $data[2];

                    # Assign
                    $hash{$date} = $dsrvalue;

                    if(defined $debug)
                    {
                         print "Date: $date, DS Value $dsvalue, DSR Value $dsrvalue ->  dsrv:$TDrv, dsrvlt:$TDrvlt, dsrvgt:$TDrvgt \n";
                    }
               }
          }
     }
     # Return our matching values
     return(\%hash);
}

#Mark#22
##
# This  
# Expects: 
#    A specific slope value
#    A less than slope value
#    A greater than slope value
#    A data ID - which is often a stock ID
#    An array of dates to test
#    A window of time to calculate slope (slope_window)
#    A slope offset relative to the end measured in days
#         This is useful for determining the rate of change
#         towards the end of the window
#         This is given as a percentage of the slope_window
# Returns:
#     A hash with slope value keyed by date.
##
sub query_slope
{

     # Our slope values
     my $slopev = $_[0];
     my $slopelt = $_[1];
     my $slopegt = $_[2];
     my $data_id = $_[3];

     # Our data source
     my $csd_id = $_[4];

     # Our dates
     my @our_dates = @{$_[5]};

     # Our slope window
     my $slope_window = $_[6];

     # Slope offset relative to the end
     my $slope_offset = $_[7];
     my $slope_offset_days;

     # Our db query stuff
     my $query;
     my $query_handle;

     # Calc store values
     my $value1;
     my $value2;

     # Our x axis and calculated slope
     my $x;
     my $slope;

     my %return_hash;

     if (defined $debug){print "Sub query_slope Vars: $slopev, $slopelt, $slopegt, CSD: $csd_id, Slope Window: $slope_window, Slope Offset: $slope_offset\n";}

     # Iterate through
     foreach my $date (@our_dates)
     {
          if(defined $debug){print "Sub query_slope Vars: $slopev, $slopelt, $slopegt, CSD: $csd_id, Slope Window: $slope_window , Slope Offset: $slope_offset\n";}

          # Just in case an offset is specified, convert to days
          if($slope_offset > 0)
          {
               $slope_offset_days = int(($slope_window * ($slope_offset * .01)) + .5);
          }else{
               $slope_offset_days = "0";  
          }

          my $start_date = &get_epoch_minus_days($date,$slope_window,$data_id);
          my $end_date = &get_epoch_minus_days($date,$slope_offset_days,$data_id);

          # Let's account for the possibility of an imperfect start date
          $query = "select value from calc_store_$csd_id where date >= ? and data_id = ? order by date asc limit 1";
          $query_handle = $dbh->prepare($query);

          # Lets get start value
          $query_handle->execute($start_date,$data_id);

          my $good_time;
          my $symbol;
          # Iterating throught results
          while (my @data = $query_handle->fetchrow_array())
          {
               # Our data value 
               $value1 = $data[0];

               if(defined $debug)
               {
                    #$good_time = &mytime($start_date);
                    #$symbol = &get_stock_symbol($stock_id);
                    #print "FX: query_slope - Start Date: $good_time, DS Value $value1, Stock Symbol: $symbol \n";
               }
          }

          # Let's account for the possibility of an imperfect end date
          $query = "select value from calc_store_$csd_id where date <= ? and data_id = ? order by date desc limit 1";
          $query_handle = $dbh->prepare($query);

          # Lets get end value
          $query_handle->execute($end_date,$data_id);
          if(defined $debug){print "Sub: query_slope - select value from calc_store_$csd_id where date <=$end_date and data_id = ? order by date desc limit 1";}

          # Iterating throught results
          while (my @data = $query_handle->fetchrow_array())
          {
               # Our data value 
               $value2 = $data[0];

               if(defined $debug)
               {
                    #$good_time = &mytime($end_date);
                    #$symbol = &get_stock_symbol($stock_id);
                    #print "FX: query_slope - End Date: $good_time, DS Value $value2, Stock Symbol: $symbol \n";
               }
          }

          $query = "select count(*) from calc_store_$csd_id where date >=? and date <=? and data_id =?"; 
          $query_handle = $dbh->prepare($query);

          # Lets our total values
          $query_handle->execute($start_date,$end_date,$data_id);

          # Iterating throught results
          while (my @data = $query_handle->fetchrow_array())
          {
               # Our data value 
               $x = $data[0];
          }
            
          if ($x gt 0)
          {
               $slope = ($value2 - $value1) / $x;
               if(defined $debug)
               {
                    print "Slope: $slope\n";
                    print "FX: query_slope - End Date: $good_time, DS Values $value1 $value2, Slope: $slope\n";
               }
          }

          if((defined $slopev) and ($slope eq $slopev) and (not defined $slopegt and not defined $slopelt))
          {
               # Assign to return hash
               $return_hash{$date} = $slope;
          }
          elsif(((defined $slopelt) and ($slope < $slopelt)) and ((defined $slopegt) and ($slope > $slopegt)) and (not defined $slopev))
          {
               # Assign to return hash
               $return_hash{$date} = $slope;
          }
          elsif((defined $slopelt) and ($slope < $slopelt) and (not defined $slopev and not defined $slopegt))
          {
               # Assign to return hash
               $return_hash{$date} = $slope;
          }
          elsif((defined $slopegt) and ($slope > $slopegt) and (not defined $slopev and not defined $slopelt))
          {
               # Assign to return hash
               $return_hash{$date} = $slope;
          }
     }
     
     return(\%return_hash);

}

##
# Provides the csd_id, given the datastore name
# Expects:
#    The datastore name  
# Returns:
#    The datastore id 
##
sub get_csd_id 
{

     # This is our provided name
     my $ds_name = $_[0];

     # Vars
     my $csd_id;

     # This is how we get it
     my $query = "select id from calc_store_descriptor where calc_store_name=?";

     # So get it
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($ds_name);
     while (my @data = $query_handle->fetchrow_array())
     {
          $csd_id = $data[0];
          return($csd_id);
     }
     # If we don't have it, say so
     if(not $csd_id)
     {
          print "There is no datastore named: $ds_name\n";
          exit;
     }
}

##
# This returns the accurate epoch date going back $days (as opposed to just subtracting the number of
# days.  FX accounts for weekends, holidays, etc.
# Expects: 
#    epochtime start time, days, a stock
# Returns: 
#    epochtime - (real stock data days, not just all days)
##
sub get_epoch_minus_days
{
     # Our reference starting point
     my $start_etime = $_[0];
     # The number of days we want to go back (and not count holidays)
     my $days = $_[1];
     # Our stock
     my $stock_id = $_[2];
     # Our date
     my $edate;

     # For this stock go back $days number of datapoints, ?1 is stock, ?2 is time reference point, and ?3 is our days
     my $query = "select date from stock_history where data_id=? and date <=? order by date desc limit ?,1";
     
     # Let us get it
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($stock_id,$start_etime,$days);
     while (my @data = $query_handle->fetchrow_array())
     {
          $edate = $data[0];
          return($edate);
     }
     if(defined $debug)
     {
          print "Function: get_epoch_minus_days - query is -> $query \n";
          print "select date from stock_history where data_id=$stock_id and date <=$start_etime order by date desc limit $days,1\n";
     }
     # If we don't have it, return 0
     if(not $edate)
     {
          return(0);
     }
}

##
# This returns a single value from a column in a table on a given date or some number of records back.
# Expects:
#    A table to query
#    A column with our data 
#    A date for us to query
#    Some number of records back from the provided query date
##
sub get_value
{
     my $table = $_[0];
     my $table_column = $_[1];
     my $data_id = $_[2];
     my $date = $_[3];
     my $offset = $_[4];

     my $query = "select $table_column from $table where date<=? and data_id=? order by date desc limit $offset,1";

     # Let us get it
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute($date,$data_id);

     while (my @data = $query_handle->fetchrow_array())
     {
          my $value = $data[0];
          return($value);
     }
     # If not value return N/A
     return("N/A");
}

##
# This function calculates the percentage change between values from 
# a date and its prior date  in an arbitrary table and column
# Expects:
#    Table name, table column, data_id, date
# Returns:
#    The percentage change expressed as a postive or negative number 
##
sub get_percentage_delta
{

     # Our supplied vars
     my $table = $_[0];
     my $table_column = $_[1];
     my $data_id = $_[2];
     my $date = $_[3];

     # Let's get the values for the current date and the previous date (offset = 1)
     my $old_value = &get_value($table,$table_column,$data_id,$date,1);
     my $new_value = &get_value($table,$table_column,$data_id,$date,0);

     # Let's make sure we can do the math, otherwise return an N/A
     if ((($old_value eq "N/A") or ($old_value == 0)) or ($new_value eq "N/A"))
     {
          print "StockID: $data_id, New Value: $new_value, Old Value: $old_value\n";
          return("N/A");
     }
     
     # Get difference of values
     my $value_delta = $new_value - $old_value;

     # Debug print
     if(defined $debug){ print "StockID: $data_id, New Value: $new_value, Old Value: $old_value, Calculation: ($value_delta / $old_value)\n";}

     # Get percentafied fraction
     my $percentage_delta = sprintf("%.2f",($value_delta / $old_value) * 100);
     #if (defined $debug){print "DataID: $data_id, New Value: $new_value, Old Value: $old_value, Percentage: $percentage_delta, Calculation: ($value_delta / $old_value)\n";}
     print "DataID: $data_id, New Value: $new_value, Old Value: $old_value, Percentage: $percentage_delta, Calculation: ($value_delta / $old_value)\n";
     # 
     return($percentage_delta);
}

##
# Sometimes the extended yahoo query returns bad data which then taints everything else.
# This function finds the bad rows, deletes them, and uses the non-extended method to fix
# Expects:
#    Nothing
# Returns:
#    Nothing
##
sub yahoo_extended_mopup
{
     # Lets find our bad data.  If the first three were 0 then the close would also be 0
     my $query = "select * from stock_history where (open = '0' and high = '0' and low ='0' and close != '0') or (date is NULL) order by data_id";

     # Run it
     my $query_handle = $dbh->prepare($query);
     # and get our count returned
     my $num_rows = $query_handle->execute();

     # Init counter
     my $i = 0;

     # Process
     while (my @data = $query_handle->fetchrow_array())
     {
          # Increment counter
          $i++;

          # Our data
          my $stock_id = $data[1];
          my $symbol = &get_stock_symbol($stock_id);
          my $edate = $data[2];
          my $date = &mytime($edate);
          
          # Print status messages
          print "$i/$num_rows: Fixing bad record for $symbol on $date\n";
          print "  Deleting bad record for $stock_id on $date\n";
          # Delete this line from the DB 
          if($edate > 0)
          {
               my $del_query = "delete from stock_history where data_id=? and date=?";
               my $del_query_handle = $dbh->prepare($del_query);
               $del_query_handle->execute($stock_id,$edate);
          }else{
               my $del_query = "delete from stock_history where data_id=? and date is NULL";
               my $del_query_handle = $dbh->prepare($del_query);
               $del_query_handle->execute($stock_id);
          }

          if ($edate > 0)
          {
               print "  Submitting to stock_history_write fx: $symbol,$stock_id,$date,$date\n\n";
               # Call the query and write function
               &stock_history_write($symbol,$stock_id,$date,$date);
          }
     }
}

#Mark#23
##
#
# Expects:
#    Stock id, date, loss percent, gain percent
# Returns:
#    1 for winner
#    0 for looser
##
sub is_winner
{
     my $stock_id = $_[0];
     my $date = $_[1];

     my $loss_percent = $_[2];
     my $gain_percent = $_[3];
     
     my $close;

     my $price_query = "select close from stock_history where data_id=? and date >=? order by date asc limit 1";
     my $query_handle = $dbh->prepare($price_query);
     $query_handle->execute($stock_id,$date);
     while (my @data = $query_handle->fetchrow_array())
     {
          $close = $data[0];
     }

     my $win_number = $close + ($close * (.01 * $gain_percent));
     my $loose_number = $close - ($close * (.01 * $loss_percent));

     if(defined $debug){print "Sub: is_winner: Win Number - $win_number Loose Number - $loose_number Close - $close\n";}
     my $winner_query = "select close from stock_history where data_id=? and date >=? and (close >=? or close <=?) order by date asc limit 1";
     my $query_handle = $dbh->prepare($winner_query);
     $query_handle->execute($stock_id,$date,$win_number,$loose_number);
     while (my @data = $query_handle->fetchrow_array())
     {
          my $return = $data[0];
          if($return > $close)
          {
               if(defined $debug){print "Sub: is_winner - $return > $close\n";}
               return(1);
          }else{
               if(defined $debug){print "Sub: is_winner - $return < $close\n";}
               return(0);
          }
     }
}

##
# Iterates through an array and compares a second array and finds their intersection
# Expects:
#    Two array refs, and optional argument "difference"
#    If difference option is specified an array of values in array0 not in array1 is returned
# Returns:
#    An array containing their intersections or their differences
##
sub intersection
{ 
     # Dereference
     my @array0 = @{$_[0]};
     my @array1 = @{$_[1]};
     my $option = $_[2];

     my @array_intersection;
     my @array_difference;

     # Iterate through
     foreach my $array0_value (@array0)
     {
          if($array0_value eq "")
          {
               print "blah\n";
          }
          # Perl version of php in_array function
          # If the value from array0 is in array1 then
          # add to intersection array
          
          if(&in_array(\@array1,$array0_value))
          {
               push(@array_intersection, $array0_value); 
          }else{
               push(@array_difference, $array0_value); 
          }
     }
     if($option eq "difference")
     {
          return(@array_difference);
     }else{
          return(@array_intersection);
     }
} 

##
# This function mimics the PHP function in_array
# http://www.go4expert.com/forums/showthread.php?t=8978
# Expects:
#    An array reference, a possible array value 
# Returns:
#    1 if yes
#    0 if no
##
sub in_array {
     my ($arr,$search_for) = @_;
     foreach my $value (@$arr) {
          return 1 if $value eq $search_for;
     }
     return 0;
}

#Mark#24
##
# This dumps the db schema in the current directory
# Expects:
#    Nothing
# Returns:
#    Nothing
##
sub dump_schema {
     my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
     $year = $year + 1900;
     exec("mysqldump -d -u $db_user --password=$db_pass $db > $db\_Schema.$hour-$min-$sec\_$mon-$mday-$year.sql");
}

##
# This dumps the db in the current directory
# Expects:
#    Nothing
# Returns:
#    Nothing
##
sub dump_db {
     my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
     $year = $year + 1900;
     exec("mysqldump -u $db_user --password=$db_pass $db | gzip > $db\_DB.$hour-$min-$sec\_$mon-$mday-$year.sql.gz");
}

##
# This function was used to convert the monolithic calc_store into multiple stores
# Expects:
#    Nothing
# Returns:
#    Nothing
##
sub convert_cs {
     # query csd
     # for each unique create a table with name of calc_store_$id where $id is taken from calc_store_descriptor
     my $query = "select id from calc_store_descriptor";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute();
     while (my @data = $query_handle->fetchrow_array())
     {
          my $csd_id = $data[0];
          unless(&does_table_exist("calc_store_$csd_id"))
          { 
               print "making table calc_store_$csd_id\n";
               my $make_table_query = "create table calc_store_$csd_id 
                    (id int not null auto_increment, 
                    data_id int(11), 
                    date int(11), 
                    value decimal(10,2),
                    primary key (id, date, value))";
               my $query_make_handle = $dbh->prepare($make_table_query);
               $query_make_handle->execute();
               # Convert data
               # Apparently there are some entries in here with a stock_id of 0 (1742 of them)
               my $data_query = "select stock_id, date, value from calc_store where calc_store_descriptor_id=? and stock_id !=0";
               print "$data_query\n";
               print "$csd_id\n";
               my $query_calc_data_handle = $dbh->prepare($data_query);
               $query_calc_data_handle->execute($csd_id);
               while (my @calc_data = $query_calc_data_handle->fetchrow_array())
               {
                    my $data_id = $calc_data[0]; 
                    my $date = $calc_data[1];  
                    my $value = $calc_data[2]; 
                    #print "$data_id, $date, $value\n";

                    my $data_convert_query = "insert into calc_store_$csd_id (data_id, date, value) values ($data_id, $date, $value)";
                    #print "$data_convert_query\n";
                    my $query_convert_handle = $dbh->prepare($data_convert_query);
                    $query_convert_handle->execute();
               }

          }
     }
}

##
# This function checks for the presense of a given db table 
# Expects:
#    A table name 
# Returns:
#    0 if table does not exist
#    1 if table exists
##
sub does_table_exist{

     # Our supplied table
     my $table = $_[0];

     # The check
     my $query = "show tables"; 
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute();
     while (my @data = $query_handle->fetchrow_array())
     {
          if ($table eq $data[0])
          {
          return(1);
          }
     }
     return(0);
}

##
# This function finds all entries of stock_histories where either the
# percentage chage for volume or price hasn't yet been calculated, then
# it calculates them and updates the stock history record
# Expects:
#    A type to check for (price or volume) 
# Returns:
#    Nothing
##
sub create_stock_percentage{

     # Either price or volume
     my $create_type = $_[0];
     
     # Get an array with all of our stocks
     my @stocks = &all_stocks("id");

     # Vars
     my $date;
     my $percentage;
     my $query;

     # Let's iterate through our supplied stocks
     foreach(@stocks)
     {
          # For the current stock
          my $stock_id = $_;

          # Set up price or volume queries
          if($create_type eq "price")
          {
               $query = "select date from stock_history where data_id=? and percentage_change_price is NULL order by date asc";
          }elsif($create_type eq "volume"){
               $query = "select date from stock_history where data_id=? and percentage_change_volume is NULL order by date asc";
          }

          # Debug
          if (defined $debug){print "Sub create_stock_percentage: $query $_\n";}
     
          # Run query
          my $query_handle = $dbh->prepare($query);
          $query_handle->execute($stock_id);
          while (my @data = $query_handle->fetchrow_array())
          {
               # Iterate through our dates without values
               $date = $data[0];
               if($create_type eq "price")
               {
                    # Let's get our value
                    $percentage = &get_percentage_delta("stock_history","close",$stock_id,$date);
                    # and update the record
                    my $update_query = "update stock_history set percentage_change_price=? where data_id=? and date=?";
                    my $query_update_handle = $dbh->prepare($update_query);
                    $query_update_handle->execute($percentage,$stock_id,$date);
               }elsif($create_type eq "volume"){
                    # do the same here if we were asked
                    $percentage = &get_percentage_delta("stock_history","volume",$stock_id,$date);
                    my $update_query = "update stock_history set percentage_change_volume=? where data_id=? and date=?";
                    my $query_update_handle = $dbh->prepare($update_query);
                    $query_update_handle->execute($percentage,$stock_id,$date);
               }         
               if (defined $debug){print "StockID $stock_id, $date, $percentage\n";}
          }
     }
}

##
# This function updates industry_history, either for all industries and/or a date
# or a specific industry and/or date.  Depending on what it has been supplied
# with it fills in the blanks and calls itself again
# Expects:
#    Nothing, or an array of industries to update, and/or a specific date to check
# Returns:
#    Nothing
##
sub update_industries
{

     # Vars
     my @industry_id_list;
     my $date;

     # Checking for an industry list
     if($_[0])
     {
          @industry_id_list = @{$_[0]};
     }
     
     # If we are called without a list of industries get one
     if (not @industry_id_list)
     {
          @industry_id_list = &get_industry_id_list;

          # Debug
          if(defined $debug)
          {
               my $count = scalar(@industry_id_list);
               print "Sub update_industries: industry id count $count\n";
          }
     }

     # Optional Date - otherwise check all recursively
     if(defined $_[1])
     {
          $date = $_[1];
     }
     
     if (not defined $date)
     {
          # If no date provided let's get our market dates and take it from there
          my @market_dates = &get_market_dates("_ALL_");

          foreach my $indu_id (@industry_id_list)
          {
               my @industry_dates;
               my @industry_passed;
               @industry_passed = ("$indu_id");

               my $industry_id_dates_q = "select date from industry_history where data_id = ?"; 
               my $query_handle = $dbh->prepare($industry_id_dates_q);
               #print "$indu_id\n";
               $query_handle->execute($indu_id);
               while(my @industry_data = $query_handle->fetchrow_array())
               {
                    #print "$industry_data[0]\n";
                    push(@industry_dates, $industry_data[0]);
               }

               my @industry_update_dates = &intersection(\@market_dates,\@industry_dates,"difference");

               #my $mcount = scalar(@market_dates);
               #print "My mdate $mcount\n";
               #my $icount = scalar(@industry_dates);
               #print "My idate $icount\n";
               
               #my $count = scalar(@industry_update_dates);
               #print "My date $count\n";
               foreach my $iupdate (@industry_update_dates)
               {
               #     print "IOUPDSTE $iupdate\n";
                    # Now let's call ourselves with a date
                    #&update_industries(\@industry_id_list, $iupdate);
                    &update_industries(\@industry_passed, $iupdate);
               }
          }

          # At this point we've gone through with all of our dates and industries
          return;
     }
     
     # At this point we should have an @industry_id_list and a single $m_date

     # Some vars that we want to fill
     my $volume;
     my $price_percentage;

     foreach(@industry_id_list)
     {
          # Our current industry id
          my $industry_id = $_;

          # Query giving us the sum of the volume and avg unweighted percentage delta
          my $query ="
          SELECT 
              sum(sh.volume),
              avg(sh.percentage_change_price),
              s.industry_id
          FROM
              stock_history as sh,
              stock as s
          where
              sh.data_id = s.id
              and s.status= 'active'
              and sh.date=?
              and s.industry_id =?";
          my $query_handle = $dbh->prepare($query);
          $query_handle->execute($date,$industry_id);
          while(my @industry_numbers = $query_handle->fetchrow_array())
          {
              $volume = $industry_numbers[0];
              $price_percentage = $industry_numbers[1];

          }
          # I was debating whether or not to include the below in the industry_history record
          #my $volume_percentage_change = &get_percentage_delta("industry_history","volume",$industry_id,$date);
          #my $price_percentage_change = &get_percentage_delta("industry_history","volume",$industry_id,$date);

          # Printing status
          print "Updating Industry #$industry_id, Volume: $volume, Price Per: $price_percentage, Date: $date\n";

          # Updating record (data_id and date are key together so there should be no duplicate entries)
          my $insert_query = "insert into industry_history (data_id,date,price_percentage_unweighted,volume) values(?,?,?,?)";
          my $query_handle = $dbh->prepare($insert_query);
          $query_handle->execute($industry_id,$date,$price_percentage,$volume);
     }
}

##
# Just a function to gives a list of industries by id 
# Expects:
#    Nothing
# Returns:
#    An array of industry id values
##
sub get_industry_id_list
{
     my @industry_id_list;
     my $query = "select id from industry order by id asc";
     my $query_handle = $dbh->prepare($query);
     $query_handle->execute; 
     while(my @data = $query_handle->fetchrow_array())
     {
          push(@industry_id_list, $data[0]);
     }
     return(@industry_id_list);
}

##
# Function providing exchange of stock 
# Expects:
#    Symbol 
#    Optional specific tests such as:
#    minor_us
#    major_us
#    in_active
# Returns:
#    Exchange where symbol is listed 
##
sub get_exchange
{

     # Our supplied 
     my $symbol = $_[0];
     my $option = $_[1];

     useExtendedQueryFormat();     # switch to extended query format
     my @quotes = getcustomquote([$symbol], # using custom format
          ["Stock Exchange"]); # note array refs

     my $exchange = $quotes[0][0];

     #print "$exchange: $symbol\n";

     if (($option eq "minor_us") and (($exchange eq "NCM") or ($exchange eq "Other OTC") or ($exchange eq "OTC BB") or ($exchange eq "PCX")))
     {
          if(defined $debug){print "Sub get_exchange: Minor - Stock: $symbol, Exchange: $exchange\n";}
          #print "$exchange: $symbol\n";
          return(1);
     }
     elsif(($option eq "major_us") and (($exchange eq "NYSE") or ($exchange eq "NasdaqNM") or ($exchange eq "NGM") or ($exchange eq "AMEX")))
     {
          if(defined $debug){print "Sub get_exchange: Major - Stock: $symbol, Exchange: $exchange\n";}
          #print "$exchange: $symbol\n";
          return(1);
     }
     elsif(($option eq "in_active") and ($exchange eq "N/A"))
     {
          if(defined $debug){print "Sub get_exchange: N/A - Stock: $symbol, Exchange: $exchange\n";}
          #print "$exchange: $symbol\n";
          return(1);
     }
     elsif(defined $option){
          # If we are here, none of our defined tests are true
          return(0);
     }
     else
     {
          # If we are here then just return the exchange
          return("$exchange");
     }
}

##
# Function testing for a certain type of exchange and if
#    not that it marks the stock inactive, the reason why 
#    and the time the decision was made
# Expects:
#    Nothing
# Returns:
#    Nothing 
##
sub test_exchange
{
     my @stocks = &all_stocks("byname");

     foreach(@stocks)
     {
          my $stock = $_;

          #print $stock;
          if(&get_exchange($stock,"major_us"))
          {
               print "test_exchange: Major: $stock\n";
               next;
          }
          elsif(&get_exchange($stock,"minor_us"))
          {
               print "test_exchange: Minor: $stock\n";
               my $query = "update stock set status = 'inactive', status_change_reason = 'minor', status_change_date = UNIX_TIMESTAMP() where symbol = ?";
               print "MARKING INACTIVE (MINOR) ->    $stock\n";
               print "$query\n";
               my $query_handle = $dbh->prepare($query);
               $query_handle->execute($stock); 
          }
          elsif(&get_exchange($stock,"in_active"))
          {
               print "test_exchange: N/A: $stock\n";
               my $query = "update stock set status = 'inactive', status_change_reason = 'inactive', status_change_date = UNIX_TIMESTAMP() where symbol = ?";
               print "MARKING INACTIVE (INACTIVE) ->   $stock\n";
               print "$query\n";
               my $query_handle = $dbh->prepare($query);
               $query_handle->execute($stock); 
          }
     }
}
