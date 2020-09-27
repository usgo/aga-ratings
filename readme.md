## AGA Ratings Scripts - Perl Scripts

The AGA Ratings scripts have two parts to run and fully update information. These scripts are the first half of the process. These perform first pass inspections for rating tournaments and then formatting and adding to the AGAGD. 

Ratings checks:  
*Current Membership checks  
*Unpaid Memberships  
*Name checks against DB  
*Too many rounds/komi/handicap  

## Setup AGA-Ratings Script
Open presults.pl and look for 'getdbiconnection  
Fill in $dsn, $user, $password  
  
Add tournaments to 'tournaments' folder  
Files are results.txt files renamed to 'results.in'  

*Help: -h*
```$ ./presults.pl -h

<progname>:
        help (-h)               print this message
        revert (-r)             remove the tournament from the database
        shoot_blanks (-n)       do NOT modify the database unless this value is 0
        online (-o)             online tournament, not to be rated
                                default value is 2
        verbose (-s)            print additional debugging messages as selected
                                example: "-s :parse_fix_file:"
./presults.pl: no filename
```

## Add Tournaments
>$ ./presults tourneyDate.in

Skip the first section with games and round information, look after $VAR1
$VAR1 has basic tournament information, diagnostics come after 
Run first to check issues, check name and location is displayed correctly.  
Tournament name is not displayed: File needs to have "TOURNEY" before tournament name  
Location is not displayed: location='Seattle, WA' is not in the header information  

>$ ./presults -n 0 tourneyDate.in  

This will add the tournament to the database and it will show on the AGAGD. Tournament should list name, city, state, total players, rounds, and show 00-00-0000 for rated date. 

>$ ./presults -r -n 0 tourneyDate.in

The -r removes the tournament from the DB. This may happen when there is an issue with the file that you did not catch in the beginning and need to make a quick change. If this changes after rating you will need to fully rerate again.

Once the tournament is in the AGAGD and needs to be rated, follow instructions for AGA-Ratings-Program to check for further issues and run bayrate.
