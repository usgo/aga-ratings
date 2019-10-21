#!/usr/bin/perl

# !./parse_tde.pl 20081123.tde | more
#
# parse a TDE file
# for players that do not already exist in the usgo_agagd database
#	(aga_id, lastname, firstname matching defines existence)
#	if there is sufficient player data from the TDE file
#		(aga_id, lastname, firstname defines sufficient)
#	and if the player's chapter is in the chapter table
# 	and if the player's state is in the state table
#	THEN insert the player into the players table
#
# TO BE DONE:
# for existing players
#	update Rating, Sigma, and set Elab_date to match Rating, Sigma and Date
# 	update MType and Mexp
# 	insert MType and Mexp
#
#
# tdlista input format
# 	Aal, Zachary	15648	Full	-20.05712	7/2/2008	LIGC	NY	1.091908	8/1/2007
# 	Aarhus, Bob	9616	Full	-21.94348	1/1/2008	NOVA	VA	.873411	11/1/2001
#	....
#
use strict;
use Data::Dumper;
use DBI;
use File::Basename;
use Getopt::Long;

my %Chapters;
my %Countries;
my %Players;
my %Player_Pins;
my %States;
my $input_file;
my $input_type;
my @base;
my $broken_data = 0;
my $chapter = 0;
my $country_update = 0;
my $no_country = 0;
my $no_state = 0;
my $new_data = 0;
my $membership_exp = 0;
my $membership_type = 0;
my $old_data = 0;
my $prevents = ":update_rating_sigma:new_rating_sigma:update_rating_sigma:";
#my $prevents = ":all_inserts:all_updates:country:country_update:no_country:new_rating_sigma:update_rating_sigma:new_rating_date:state:";
my $date_update = 0;
my $date_new = 0;
my $rating_update = 0;
my $rating_sigma_only = 0;
my $rating_new = 0;
my $reject_file = "";
my $sigma_update = 0;
my $sigma_new = 0;
my $shoot_blanks = 1;
my $state_update = 0;
my $verbose = ":unknown_states:broken_data:inserts:updates:";
#my $verbose = ":broken_data:database_records:unknown_states:";
#my $verbose = ":broken_data:database_records:foreign_states:get_country_codes:parse_tde_file:rating_sigma:set_US_if_state:silly_country_codes:unknown_states:state_change:inserts:updates:parse_tda_file:chapters:";

my %other;

# replace silly country code with proper ones
my $SillyCountryFixes = {
	  'INSISTS)' => 'NC',
	  'ARABI' => 'Saudi Arabia',
          'BRITAIN' =>  'United Kingdom',
	  'CH44 OEP' => 'United Kingdom',
          'CROATIA' => 'Croatia (Hrvatska)',
          'CZECH' => 'Czech Republic',
          'ENGLAND UK' =>  'United Kingdom',
          'ENGLAND' =>  'United Kingdom',
          'GBRIT' =>  'United Kingdom',
          'ITALIA' => 'Italy',
          'HOLLAND' => 'Netherlands',
          'HONG KONG CHINA' => 'Hong Kong',
          'KOREA' => 'Korea (South)',
	  'M4G3K8' => 'Canada',
          'NETHERLAND' => 'Netherlands',
          'NEW ZEALAND' => 'New Zealand (Aotearoa)',
          'THE NETHERLANDS' => 'Netherlands',
          'PRC CHINA' => 'China',
          'P.R. CHINA' => 'China',
          'P. R. CHINA' => 'China',
          'ROC' => 'Taiwan',
          'RUSSIA' => 'Russian Federation',
          'SCOTLAND' =>  'United Kingdom',
	  'SK' => 'Slovakia',
          'SOUTH KOREA' => 'Korea (South)',
	  'SWITZ' => 'Switzerland',
          'TIAWAN' => 'Taiwan',
          'TAIWAN, R.O.C.' => 'Taiwan',
          'UK' => 'United Kingdom',
          'UKRANIA' => 'Ukraine',
          'USSR' => 'Russian Federation',
          'UNITED KINGDON' =>  'United Kingdom',
          'VIETNAM' => 'Viet Nam',
          'WALES, G.B.' =>  'United Kingdom',
          'YUGOSLAVIA' => 'Yugoslavia (former)',
          'YUGOSLOVIA' =>  'Yugoslavia (former)',
        };

if ( $rating_sigma_only ) {
	$prevents .= ":all_inserts:chapter:country:country_update:no_country:new_rating_date:state:";
}

#--------------------------------------------------------------------------------

parse_args(); 	# $input_file, $reject_file, $input_type, and flags

my $dbh       = getdbiconnection();

get_player_pins($dbh, \%Player_Pins); # printf "Player_Pins: %d\n", scalar keys %Player_Pins;
get_chapter_codes($dbh, \%Chapters);
get_state_codes($dbh, \%States);
get_country_codes($dbh, \%Countries);

if ( $input_type eq "tde" ) {
	parse_tde_file($input_file);
} elsif ( $input_type eq "tda" ) {
	parse_tda_file($input_file);
} else {
	print qq{Can't parse files of type "$input_type"\n};
	$dbh->disconnect();
	exit;
}

silly_country_codes(\%Players);

canada_not_part_of_United_States(\%Players);

# fix_persistent_brokenness(\%Players);

reject_bad_tde_data(\%Players);
split_nations_from_states(\%Players);
set_US_if_state(\%Players);
fix_names(\%Players);

if (not $shoot_blanks) {
	open REJECT, ">", $reject_file or 
		die "$0: Failed to open REJECT file: \"$reject_file\": $!\n";
}

process_each_player($dbh, \%Players);

close REJECT;
$dbh->disconnect();


printf "Player_Pins: %d\n", scalar keys %Player_Pins;

print "\n";
print "Old Data:  $old_data, New Data: $new_data, Broken Data: $broken_data (see $reject_file)\n";
print "Country:              None: $no_country: Updated: $country_update\n";
print "State:                None: $no_state: Updated: $state_update\n";
print "Chapter:                    $chapter\n";
print "Updated Ratings:            $rating_update, New Ratings: $rating_new\n";
print "Updated Sigmas:             $sigma_update, New Sigmas: $sigma_new\n";
print "Updated Dates:              $date_update, New Dates: $date_new\n";
print "Updated Memberships:  Type: $membership_type Exp: $membership_exp\n";

print "\nUnknown States (and number occurances): ". Dumper(\%other);
exit;

#--------------------------------------------------------------------------------

# Can't get these fixed despite repeated requests over more than one year
#
sub fix_persistent_brokenness($) {
	my $Players = shift;
	delete $Players->{10115};
	$Players->{28}{'country'} = 'CA';			# Phil Waldron
	$Players->{272}{'first_name'} = 'Edward Alvin';
	$Players->{638}{'country'} = 'UK';
	$Players->{1350}{'first_name'} = 'John J.';
	$Players->{1632}{'first_name'} = 'Myron P.';
	$Players->{2004}{'first_name'} = 'William E.';
	$Players->{3293}{'last_name'} = 'Queston';
	$Players->{3293}{'first_name'} = 'David';
	$Players->{3524}{'first_name'} = 'James';
	$Players->{3584}{'first_name'} = 'Charles';
	$Players->{3955}{'first_name'} = 'Timothy';
	$Players->{4194}{'first_name'} = 'Jie (Brony)';
	$Players->{4246}{'first_name'} = 'Ming (Michael)';
	$Players->{4679}{'first_name'} = 'Charles J.';
	$Players->{5027}{'last_name'} = 'Bonvaouloir';
	$Players->{5385}{'first_name'} = 'Lawrence';
	$Players->{5591}{'first_name'} = 'Robert James';
	$Players->{6441}{'chapter'} = 'MHGA';
	$Players->{6539}{'country'} = 'CA';			# Jean Waldron
	$Players->{6919}{'first_name'} = 'Wei-Wei';
	$Players->{9585}{'last_name'} = 'Kanamaru';
	$Players->{9791}{'chapter'} = 'TUCS';
	$Players->{10567}{'last_name'} = 'Cape';
	$Players->{10620}{'last_name'} = 'Hill';
	$Players->{10620}{'state'} = 'IL';
	$Players->{11343}{'first_name'} = 'David N.';
#	$Players->{11500}{'mexp'} = '2009-10-10';
	$Players->{11744}{'last_name'} = 'Swanson';
	$Players->{11744}{'first_name'} = 'Paul';
	$Players->{11969}{'last_name'} = 'Yang';
	$Players->{12248}{'last_name'} = 'Eubank II';
	$Players->{12590}{'first_name'} = 'Feijun (Frank)';
	$Players->{13768}{'first_name'} = 'Peter' ;
	$Players->{14207}{'first_name'} = "Eric";
	$Players->{14569}{'first_name'} = "Daniel (Dae Hyuk)";
	$Players->{14669}{'last_name'} = 'Kiguchi';
	$Players->{14837}{'first_name'} = 'Lisa';
	$Players->{15506}{'last_name'} = 'Clifford';
	$Players->{15514}{'last_name'} = 'Gualtier';
	$Players->{15514}{'first_name'} = 'Michael';
	$Players->{15968}{'last_name'} = 'Towfiq';
	$Players->{15921}{'first_name'} = 'Christopher';
	$Players->{16838}{'first_name'} = 'Lie (Norton)';
	$Players->{17055}{'first_name'} = 'Jonathan Q.';
	$Players->{18058}{'country'} = 'AE';
	$Players->{18058}{'state'} = '--';
	$Players->{18247}{'last_name'} = 'Nishida';
	$Players->{18281}{'last_name'} = 'Akanqi';
	$Players->{18297}{'first_name'} = 'Michie-O';
	$Players->{18298}{'first_name'} = 'Mitsuko-O';
	$Players->{18299}{'first_name'} = 'Ayaka-O';
	$Players->{18452}{'first_name'} = 'Qucheng (Roger)';
	$Players->{18588}{'last_name'} = 'Cheung';
	$Players->{19241}{'last_name'} = 'Tremblay';
	$Players->{19241}{'first_name'} = 'Pascal';
	$Players->{19242}{'last_name'} = 'Guennap';
	$Players->{19242}{'first_name'} = 'Chaz';
	$Players->{19243}{'last_name'} = 'Guo';
	$Players->{19243}{'first_name'} = 'Derek';
	$Players->{19244}{'last_name'} = 'Moriguchi';
	$Players->{19244}{'first_name'} = 'Chikashi';
	$Players->{19515}{'last_name'} = "Wang";
	$Players->{19515}{'first_name'} = "Funing";
	$Players->{19807}{'last_name'} = "Ford";
	$Players->{19807}{'first_name'} = "Mark";
	$Players->{19812}{'last_name'} = "Russel";
	$Players->{19939}{'first_name'} = "Jiansheng (Jason)";
	$Players->{20008}{'first_name'} = "Edward James";
#	$Players->{20045}{'last_name'} = "Ozawa";
	$Players->{20053}{'last_name'} = "Zensius";
	$Players->{20053}{'first_name'} = "Peter";
	$Players->{20288}{'first_name'} = 'Aaron';
	$Players->{20580}{'first_name'} = "Wendy";
	$Players->{20261}{'last_name'} = "Barlow";
	$Players->{20261}{'first_name'} = "Jonathan";
	$Players->{20864}{'first_name'} = "Austin";
	$Players->{20897}{'first_name'} = "Andrew";
	$Players->{21040}{'first_name'} = "Yen-Chen (Joshua)";
	$Players->{21043}{'last_name'} = "Gao";
	$Players->{21043}{'first_name'} = "Rui Lu";
	$Players->{21044}{'last_name'} = "Gao";
	$Players->{21044}{'first_name'} = "Wu Shen";
	$Players->{21045}{'last_name'} = "Dong";
	$Players->{21045}{'first_name'} = "Zhe Ke";
	$Players->{21080}{'first_name'} = "Audrey J.";
	$Players->{21081}{'first_name'} = "Lauren S.";
	$Players->{21082}{'first_name'} = "Simon J.";
#	$Players->{21115}{'last_name'} = "Ervin";
        $Players->{21139}{'first_name'} = "Kevin";
	$Players->{21176}{'last_name'} = "Jhong";
	$Players->{21466}{'last_name'} = "Suprada";
	$Players->{21466}{'first_name'} = "G N";
	$Players->{21490}{'last_name'} = "Nicolas";
	$Players->{22186}{'last_name'} = "Chen";
	$Players->{22186}{'first_name'} = "Ching-Tso";
	$Players->{22279}{'last_name'} = "Conniff";
	$Players->{22279}{'first_name'} = "Leo";
}

sub canada_not_part_of_United_States($) {
	my $Players = shift;
	foreach my $id (keys %{$Players} ) {
		if ($Players->{$id}->{'state'} eq "AB" or	# Alberta
		    $Players->{$id}->{'state'} eq "BC" or	# British Columbia
		    $Players->{$id}->{'state'} eq "MB" or	# Manitoba
		    $Players->{$id}->{'state'} eq "NB" or	# New Brunswick
		    $Players->{$id}->{'state'} eq "NL" or	# Newfoundland and Labrador
		    $Players->{$id}->{'state'} eq "NS" or	# Nova Scotia
		    $Players->{$id}->{'state'} eq "NT" or	# Northwest Territories
		    $Players->{$id}->{'state'} eq "NU" or	# Nunavut
		    $Players->{$id}->{'state'} eq "ON" or	# Ontario
		    $Players->{$id}->{'state'} eq "PE" or	# Prince Edward Island
		    $Players->{$id}->{'state'} eq "QC" or	# Quebec
		    $Players->{$id}->{'state'} eq "SK" or	# Saskatchewan
		    $Players->{$id}->{'state'} eq "YT" ) {	# Yukon
			print "$Players->{$id}->{'state'} is not one of the United States, Sam.\n";	
			$Players->{$id}->{'country'} = "CA";
			$Players->{$id}->{'state'} = "--";
		}
	}
}

# get a connection to the database so that we can query for the data needed
#
sub getdbiconnection() {
     # DBI connection information
     my $dsn = "DBI:mysql:dbname:localhost";
     my $user = "...";
     my $password = "...";

     my $dbh = DBI->connect($dsn, $user, $password, { RaiseError => 1 });
#     my $dbh = DBI->connect($dsn, $user, "", { RaiseError => 1 });
}

sub get_player_pins($$) {
     my $dbh = shift;
     my $Player_Pins = shift;

     my $query = qq{ SELECT Pin_Player FROM players };
     my $sth = $dbh->prepare($query) || die "$DBI::errstr";

     $sth->execute() || die "$DBI::errstr";
     while (my @fetch = $sth->fetchrow_array()) {
            $Player_Pins->{$fetch[0]} = 1;
     }
}

sub get_chapter_codes($$) {
     my $dbh = shift;
     my $Chapters = shift;
     my $query = qq{ SELECT Chapter_Code FROM chapter };
     my $sth = $dbh->prepare($query) || die "$DBI::errstr";

     $sth->execute() || die "$DBI::errstr";
     while (my @fetch = $sth->fetchrow_array()) {
            $Chapters->{$fetch[0]} = 1;
     }
}

sub get_country_codes($$) {
     my $dbh = shift;
     my $Countries = shift;
     my $query = qq{ SELECT Country_Code, Country_Descr FROM country };
     my $sth = $dbh->prepare($query) || die "$DBI::errstr";

     $sth->execute() || die "$DBI::errstr";
     while (my @fetch = $sth->fetchrow_array()) {
            $Countries->{uc($fetch[1])} = $fetch[0];
     }
     if ($verbose =~ m/:get_country_codes:/) {
	print Dumper($Countries);
     }
}

sub get_state_codes($$) {
     my $dbh = shift;
     my $States = shift;
     my $query = qq{ SELECT State_Code FROM state };
     my $sth = $dbh->prepare($query) || die "$DBI::errstr";

     $sth->execute() || die "$DBI::errstr";
     while (my @fetch = $sth->fetchrow_array()) {
            $States->{$fetch[0]} = 1;
     }
}

sub parse_date($$) {
	my $id = shift;
	my $date = shift;
	my ($month, $day, $year);
	if ( $date =~ m:\d{1,2}/\d{1,2}/\d{4}: ) {
		($month, $day, $year) = split(/\//, $date);
	}
	elsif ( $date =~ m:\d{1,2}-\d{1,2}-\d{4}: ) {
		($month, $day, $year) = split(/-/, $date);
	}
	elsif ( $date =~ m:\d{4}-\d{1,2}-\d{1,2}: ) {
		($year, $month, $day) = split(/-/, $date);
	}
	if ( $month > 12 ) {
		my $tmp = $month;
		$month = $day;
		$day = $tmp;
	}
	$date = sprintf "%4d-%02d-%02d", $year, $month, $day;
	return $date;	
}

sub parse_mtype($$) {
	my $id = shift;
	my $mtype = shift;
	$Players{$id}{mtype} = $mtype;
	if ($mtype =~ m/Forgn/i) {
		$Players{$id}{foreign} = 1;
	}
	if ($mtype ne "Comp" and
		$mtype ne "Donar" and
		$mtype ne "Forgn" and
		$mtype ne "Full" and 
		$mtype ne "Life" and 
		$mtype ne "Limit" and 
		$mtype ne "Non" and
		$mtype ne "Spons" and 
		$mtype ne "Sust" and 
		$mtype ne "Youth") {
		print "BROKEN MType: $%mtype\n";
		exit;
	}
}

sub parse_name($$) {
	my $id = shift;
	my $name = shift;

	my @etc;

	$name =~ s/"//g;
	$name =~ s/,\s*/,/;

	my ($last_name, $first_name) = split (/,/, $name);
		# ($first_name, @etc) = split (/ /, $first_name);
	$first_name =~ s/\[/(/g;
	$first_name =~ s/]/)/g;
	$first_name =~ s/^\s//g;
	$first_name =~ s/\s$//g;
	$first_name = ucfirst(lc($first_name)) if ($first_name !~ m/.*[ -\/].*/
		and $first_name !~ m/\./
		and $first_name != "JL");
		# $first_name = join ' ', $first_name, @etc;
	$Players{$id}{first_name} = $first_name;
		# ($last_name, @etc) = split (/ /, $last_name);
	$last_name = ucfirst(lc($last_name)) if ($last_name !~ m/.*[ -\/].*/ 
		and $last_name !~ m/^D\'[A-Z]/i
		and $last_name !~ m/^Di[A-Z]/i
		and $last_name !~ m/^d[eu][A-Z]/i
		and $last_name !~ m/^l[aeo][A-Z]/i
		and $last_name !~ m/^M[a]{0,1}c/
		and $last_name !~ m/^O\'/
		and $last_name !~ m/^Ou[A-Z]/
		and $last_name !~ m/^Van[A-Z]/
		and $last_name !~ m/^Vande[A-Z]/
		and $last_name !~ m/^NEMESIS$/
		and $last_name !~ m/^OFFILIB$/);
	$last_name =~ s/^\s*//g;
	$last_name =~ s/\s*$//g;
		# $last_name = join ' ', $last_name, @etc;
	$Players{$id}{last_name} = $last_name;
	return ($first_name, $last_name);
}

# Aal, Zachary	15648	Full	-20.05711	2008-7-2	LIGC	NY	1.09190	2007-8-1
# Aal, Zachary	15648	Full	-20.05712	7/2/2008	LIGC	NY	1.091908	8/1/2007
# Aal, Zachary	15648	Full	-20.05712	7/2/2008	LIGC	NY	1.091908	8/1/2007
# Aarhus, Bob	9616	Full	-21.94348	1/1/2008	NOVA	VA	.873411	11/1/2001
# Aaron, William C.	7206	Full		12/28/1994		CA		
sub parse_tda_file($) {
	my $keep_tabs = 1;
	my $f = shift;
	my $id;

	open TDA, "<", $f or die "$0: Failed to open TDA file: \"$f\": $!\n";
	while (my $line = <TDA>) {
		next if ($line =~ m/^\s*$/);
		next if ($line =~ m/^\s*#/);
		$line = prep_line($line, $keep_tabs);
		my ($name, $id, $mtype, $rating, $mexp, $chapter, $state, $sigma, $date) = split /\t/, $line;

		my( $first_name, $last_name) = parse_name($id, $name);

		if ( $mexp ne "" ) {
			$mexp = parse_date($id, $mexp);
			$Players{$id}{mexp} = $mexp;
		}
		$Players{$id}{mtype} = $mtype;

		if ( $date ne "" ) {
			$date = parse_date($id, $date);
			$Players{$id}{date} = $date;
		}

		$Players{$id}{rating} = $rating if ( $rating ne "" );
		$Players{$id}{sigma} = $sigma if ( $sigma ne "" );

		if ( $chapter ne "" ) {
			$Players{$id}{chapter} = uc($chapter);
		}
		else {
			$Players{$id}{chapter} = "none";
		}
		$Players{$id}{state} = uc($state) if ( $state ne "" );

		if ( $verbose =~ m/:parse_tda_file:/ ) {
			print "parse_tda_file()\n";
			print "\tLast Name:   $Players{$id}{last_name}\n";
			print "\tFirst Name:  $Players{$id}{first_name}\n";
			print "\tId:          $id\n";
			print "\tRating:      $Players{$id}{rating}\n";
			print "\tSigma:       $Players{$id}{sigma}\n";
			print "\tRating Date: $Players{$id}{date}\n";
			print "\tType:        $Players{$id}{mtype}\n";
			print "\tMember Date: $Players{$id}{mexp}\n";
			print "\tState:       $Players{$id}{state}\n";
			print "\tChapter:     $Players{$id}{chapter}\n";
			print "\n";
		}
	}
}

# 5494
# 	Name="Waldron, Derek"
# 	Rating="2.10000"
#	Date="8/15/1993"
#	MType="Forgn"
#	MExp="12/28/1979"
#	State="CANADA"
#6539
#	Name="Waldron, Jean"
#	Rating="-6.58104"
#	Sigma="0.26400"
#	Date="9/1/2008"
#	MType="Forgn"
#	MExp="8/18/2004"
#	Chapter="GFY"
#
# Populate %Players with data from the TDE file
sub parse_tde_file($) {
	my $keep_tabs = 0;
	my $f = shift;
	my $id;

	open TDE, "<", $f or die "$0: Failed to open TDE file: \"$f\": $!\n";

	while (my $line = <TDE>) {
		$line = prep_line($line, $keep_tabs);
		# 7791
		if ($line =~ m/\s*#/) {
			next;
		}
		elsif ($line =~ m/^(\d+)$/) {
			$id = $line;
		} 
		# Name="Suzuki, Fumio"
		elsif ($line =~ s/\s*Name\s*=\s*(.*)/$1/) {
			parse_name($id, $line);
			my ($last_name, $first_name) = split (/,/, $line);
		}
		# Rating="2.63779"
		elsif ($line =~ s/\s*Rating\s*=\s*(.*)/$1/) {
			$line =~ s/"//g;
			$Players{$id}{rating} = $line;
		}
		# Sigma="0.32343"
		elsif ($line =~ s/\s*Sigma\s*=\s*(.*)/$1/) {
			$line =~ s/"//g;
			$Players{$id}{sigma} = $line;
		}
		# Date="9/1/2009"
		elsif ($line =~ s/\s*Date\s*=\s*(.*)/$1/) {
			$line =~ s/"//g;
			$Players{$id}{date} = parse_date($id, $line);
		}
		# Chapter="CINC"
		elsif ($line =~ s/\s*Chapter\s*=\s*(.*)/$1/) {
			$line =~ s/"//g;
			$Players{$id}{chapter} = uc($line);
		}
       		# State="ENGLAND"
		elsif ($line =~ s/\s*State\s*=\s*(.*)/$1/) {
			$line =~ s/"//g;
			$Players{$id}{state} = uc($line);
		}
		# MType="Forgn"
#		elsif ($line =~ m/\s*MType\s*=\s*"Forgn"/i) 
		elsif ($line =~ s/\s*MType\s*=\s*(.*)/$1/) {
			$line =~ s/"//g;
			parse_mtype($id, $line);

		}
		# MExp="8/28/1995"
		elsif ($line =~ s/\s*MExp\s*=\s*(.*)/$1/) {
			$line =~ s/"//g;
			$Players{$id}{mexp} = parse_date($id, $line);
		}
	}
	if ($verbose =~ m/:parse_tde_file:/) {
		print "parse_tde_file(): ", Dumper(\%Players);
	}
#	print Dumper($Players{11766});
	
	close TDE;
}

sub prep_line($$) {
	my $line = shift;
	my $keep_tabs = shift;
	chomp $line;
	$line =~ s/\t/0x00/g if ( $keep_tabs );
#	$line =~ s/[ ]+/ /g;
#	$line =~ s/[	]+/ /g;
	$line =~ s/^\s+//;
	$line =~ s/\s+$//;
	$line =~ s/\s+/ /g;
	$line =~ s///;
	$line =~ s/0x00/\t/g if ( $keep_tabs );
	return ($line);
}

sub fix_names($) {
	my $Players = shift;
#print "fix_names(10115): (entry):", Dumper($Players->{10115}), "\n";
#	$Players->{10115}{last_name} = "Guo"; $Players->{10115}{first_name} = "Wangzi";
#	$Players->{11969}{last_name} = "Yang"; $Players->{11969}{first_name} = "Huan";
#	$Players->{12248}{last_name} = "Eubank II";
#	$Players->{15726}{first_name} = "Yun Xuan";
#	$Players->{17618}{last_name} = "Pippin"; $Players->{17618}{first_name} = "Wallace";
#print "fix_names(10115): (exit):", Dumper($Players->{10115}), "\n";
}

sub silly_country_codes($) {
	my $Players = shift;
	foreach my $id (keys %{$Players}) {
		print "silly_country_codes($id): (entry)", Dumper($Players->{$id}) if ($verbose =~ m/:silly_country_codes:/);
		if ( (exists($Players->{$id}{state}) and exists($SillyCountryFixes->{uc($Players->{$id}{state})}) ) ) {
			my $state = $SillyCountryFixes->{uc($Players->{$id}{state})};
			if ($verbose =~ m/:foreign_states:/) { 
				print "Foreign State ($id): $Players->{$id}{state} $Countries{uc($Players->{$id}{state})} -> $state -> $Countries{uc($state)}\n";
			};
			$Players->{$id}{state} = $state;
		}
		print "silly_country_codes($id): (exit)", Dumper($Players->{$id}) if ($verbose =~ m/:silly_country_codes:/);
	}
}

sub reject_bad_tde_data($) {
	my $Players = shift;
	foreach my $id (keys %Players) {
		exit if ( bad_tde_data($id) );
	}
}

sub bad_tde_data() {
         my $id = shift;
         my $insert = shift;
         my $fields = shift;
         my $values = shift;

         # Gotta have an AGAID, a first name and a last name
	 if ( not exists($Players{$id}{first_name}) and not $rating_sigma_only ) {
		$broken_data++;
                print "bad_tde_data($id): Broken Data (first_name): $id $insert $fields ) $values )\n", Dumper($Players{$id}), "\n" if ($verbose =~ m/:broken_data:/);
		print REJECT "Broken Data (first_name): $insert $fields ) $values )\n";
                return -1; 
         }

	 if ( 0 and $id != 15921 and $id != 3955 and not exists($Players{$id}{last_name}) and not $rating_sigma_only ) {
print "Broken Data (last_name): $id: ", Dumper($Players{$id}), "\n";
		$broken_data++;
                print "bad_tde_data($id): Broken Data (last_name): $id $insert $fields ) $values )\n", Dumper($Players{$id}), "\n" if ($verbose =~ m/:broken_data:/);
		print REJECT "Broken Data (last_name): $insert $fields ) $values )\n";
                return -1; 
         }

         # if a chapter is listed, it must exist
	 if ( (exists($Players{$id}{chapter}) 
		and not exists($Chapters{$Players{$id}{chapter}})) 
		and not $rating_sigma_only ) {
		$broken_data++;
                print "bad_tde_data($id): Broken Data: (chapter) $insert $fields ) $values )\n", Dumper($Players{$id}), "\n" if ($verbose =~ m/:broken_data:/);
		print REJECT "Broken Data: (chapter): $id $insert $fields ) $values )\n";
                return -1; 
         }

         return 0;
}

sub split_nations_from_states($) {
	my $Players = shift;
	foreach my $id (keys %{$Players}) {
		print "split_nations_from_states($id): ", Dumper($Players->{$id}) if ($verbose =~ m/:split_nations_from_states:/);
		# a country we can recognize
		if ( (exists($Players->{$id}{state}) and exists($Countries{uc($Players->{$id}{state})}) ) ) {
			$Players->{$id}{country} = $Countries{uc($Players{$id}{state})};	# FIX bad data
			$Players->{$id}{foreign} = 1;
			delete $Players{$id}{state};
			if ($verbose =~ m/:foreign_states:/) { 
				print "Foreign State ($id): $Players->{$id}{state} -> $Countries{uc($Players->{$id}{state})}\n";
			};
		}
		print "split_nations_from_states($id): ", Dumper($Players->{$id}) if ($verbose =~ m/:split_nations_from_states:/);
	}
}

sub set_US_if_state($) {
	my $Players = shift;
	foreach my $id (keys %{$Players}) {
		# one of the fifty states
		print "set_US_if_state($id) (entry): ", Dumper($Players->{$id}) if ($verbose =~ m/:set_US_if_state:/);
		if ( (exists($Players->{$id}{state}) and exists($States{$Players->{$id}{state}}) ) ) {
			$Players->{$id}{country} = 'US';
		}
		# state is optional....AGA data on state missing for hundreds of players
		# if a state is listed, it must be a country or one of the fifty states
		elsif ( not exists($Players->{$id}{state}) ) {
			# permitted state				# FIX in future
		}
		# all other values for states
		else {
			$broken_data++;
			$other{$Players->{$id}{state}}++;
			if ($verbose =~ m/:unknown_states:/ ) { print "Unknown State: ", Dumper($Players->{$id}), "\n"; } ;
			return -1;
		}
		print "set_US_if_state($id) (exit): ", Dumper($Players->{$id}) if ($verbose =~ m/:set_US_if_state:/);
	}
}

# process data from the TDE file
sub process_each_player($$) {
	my $dbh = shift;
	my $Players = shift;

	foreach my $id (sort { $a <=> $b } keys %Players) {

		my $query = qq{ SELECT Pin_Player, Last_Name, Name, Club, Rating, Sigma, State_Code, Country_Code, MType, MExp, Elab_Date FROM players WHERE Pin_Player = $id };
		my $sth = $dbh->prepare($query) || die "$DBI::errstr";
		# print "\nQuery: $query\n";
		$sth->execute() || die "$DBI::errstr";
		my @fetch = $sth->fetchrow_array();

		# Update Existing AGAGD Records
		if ($#fetch > -1) {
			update_database($dbh, $Players, $id, \@fetch);
		}

		# Create New AGAGD Records
		else {
			insert_database($dbh, $Players, $id);
		}
		undef $Player_Pins{$id};
	}
}


sub update_database($$$@) {
	my $dbh = shift;
	my $Players = shift;
	my $id = shift;
	my $fetch = shift;

	print "update_database($id): ", Dumper($Players->{$id}) if ($verbose =~ m/:database_records:/);
	$old_data++;
        my $update = qq{ UPDATE players SET };
        my $fields = qq{};
        my $where  = qq{ WHERE Pin_Player = $id };
# check database for Id
# 	if found and names match, update rating, sigma, last_appearance, etc.
#	if found and names dont match, write out error
#	if not found, create new record in database
        my $pin	= $fetch->[0];
        my $last_name	= $fetch->[1];
        my $name	= $fetch->[2];
        my $club	= $fetch->[3];
        my $rating	= $fetch->[4];
        my $sigma	= $fetch->[5];
        my $state	= $fetch->[6];
        my $country	= $fetch->[7];
	my $mtype	= $fetch->[8];
	my $mexp	= $fetch->[9];
	my $date	= $fetch->[10];
        print qq{Current Database:\n\tPin: $pin\tLast Name: $last_name Name: $name Club: $club MType: $mtype Mexp: $mexp\n\tRating: $rating Sigma: $sigma Date: $date\n\tCountry: $country State: $state\n} if ($verbose =~ m/:database_records:/);

	# Add some Sanity
	if ( $Players{$id}{state} eq "US" ) {
		delete $Players{$id}{state};
		$Players{$id}{country} = "US";
	}
	if ( exists $States{$Players{$id}{state}} and $Players{$id}{country} ne "US") {
#		print Dumper($Players{$id}), "\n";
#		print qq{$id: State is in the US but country is not set to US\n};
		$Players{$id}{country} = "US";
	}
	if ( $country eq "--" and not exists $Players{$id}{country} ) {
#		print Dumper($Players{$id}), "\n";
#		print qq{$id: Setting Country to "--" in place of no country\n};
		$Players{$id}{country} = "--";
	}
	if ( not exists($Players{$id}{state}) ) {
		$Players{$id}{state} = "--";
	}
	# Sanity Checking
        if ( lc($last_name) ne lc($Players{$id}{last_name}) and not $rating_sigma_only ) {
		print Dumper($Players{$id}), "\n";
# 20160310		die qq{$id: Last Name of player changed: "$last_name" vs "$Players{$id}{last_name}"\n};
        }
        if ( lc($name) ne lc($Players{$id}{first_name}) and not $rating_sigma_only ) {
		print Dumper($Players{$id}), "\n";
# 20160310		die qq{$id: First Name of player changed: "$name" vs "$Players{$id}{first_name}"\n};
        }
        if ( $club ne "none" and $club ne $Players{$id}{chapter} and not $rating_sigma_only ) {
#		print Dumper($Players{$id}), "\n";
		print qq{$id: Chapter of player changed: "$club" vs "$Players{$id}{chapter}"\n} if ($verbose =~ m/:chapters:/);
#		die qq{$id: Chapter of player changed: "$club" vs "$Players{$id}{chapter}"\n};
        }
#        if ( $country ne "US" and $country ne $Players{$id}{country} and not $rating_sigma_only ) 
        if ( $country ne "" and $country ne $Players{$id}{country} ) {
#		print Dumper($Players{$id}), "\n";
#		print qq{$id: Country of player changed: "$country" vs "$Players{$id}{country}"\n};
#		die qq{$id: Country of player changed: "$country" vs "$Players{$id}{country}"\n};
        }
        elsif ( $country eq "US" and $state ne "none" and $state ne $Players{$id}{state} and not $rating_sigma_only ) {
#		print Dumper($Players{$id}), "\n";
		print qq{$id: State of player changed: "$state" vs "$Players{$id}{state}"\n}
			if ( $ verbose =~ m/:state_change:/);
#		die qq{$id: State of player changed: "$state" vs "$Players{$id}{state}"\n};
        }

	# Prepare to update database for Chapter / Club
        if ( $prevents !~ m/:chapter:/ ) {
		if ($club eq $Players{$id}{chapter}) {
			;
		}
		elsif ($club eq "none" and $Players{$id}{chapter} eq "") {
			;
		}
		elsif ($club ne $Players{$id}{chapter}) {
			$chapter++;
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ Club = '$Players{$id}{chapter}' };
		}
        }

	# Prepare to update database for MType and Mexp
        if (exists $Players{$id}{mtype} and $prevents !~ m/:membership_type:/) {
		if ($mtype ne $Players{$id}{mtype}) {
			$membership_type++;
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ MType = '$Players{$id}{mtype}' };
		}
		elsif ($mtype eq $Players{$id}{mtype}) {
			;
		}
        }
        if (exists $Players{$id}{mexp} and $prevents !~ m/:membership_exp:/) {
		if ($mexp ne $Players{$id}{mexp}) {
			$membership_exp++;
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ MExp = '$Players{$id}{mexp}' };
		}
		elsif ($mexp eq $Players{$id}{mexp}) {
			;
		}
        }

	# Prepare to update database for Country, Rating, Sigma, Elab_Date
        if ( $prevents !~ m/:country:/) {
		if ($country eq $Players{$id}{country}) {
			;
		}
		elsif ($country ne $Players{$id}{country} and $prevents !~ m/:country_update:/) {
			$country_update++;
			$fields .= qq{ , } if ($fields ne qq{});
			if ( exists $Players{$id}{country} ) {
#				$fields .= qq{ Country_Code = '$Players{$id}{country}', State_Code = NULL };
				$fields .= qq{ Country_Code = '$Players{$id}{country}' };
			}
			else {
				$fields .= qq{ Country_Code = '--' };
			}
		}
		elsif ($prevents !~ m/:no_country:/) {
			$no_country++;
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ Country_Code = NULL, State_Code = NULL };
		}
        }
        if ( $prevents !~ m/:state:/) {
		if ($state eq $Players{$id}{state}) {
			;
		}
		elsif ($state ne $Players{$id}{state} and $prevents !~ m/:state:/) {
			$state_update++;
			$fields .= qq{ , } if ($fields ne qq{});
			if ( exists $Players{$id}{state} ) {
				$fields .= qq{ State_Code = '$Players{$id}{state}' };
			} 
			else {
# 20100304				$fields .= qq{ State_Code = NULL };
				$fields .= qq{ State_Code = "--" };
			}
		}
		elsif ($prevents !~ m/:no_state:/) {
			$no_state++;
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ Country_Code = NULL, State_Code = NULL };
		}
        }
#
#1: Newly rated player: "" vs "0.00000"
#1: Newly assigned sigma: "" vs "0.00000"
# UPDATE players SET  MType = 'Full'  ,  MExp = '1980-12-28'  ,  Rating = "0.00000"  ,  Sigma = "0.00000"  WHERE Pin_Player = 1
#
	if ( exists($Players{$id}{rating}) ) {
         	if ( $rating eq "" and $Players{$id}{rating} ne "0.00000" and $prevents !~ m/:new_rating_sigma:/) {
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ Rating = "$Players{$id}{rating}" };
			$rating_new++;
			print qq{$id: Newly rated player: "$rating" vs "$Players{$id}{rating}"\n}
				if ($verbose =~ m/:rating_sigma:/);
			# print Dumper($Players{$id}), "\n";
		}
		elsif ( $rating != $Players{$id}{rating} and $Players{$id}{rating} ne "0.00000" 
			and ( abs($rating - $Players{$id}{rating}) > 0.00001 )
			and $prevents !~ m/:update_rating_sigma:/) {
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ Rating = "$Players{$id}{rating}" };
			$rating_update++;
			print qq{$id: Updated Rating of player: "$rating" vs "$Players{$id}{rating}"\n}
				if ($verbose =~ m/:rating_sigma:/);
			# print Dumper($Players{$id}), "\n";
		}
	}
	if ( exists($Players{$id}{sigma}) ) {
         	if ( $sigma eq "" and $Players{$id}{sigma} ne "0.00000" and $prevents !~ m/:new_rating_sigma:/) {
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ Sigma = "$Players{$id}{sigma}" };
			$sigma_new++;
			print qq{$id: Newly assigned sigma: "$sigma" vs "$Players{$id}{sigma}"\n}
				if ($verbose =~ m/:rating_sigma:/);
			# print Dumper($Players{$id}), "\n";
		}
		elsif ( $sigma != $Players{$id}{sigma} and $Players{$id}{sigma} ne "0.00000"  
			and ( abs($sigma - $Players{$id}{sigma}) > 0.00001 )
			and $prevents !~ m/:update_rating_sigma:/) {
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ Sigma = "$Players{$id}{sigma}" };
			$sigma_update++;
			print qq{$id: Updated Sigma of player: "$sigma" vs "$Players{$id}{sigma}"\n}
				if ($verbose =~ m/:rating_sigma:/);
			# print Dumper($Players{$id}), "\n";
		}
	}
	if ( exists($Players{$id}{date}) ) {
         	if ( $date eq ""   and $prevents !~ m/:new_rating_date:/) {
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ Elab_Date = "$Players{$id}{date}" };
			$date_new++;
			print qq{$id: Newly assigned date: "$date" vs "$Players{$id}{date}"\n}
				if ($verbose =~ m/:rating_date:/);
			# print Dumper($Players{$id}), "\n";
		}
		elsif ( $date ne $Players{$id}{date} 
			and $Players{$id}{date} ne "2099-12-31"				# WTF Paul 
			and $prevents !~ m/:update_rating_sigma:/) {
			$fields .= qq{ , } if ($fields ne qq{});
			$fields .= qq{ Elab_Date = "$Players{$id}{date}" };
			$date_update++;
			print qq{$id: Updated Date of player: "$date" vs "$Players{$id}{date}"\n}
				if ($verbose =~ m/:rating_sigma:/);
			# print Dumper($Players{$id}), "\n";
		}
	}
	my $update = $update . $fields . $where;

# XXX
# XXX   Justin's new code is wiping out country codes				# 20110724
# XXX
if ($update =~ m/UPDATE players SET  Country_Code = '--'  WHERE Pin_Player = /) {
	print qq{Skipping deletion of $country and replacement with "$Players{$id}{country}" for player $id\n};
}
else {
print "$update;\n" if ( $fields ne qq{} and $verbose =~ m/:updates:/ );		# 20091009
	if ($fields ne "") {
		if (not $shoot_blanks and $prevents !~ m/:all_updates:/) {
			 my $sth = $dbh->prepare($update) || die "$DBI::errstr";
			 $sth->execute() || die "$DBI::errstr";
		}
	}
}
}

sub insert_database($$$) {
	my $dbh  = shift;
	my $Players = shift;
	my $id = shift;
        my $insert = qq{ INSERT into players };
        my $fields = qq{ ( Pin_Player };
        my $values = qq{ VALUES ( $id };

	if ( exists $States{$Players{$id}{state}} and $Players{$id}{country} ne "US") {
#		print Dumper($Players{$id}), "\n";
#		print qq{$id: State is in the US but country is not set to US\n};
		$Players{$id}{country} = "US";
	}

        if (exists($Players{$id}{last_name})) { 
		$fields .= qq{, Last_Name}; 
		$values .= qq{, "$Players{$id}{last_name}"}
	};

        if (exists($Players{$id}{first_name})) { 
		$fields .= qq{, Name}; 
		$values .= qq{, "$Players{$id}{first_name}"}
	};

        if (exists($Players{$id}{chapter})) { 
		$fields .= qq{, Club};			# XXX change to Chapter ??
		$values .= qq{, "$Players{$id}{chapter}"}
	}
	else {
		$fields .= qq{, Club};			# XXX change to Chapter ??
		$values .= qq{, "none"};
	};

        if (exists($Players{$id}{date})) { 
		$fields .= qq{, Elab_Date}; 
		$values .= qq{, "$Players{$id}{date}"}
	};

        if (exists($Players{$id}{rating})) { 
		$fields .= qq{, Rating};
		$values .= qq{, "$Players{$id}{rating}"};
	};

        if (exists($Players{$id}{sigma})) { 
		$fields .= qq{, Sigma};
		$values .= qq{, "$Players{$id}{sigma}"};
	};

        if (exists($Players{$id}{state})) {
		$fields .= qq{, State_Code};
		$values .= qq{, "$Players{$id}{state}"};
	};

        if (exists($Players{$id}{country})) {
		$fields .= qq{, Country_Code};
		$values .= qq{, "$Players{$id}{country}"};
	};

	my $insert = $insert . $fields . " ) ". $values . " ) ";

#	 next unless ( (exists($Players{$id}{state}) and (exists($States{$Players{$id}{state}}))) or
#	               (not exists($Players{$id}{first_name})) or
#		       (not exists($Players{$id}{last_name})) ); 
	if ( $prevents !~ m/:all_inserts:/ ) {
		print "$insert ;\n" if ( $verbose =~ m/:inserts:/);
		if (not $shoot_blanks) {
			 my $sth = $dbh->prepare($insert) || die "$DBI::errstr";
			 $sth->execute() || die "$DBI::errstr";
		}
	}
	if (not defined $Player_Pins{$id}) {
		print "$id in Players but not in Player_Pins\n\t", Dumper($Players{$id}), "\n" if ($verbose =~ m/:insert_database:/);
	}
	$new_data++;
}


sub parse_args() {
	$input_file = $ARGV[0];
	@base = fileparse($ARGV[0], qr/\.[^.]*/);
	# foreach my $i (0 .. $#base) { print "$i: $base[$i]\n"; } ;
	( $input_type = $base[2] ) =~ s/^\.(.)/$1/;
	$reject_file = $base[0] . ".rej";

	my $result = GetOptions(
#			"help|h!"	=> \$help,
#			"revert|r!"	=> \$revert,
			"shoot_blanks|n:i" => \$shoot_blanks,
			"verbose:s"	=> \$verbose,
		);
}
