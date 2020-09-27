#!/usr/bin/perl
# !./parse_results.pl port20051023.in
# Jonathan M. Bresler, 2008
#
#

use strict;
use Data::Dumper;
use DBI;
use File::Basename;
use Getopt::Long;

my $MIN_HANDICAP = -1;
my $MAX_HANDICAP = 10;
my $MIN_KOMI = -20;
my $MAX_KOMI = 20;
my $CHECK_DAY = 1;
my $DONT_CHECK_DAY = 0;
my $MAX_ROUNDS = 16;

# if $exactness does not include last_name then TMP11 of 20090103jujoopen.in 
# becomes Joseph A. Berry which is not right.   11 should be 10800 Xiao, Qiang.
# 20090103jujoopen.fix does not exist.
my $exactness = ":agaid:last_name:";	#year:month:"; #":last_name:first_name:day:";
my $allow_player_fetch = 1;		# fetch players's details not listed 
					# in input files from database using id

# Completely processed failed are moved from the input directory to the completed directory
my $processed = "/Processed/";

# Direcotries for bad data files, at the first error,
# the results files is moved to the appropriate directory and processing stops. 
my $daymismatch = "/BadDay/";
my $badkomi = "/BadKomi/";
my $monthmismatch = "/BadMonth/";
my $badresult = "/BadResult/";
my $yearmismatch = "/BadYear/";
my $checkgames = "CheckGames/";
my $checkplayers = "CheckPlayers/";
my $checktournament = "CheckTournament/";
my $duplicateplayer = "/DuplicatePlayer/";
my $duplicatetournament = "/DuplicateTournament/";
my $mismatchednames = "/MismatchedNames/";
my $missingplayer = "/MissingPlayer/";
my $nogames = "/NoGames/";
my $noplayers = "/NoPlayers/";
my $sameplayer = "/SamePlayer/";
my $toofewrounds = "/TooFewRounds/";
my $toomanyrounds = "/TooManyRounds/";

my %IdChanges;	# IdChanges: PinChanges -> Player_Variations
my %Players;
my %PlayersDB;
my %Rejects;
my %TempIDs;
my %Tournament;
my %CrosstabGames;
my @Games;
my $abort = 0;
my $abort_after_parse = 0;
my $fix_file;
my $foo = 0;						# 20100228
my $ignore_all_hashes = 0;	# ugly hack
my $largest_agaid = -1;
my $new_games = 0;
my $online = 0;
my $results_file;
my $revert = 0;
#my $round = 1;	BYE_XXX
my $round = 0;
my $shoot_blanks = 2;
my $sort_by_rating_first = 0;
my $new_tournaments = 0;
my $updated_players = 0;
# my $verbose = ":parse_args:update_tournament:";
#my $verbose = ":parse_fix_players:TempIDs:check_tournament:update_tournament:";

#my $verbose = ":check_players:update_tournament:update_last_update::";
#my $verbose = ":check_tournament:check_games:check_players:record_last_update:Found_Game:Tournament:player_variations:parse_rank_detail:parse_rank:fetch_player:create_games:create_tournament:find_next_round:generate_crosstab:getdbiconnection:parse:parse_args:parse_fix_file:parse_fix_players:parse_header:parse_players:parse_results_file:parse_round:update_players:validate_players:tally_games:idle_players:players_ids_to_agaids:games_ids_to_agaids:parse_name:revert:";
#":Tournament:create_tournament:convert_date:"; # :check_players:";
# ":parse:Unparsedline:Found_Game:Tournament:";
#my $verbose = ":parse_fix_players:Found_Game:Unparsedline:parse_players:idle_players:";
#my $verbose = ":Found_Game:parse_players:";
#my $verbose = ":check_players:Found_Game:parse_players:";
#my $verbose = ":Players:TempIDs:Unparsed:Tournament:";
# ":CrosstabGames:Found_Game:generate_crosstab:Players:Summary:TempIDs:Tournament:";
# :access_and_parse:convert_date:parse_players:parse_header:parse_round:generate_crosstab:";
# :create_games:create_tournament:find_next_round:generate_crosstab:getdbiconnection:parse_args:parse_fix_file:parse_fix_players:parse_header:parse_players:parse_results_file:parse_round:update_players:validate_players:
# my $verbose = ":create_games:";
my $verbose = ":Found_Game:tally_games:";
my $verbose = ":parse:";
my $verbose = ":check_players:";
my $verbose = ":create_tournament:";

########################
# Processing Begins Here 
########################
print "\n\n";

my $dbh = getdbiconnection();
get_id_changes($dbh, \%IdChanges);	# IdChanges: PinChanges -> Player_Variations

my ($fix_file, $results_file) = parse_args_and_obtain_filenames();

my $open_fatal = 0;
access_and_parse($fix_file, $open_fatal);
	print "TempIDs: ", Dumper(\%TempIDs) if ($verbose =~ m/:parse_fix_file:/);

$open_fatal = 1;
access_and_parse($results_file, $open_fatal);
die qq{\nParse of one or more lines of "$results_file" failed} if ($abort_after_parse);

players_ids_to_agaids(\%Players);
games_ids_to_agaids(\@Games);

# print Dumper(\@Games);	20110703
# exit;
if ( not tally_games(\%Players, \@Games, \%CrosstabGames) ) {
	move_file($checkgames);
	die qq{$results_file: $Tournament{name}: no games};
}

foreach my $id (keys %Players) {
	if (not exists ($Players{$id}{score})) {
		warn qq{$results_file: Player '$Players{$id}{last_name}, $Players{$id}{name}' ($id) did not play a single game} if ($verbose =~ /:idle_players:/);
		delete $Players{$id};
	}
}

if ( 0 and not check_tournament($dbh, \%Tournament, scalar(keys %Players), scalar(@Games), $revert) ) {
# if (not check_tournament($dbh, \%Tournament, scalar(keys %Players), scalar(@Games), $revert) ) {
	$dbh->disconnect(); 
	move_file($checktournament);
	die qq{$results_file: "$Tournament{name}": existing rated tournament};
}

$largest_agaid = get_largest_agaid($dbh);

# do the right thing by check_players for $revert
if (not check_players($dbh, \%Players, \%PlayersDB, \%Tournament, $revert) ) { # $Tournament{code}, $revert) ) { 2015-03-29
	$dbh->disconnect();
	move_file($checkplayers);
	die qq{$results_file: $Tournament{name}: missing players};
}

if (not check_games(\%Tournament, \%Players, \@Games, $dbh, \%PlayersDB, $revert) ) {
	$dbh->disconnect();
	move_file($checkgames);
	die qq{$results_file: $Tournament{name}: bad games};
}

foreach my $id (keys %Players) {
	if (not exists ($Players{$id}{score})) {
		warn qq{$results_file: Player '$Players{$id}{last_name}, $Players{$id}{name}' ($id) did not play a single game} if ($verbose =~ /:idle_players:/);
		delete $Players{$id};
	}
}

generate_crosstab(\%Tournament, \%Players, \%CrosstabGames);

if ( $abort ) {
	print qq{\nTournament NOT added to the AGA Go Database\n};
	print qq{\nChanges required before this tournament can be rated and added to the \n};
        print qq{AGA Go Database.\n\n};
	print qq{If you believe that the results file is correct and complete,\n};
	print qq{please email the results file to agagd\@usgo.org\n\n};
	print qq{Thank you.\n\n};
}

if (not commit_tournament($dbh, \%Tournament, \%Players, $revert) ) {
	die qq{$Tournament{name}: commit_tournament failed};
}
else  {
	record_last_update($Tournament{start});
}
$dbh->disconnect();

	print "CrosstabGames: ", Dumper(\%CrosstabGames), "\n" if ($verbose =~ m/:CrosstabGames:/);
	print "TempIDs: ", Dumper(\%TempIDs), "\n" if ($verbose =~ m/:TempIDs:/);
	print "Players: ", Dumper(\%Players), "\n" if ($verbose =~ m/:Players:/);
	print "Tournament: ", Dumper(\%Tournament), "\n" if ($verbose =~ m/:Tournament:/);

if ($verbose =~ m/:Summary:/) {
	printf "File: %20s,     Tournaments: %4d,         Players: %4d,     Games: %4d\n", 
		$results_file, length(%Tournament), length(%Players), scalar(@Games);
	printf "File: %20s, New Tournaments: %4d, Updated Players: %4d, New Games: %4d\n",
		$results_file, $new_tournaments, $updated_players, $new_games;
}

move_file($processed);

exit;

#-------------------------------------------------------------------------------

sub record_last_update($) {
	print "record_last_update()\n" if ($verbose =~ m/:record_last_update:/);;
	my $date = shift;
	my @months = ('zero fill', 'January', 'February', 'March', 'April', 'May', 'June',
		'July', 'August', 'September', 'October', 'November', 'December');

	my $query = qq{ SELECT Update_date FROM sys_reg };
	my $sth = $dbh->prepare($query) || die "record_last_update(): $query\n $DBI::errstr";
	$sth->execute() || die "record_last_update(): $query\n $DBI::errstr";
	my @fetch = $sth->fetchrow_array();
	$sth->finish;
	print "Fetch: ", Dumper(@fetch), "\n" if ($verbose =~ m/:record_last_update:/);
	my ($db_d, $db_m, $db_y) = split / /, $fetch[0];
	for (my $i = 0; $i < $#months; $i++) {
		if ($db_m eq $months[$i]) {
			$db_m = $i;
			last;
		}
	}
	print "$db_y, $db_m, $db_d\n" if ($verbose =~ m/:record_last_update:/);

	my ($t_y, $t_m, $t_d) = split /-/, $date;
	print "$t_y, $t_m, $t_d\n" if ($verbose =~ m/:record_last_update:/);

	if ($t_y > $db_y or
		( $t_y == $db_y and $t_m > $db_m) or
		( $t_y == $db_y and $t_m > $db_m and $t_d > $db_d) ) {

						# my @today = localtime(); 
		my $year = $t_y;		# my $year = 1900 + $today[5]; 
		my $month = @months[$t_m];	# my $month = @months[$today[4]]; 
		my $day = $t_d;			# my $day = $today[3];

		my $update = qq{UPDATE sys_reg set Update_date = '$day $month $year'};
		my $sth = $dbh->prepare($update) || die "$DBI::update";
		print "record_last_update($date): $update\n" 
			if ($verbose =~ m/:record_last_update:/ or
				$verbose =~ m/:update_last_update:/);

		do {
			$sth->execute() || die "record_last_update(): $update\n $DBI::errstr";
		} unless ($shoot_blanks);
	}
}

sub games_ids_to_agaids($) {
	my $Games = shift;
	print Dumper($Games) if ($verbose =~ m/:games_ids_to_agaids:/);
	foreach my $game ( @{$Games} ) {
if ( 0 == 1 ) { # 20100208
		if ($game->{white} =~ m/new/i)   { $game->{white} =~ s/new//i; }
		if ($game->{black} =~ m/new/i)   { $game->{black} =~ s/new//i; }
		if ($game->{white} =~ m/new_/i)  { $game->{white} =~ s/new_//i; }
		if ($game->{black} =~ m/new_/i)  { $game->{black} =~ s/new_//i; }
		if ($game->{white} =~ m/t([0-9]+)/i)   { $game->{white} =~ s/t//i; }
		if ($game->{black} =~ m/t([0-9]+)/i)   { $game->{black} =~ s/t//i; }
		if ($game->{white} =~ m/temp/i)  { $game->{white} =~ s/temp//i; }
		if ($game->{black} =~ m/temp/i)  { $game->{black} =~ s/temp//i; }
		if ($game->{white} =~ m/tmp/i)   { $game->{white} =~ s/tmp//i; }
		if ($game->{black} =~ m/tmp/i)   { $game->{black} =~ s/tmp//i; }
} # 20100208
		if ($game->{white} =~ m/usa/i)   { $game->{white} =~ s/usa//i; }
		if ($game->{black} =~ m/usa/i)   { $game->{black} =~ s/usa//i; }
		$game->{white} = $IdChanges{$game->{white}} if (exists($IdChanges{$game->{white}}));	# IdChanges: PinChanges -> Player_Variations
		$game->{black} = $IdChanges{$game->{black}} if (exists($IdChanges{$game->{black}}));	# IdChanges: PinChanges -> Player_Variations
	}
	print Dumper($Games) if ($verbose =~ m/:games_ids_to_agaids:/);
}

sub players_ids_to_agaids ($) {
	my $Players = shift;
	print "players_ids_to_agaids(): Players: ", Dumper($Players) if ($verbose =~ m/:players_ids_to_agaids:/);
	foreach my $id (keys %{$Players}) {
		print "ID: $id\n" if ($verbose =~ m/:players_ids_to_agaids:/);
		my $agaid = $id;
		print "AGAID numbers only: $id -> $agaid\n" if ($verbose =~ m/:players_ids_to_agaids:/);
if ( 0 == 1 ) { # 20100208
		if ($id =~ /new/i)  { $agaid =~ s/new//i; };
		if ($id =~ m/t([0-9]+)/i)   { $id =~ s/t//i };
		if ($id =~ /temp/i) { $agaid =~ s/temp//i; };
		if ($id =~ /tmp/i)  { $agaid =~ s/tmp//i; };
} # 20100208
		if ($id =~ /usa/i)  { $agaid =~ s/usa//i; }

		$agaid = $IdChanges{$agaid} if (exists($IdChanges{$agaid}));	# IdChanges: PinChanges -> Player_Variations

		if ($agaid != $id) {
			foreach my $key (keys %{$Players->{$id}}) {
			print "$id: $key: $Players->{$id}{$key}\n" if ($verbose =~ m/:players_ids_to_agaids:/);
				$Players->{$agaid}{$key} = $Players->{$id}{$key};
			}
			delete $Players->{$id};
		}
	}
	print "players_ids_to_agaids(): Players: ", Dumper($Players) if ($verbose =~ m/:players_ids_to_agaids:/);
}

# IdChanges: PinChanges -> Player_Variations
sub get_id_changes($$) {
	my $dbh = shift;
	my $IdChanges = shift;

	my $query = qq{ SELECT Old_Pin_Player, New_Pin_Player from pin_changes };
	my $sth = $dbh->prepare($query) || die "get_id_changes(): $query\n $DBI::errstr";

	$sth->execute() || die "get_id_changes(): $query\n $DBI::errstr";
	my @fetch = $sth->fetchrow_array();

	while (scalar(@fetch) and my @fetch = $sth->fetchrow_array()) {
		$IdChanges->{$fetch[0]} = $fetch[1];
	}
	$sth->finish;
	print "get_id_changes(): ", Dumper($IdChanges), "\n" if ($verbose =~ m/:IdChanges:/);
}

sub tally_games($$$) {
	my $Players = shift;
	my $Games = shift;
	my $CrosstabGames = shift;

	return 0 if (scalar @{$Games} == 0);

	# add a 'score' and 'tot_games' to each player to the Players hash which uses AGA IDs for its keys
	# each player now has 'name', 'last_name', 'rank', and 'score' and 'tot_games'
	foreach my $game ( @{$Games} ) {
		my $white = $game->{white};
		my $black = $game->{black};
		my $handicap = $game->{handicap};
		my $result = $game->{result};
		$Players{$white}{tot_games} = $Players{$white}{tot_games} + 1;
		$Players{$black}{tot_games} = $Players{$black}{tot_games} + 1;
		if (not exists $game->{round} ) {
#			$round = 0;	# BYE_XXX
			$round = -1;
			do {
				$round++;
				$Tournament{rounds} = $round if ($round >= $Tournament{rounds});
			} while (exists ($CrosstabGames->{$round}{$white}) or 
				exists ($CrosstabGames->{$round}{$black}));
		}
		else {
			$round = $game->{round} -1 ;
			$Tournament{rounds} = $round if ($round >= $Tournament{rounds});
		}

		$CrosstabGames->{$round}{$white}{opponent} = $black;
		$CrosstabGames->{$round}{$white}{color} =  "w";
		$CrosstabGames->{$round}{$black}{handicap} =  0;

		$CrosstabGames->{$round}{$black}{opponent} = $white;
		$CrosstabGames->{$round}{$black}{color} =  "b";
		$CrosstabGames->{$round}{$black}{handicap} =  $handicap;

		if  ($result eq "W" or $result eq "w")  {
			$Players{$white}{score}++;
			$CrosstabGames->{$round}{$white}{result} = "+";
		}
		else {
			$Players{$white}{score}+= 0;
			$CrosstabGames->{$round}{$white}{result} = "-";
		}
		if  ($result eq "B" or $result eq "b")  {
			$Players{$black}{score}++;
			$CrosstabGames->{$round}{$black}{result} = "+";
		}
		else {
			$Players{$black}{score}+= 0;
			$CrosstabGames->{$round}{$black}{result} = "-";
		}
	}
	print "tally_games(): CrosstabGames = ", Dumper($CrosstabGames) if ($verbose =~ m/:tally_games:/);
	return 1;
}

sub update_players($$$$) {
	my $Tournament = shift;
	my $Players = shift;
	my $PlayersDB = shift;
	my $revert = shift;
	print "update_players()\n" if ($verbose =~ m/:update_players:/);
	foreach my $id (keys %{$Players}) {
		# print "Players->{id}: ", Dumper($Players->{$id}), "\n";
		# print "PlayersDB->{id}: ", Dumper($PlayersDB->{$id}), "\n";
		print "Players->{id}: $id $Players->{$id}{last_name}, $Players->{$id}{name}\n" 
			if ($verbose =~ m/:update_players:/);
	next if ($id == 0);							# !! FIX XXX
		if (not $revert) {
			$Players->{$id}{tot_tournaments} = $PlayersDB->{$id}{tot_tournaments} + 1;
			$Players->{$id}{tot_games} += $PlayersDB->{$id}{tot_games};
		}
		else {
			$Players->{$id}{tot_tournaments} = $PlayersDB->{$id}{tot_tournaments} - 1;
			$Players->{$id}{tot_games} = $PlayersDB->{$id}{tot_games} - $Players->{$id}{tot_games};
		}
		my $update = qq{ UPDATE players SET Tot_Tournaments = $Players->{$id}{tot_tournaments}, Tot_Games = $Players->{$id}{tot_games} };

		print "update_players(): tournament date: $Tournament->{date} vs last_appearance_date: $PlayersDB->{$id}{last_appearance_date}\n" if ($verbose =~ m/:update_players:/);
		if (not $revert) {
#			if ($Tournament->{date} != 0 and $Tournament->{date} > $PlayersDB->{$id}{last_appearance_date}) {
			my ($t_y, $t_m, $t_d) = split "-", $Tournament->{date};
			my ($l_y, $l_m, $l_d) = split "-", $PlayersDB->{$id}{last_appearance_date};
			if ($Tournament->{date} != 0 and 
				( ($t_y > $l_y) or 
				  ($t_y = $l_y and $t_m > $l_m) or 
				  ($t_y = $l_y and $t_m = $l_m and $t_d > $l_d ))) {
				$update .= qq{, Last_Rank = "$Players->{$id}{rank}"};
				$update .= qq{, Last_Appearance = "$Tournament->{code}"}; #, Elab_Date = "$Tournament->{start}" ;
			}
		}
		else {
			if ($Tournament->{start} != 0 and $Tournament->{start} == $PlayersDB->{$id}{last_appearance_date}) {
				my $query = "SELECT Tournament_Code FROM games WHERE (Pin_Player_1 = $id OR Pin_Player_2 = $id) AND Game_Date != \'$PlayersDB->{$id}{last_appearance_date}\' ORDER BY Game_Date DESC";
				my $query = "SELECT DISTINCT Tournament_Code, Game_Date FROM games WHERE (Pin_Player_1 = $id OR Pin_Player_2 = $id) AND Game_Date != \'$PlayersDB->{$id}{last_appearance_date}\' GROUP BY Tournament_Code ORDER BY Game_Date DESC LIMIT 1";
print "update_players(): revert: $revert, $query\n"; 	# 20100305
				my $sth = $dbh->prepare($query) || die "update_players(): $query\n $DBI::errstr";
				$sth->execute() || die "update_players(): $query\n $DBI::errstr";
				my @fetch = $sth->fetchrow_array();
print "most recent tournament: $fetch[0]\n";	# 20100305
				if ($fetch[0] eq "") {
					$update .= qq{, Last_Appearance = NULL};
				}
				else {
					$update .= qq{, Last_Appearance = \'$fetch[0]\'};
				}
			}
		}

		$update .= qq{ WHERE Pin_Player = $id };
		print "update_players(): $update\n\t$Tournament->{code}, $Tournament->{date} vs $PlayersDB->{$id}{last_appearance_date}\n\n" if ($verbose =~ m/:update_players:/ or $verbose =~ m/:revert:/); # 20100305
		do {
			my $sth = $dbh->prepare($update) || die "update_players(): $update\n $DBI::errstr";
			$sth->execute() || die "update_players(): $update\n $DBI::errstr";
			$updated_players++;
		} unless ($shoot_blanks);
	}
	print "\n" if ($verbose =~ m/:update_players:/);
}

sub commit_tournament($$$) {
	my $dbh = shift;
	my $Tournament = shift;
	my $Players = shift;
	my $revert = shift;

	if (not $revert) {		# the common case
		create_tournament($dbh, $Tournament, scalar(keys %{$Players}) );
		create_games($dbh, $Tournament, $Players, \@Games);
		update_players($Tournament, $Players, \%PlayersDB, $revert);
	}
	else {
# print "\n\n\nUp to here for revert....do the check_players() thing for revert\n";
#exit if ($revert);
		update_players($Tournament, $Players, \%PlayersDB, $revert);
		create_games($dbh, $Tournament, $Players, \@Games, $revert);
		create_tournament($dbh, $Tournament, scalar(keys %{$Players}), $revert );
	}

	return 1;
}

sub check_games($$$$$$) {
	my $tournament = shift;
	my $players = shift;
	my $games = shift;
	my $dbh = shift;
	my $PlayersDB = shift;
	my $revert = shift;

	if ( not scalar @{$games} ) {
		move_file($nogames);
		die qq{$0: $results_file: No games found};
	}

	foreach my $game ( @{$games} ) {
		print "check_games(): ", Dumper($game), "\n" if ($verbose =~ m/:check_games:/);
		my $white = $game->{white};
		my $black = $game->{black};
		if (not exists($Players{$white})) {
			if (not fetch_player($white, $dbh, $PlayersDB, $players) ) {
				print "$results_file: check_games(): Missing Player white ($white) in game: ", Dumper($game), "\n";
				move_file($missingplayer);
				return 0;
			}
		}
		if (not exists($Players{$black})) {
			if (not fetch_player($black, $dbh, $PlayersDB, $players) ) {
				print "$results_file: check_games(): $results_file: Missing Player black ($black) in game: ", Dumper($game), "\n";
				move_file($missingplayer);
				return 0;
			}
		}
		if ($white eq $black) {
			move_file($sameplayer);
			print "$results_file: Same Player: TempIDs: ", Dumper(\%TempIDs);
			print "$results_file: Same Player: White: $white -> ", Dumper($players->{$black});
			print "$results_file: Same Player: Black: $black -> ", Dumper($players->{$white});
			print "$results_file: Same Player: check_games(): $results_file: White ($white) is Black ($black) in game: ", Dumper($game), "\n";
			return 0;
			die qq{$results_file: Same Player: White Player($white) same as Black Player($black)\n}
		}
		if ($revert) {
			my $query = qq{SELECT count(*) FROM games WHERE Pin_Player_1 = $white AND Pin_Player_2 = $black and Tournament_Code = "$tournament->{code}"}; 
			print "check_games(): Query: $query\n"  if ($verbose =~ m/:check_games:/);
			my $sth = $dbh->prepare($query) || die "record_last_update(): $query\n $DBI::errstr";
			$sth->execute() || die "record_last_update(): $query\n $DBI::errstr";
			my @fetch = $sth->fetchrow_array();
			$sth->finish;
			print "Fetch: ", Dumper(@fetch), "\n" if ($verbose =~ m/:check_games:/);
			if (scalar(@fetch) > 1) {
				printf "Query found %d games.  Abandoning revert!\n", scalar(@fetch);
				return 0;
			}
		}
	}
	return 1;
}

sub fetch_player ($$$$) {
	my $player = shift;
	my $dbh = shift;
	my $PlayersDB = shift;
	my $Players = shift;

	return 0 if (not $allow_player_fetch);

	my $query = qq{ SELECT p.Pin_Player, p.Last_Name, p.Name, p.Tot_Tournaments, p.Tot_Games, t.Tournament_Date, t.Tournament_Code FROM players p, tournaments t WHERE p.Pin_Player = "$player" AND p.Last_Appearance = t.Tournament_Code };
	my $sth = $dbh->prepare($query) || die "fetch_player(): $query\n $DBI::errstr";

	$sth->execute() || die "fetch_player(): $query\n $DBI::errstr";
	my @fetch = $sth->fetchrow_array();
	$sth->finish;

	if ($fetch[0] == $player) {
		$PlayersDB->{$fetch[0]}{last_name} = $fetch[1];
		$PlayersDB->{$fetch[0]}{name} = $fetch[2];
		$PlayersDB->{$fetch[0]}{tot_tournaments} = $fetch[3];
		$PlayersDB->{$fetch[0]}{tot_games} = $fetch[4];
		$PlayersDB->{$fetch[0]}{last_appearance_date} = $fetch[5];
		$PlayersDB->{$fetch[0]}{last_tournament} = $fetch[6];

		$Players->{$player}{tot_tournaments} = $PlayersDB->{$fetch[0]}{tot_tournaments};
		$Players->{$player}{tot_games} = $PlayersDB->{$fetch[0]}{tot_games};
		$Players->{$player}{last_appearance_date} = $PlayersDB->{$fetch[0]}{last_appearance_date};

		print "fetch_player($player): ", Dumper($PlayersDB{$player}), "\n" if ($verbose =~ m/:fetch_player:/);

		return 1;
	}
#	else {
#		check the PinChanges tables
#		if found there use that as the ID
#		update all the games
#			and the crosstab games
#	}
	return 0;
}

sub player_variations($$$$$) {
	my $dbh = shift;
	my $player = shift;
	my $field = shift;
	my $old   = shift;
	my $new   = shift;
	my $query = qq{ SELECT Old, New from player_variations WHERE Pin_Player = $player AND Field = "$field" AND Old = "$old" };
	print "player_variations(): $query\n" if ($verbose =~ m/:player_variations:/);
	my $sth = $dbh->prepare($query) || die "player_variations(): $query\n $DBI::errstr";
	$sth->execute() || die "player_variations(): $query\n $DBI::errstr";
	my @fetch = $sth->fetchrow_array();
	print "player_variations(): ", Dumper(\@fetch), "\n" if ($verbose =~ m/:player_variations:/);
	$sth->finish;
	if ($new eq $fetch[1]) {
		return (1);
	}
	else {
		if ($old ne "" and $new ne "") {
			print qq{player_variations(): $player, "$field", "$old", "$new"\n};
			# print qq{player_variations(): $player: $old vs $fetch[0], $new vs $fetch[1]\n};
		}
		return (0, $player, $field, $old, $new);
	}
}

sub get_largest_agaid($) {
	my $dbh = shift;

	my $query = qq{ SELECT MAX(Pin_Player) FROM players };
	my $sth = $dbh->prepare($query);
	$sth->execute() || die "check_players(): $query\n $DBI::errstr";
	my @fetch = $sth->fetchrow_array();
	$sth->finish;

	die ( "Failed to get the largest Player ID from database using \"$query\"\n" ) if ( $#fetch < 0);

	return "$fetch[0]";
}

sub check_players($$$$) {
	my $dbh = shift;
	my $Players = shift;
	my $PlayersDB = shift;
	my $Tournament = shift;		# my $tournament = shift; 2015-03-29
	my $revert = shift;

        print Dumper{%{$Tournament}};
	if (not keys %{$Players} ) {
		die qq{check_players(): $results_file: No players found.};
	}

	foreach my $player (sort keys %{$Players}) {
		print "check_players(): $player: ", Dumper($Players->{$player}), "\n" if ($verbose =~ m/:check_players:/);

		# AGA IDs must be numbers only.    Letters are used as one way to flag temporary IDs
		if ( $player =~ m/[^\d]+/ ) {
			print qq{NOTE:  "$player" is not a valid AGA ID for "$Players->{$player}{name} $Players->{$player}{last_name}"\n};
			print qq{       The AGA will assign an AGA ID to this player.\n\n};
			$abort = 1;
			next;
		}

#		my $rank = $Players->{$player}->{rank};
#		$rank =~ s/[pdk]//i;
#		if ($rank < 1 or $rank > 10) {
#			die "check_players(): Invalid rank: $rank for player: $player, $Players->{$player}->{name}";
#		}
#		my $query = qq{ SELECT Pin_Player, Last_Name, Name FROM players where Pin_Player = "$player"};
#		my $query = qq{ SELECT p.Pin_Player, p.Last_Name, p.Name, p.Tot_Tournaments, p.Tot_Games, t.Tournament_Date, p.Rating, p.Last_Rank, p.Last_Appearance FROM players p, tournaments t WHERE p.Pin_Player = $player};

		# Using the AGA ID, try to get the player information
# 20170818		my $query = qq{ SELECT p.Pin_Player, p.Last_Name, p.Name, p.Tot_Tournaments, p.Tot_Games, 
#					t.Tournament_Date, p.Rating, p.Last_Rank, t.Tournament_Code,
#                                        m.renewal_due
#				FROM players p, tournaments t, members m
#				WHERE p.Pin_Player = $player AND p.Last_Appearance = t.Tournament_Code AND p.Pin_Player = m.member_id };
		my $query = qq{ SELECT p.Pin_Player, p.Last_Name, p.Name, p.Tot_Tournaments, p.Tot_Games, 
					p.Rating, p.Last_Rank,
                                        m.renewal_due, m.dues_last_paid, m.last_changed
				FROM players p, members m
				WHERE p.Pin_Player = $player AND p.Pin_Player = m.member_id };
		print "check_players(): Query = $query\n" if ($verbose =~ m/:check_players:/);
		my $sth = $dbh->prepare($query) || die "check_players(): $query\n $DBI::errstr";
		$sth->execute() || die "check_players(): $query\n $DBI::errstr";
		my @fetch = $sth->fetchrow_array();
		print "check_players(): Fetch = \n", Dumper(@fetch), "\n" if ($verbose =~ m/:check_players:/);
		$sth->finish;

		# Does the AGA ID exist ?
		if ($#fetch < 0) {
			if ( $largest_agaid >= $player ) {
				print qq{ERROR: AGA ID: "$player" listed in the tournament results file does not exist.\n};
				print qq{       and should not be used as a temporary ID number because "$player"\n};
#				print qq{       is less than the largest assigned AGA ID which is "$largest_agaid"\n\n};
				$abort = 1;
				next;
			}
			else {
				my $query = qq{ SELECT m.member_id, m.family_name, m.given_names, m.renewal_due, c.code,
							p.Pin_Player, p.Tot_Tournaments, p.Tot_Games, p.Rating, p.Last_Rank, 
							t.Tournament_Code, t.Tournament_Date 
						FROM members m
						LEFT OUTER JOIN chapters c ON m.chapter_id = c.member_id
						LEFT OUTER JOIN players p ON p.Pin_Player = m.member_id 
						LEFT OUTER JOIN tournaments t ON p.Last_Appearance = t.Tournament_Code 
						WHERE m.member_id = $player};								# 20151109
				my $sth_inner = $dbh->prepare($query) || die "check_players(): $query\n $DBI::errstr";							# 20151109
				$sth_inner->execute() || die "check_players(): $query\n $DBI::errstr";									# 20151109
				my @fetch_inner = $sth_inner->fetchrow_array();												# 20151109
				$sth_inner->finish;															# 20151109
				print Dumper(@fetch_inner);														# 20151109

				if (not $foo) {
					# print qq{$tournament: Missing Player:  "$Players->{$player}->{name} $Players->{$player}->{last_name}" with ID "$player"\n};
                                        # print qq{$tournament: Missing Player:  "$player"   "$Players->{$player}->{last_name}, $Players->{$player}->{name}"\n}; 2015-03-29
                                        print qq{$Tournament->{code}: Missing Player:  "$player"   "$Players->{$player}->{last_name}, $Players->{$player}->{name}"\n};
#					exit(-1);
					print qq{       must be a new member of the AGA, as the AGA ID listed in \n};
					print qq{       the results file is greater than the largest assigned AGA ID \n};
					print qq{       which is "$largest_agaid" at this time.\n\n};
				}
				else {
					print qq{INSERT into players (Pin_Player, Last_Name, Name, Club, Country_Code) values ($player, "$Players->{$player}->{last_name}", "$Players->{$player}->{name}", "none", "--");\n};
				}
				$abort = 1;
				next;
			}
		}

		my $length = length($Players->{$player}->{name}) < length($fetch[2]) ? 
			length($Players->{$player}->{name}) : length($fetch[2]);

		if ($exactness =~ m/:agaid:/ and $fetch[0] eq "" ) {
			print qq{ERROR:\tUnknown Player: "$player" is not listed among the players in the AGA Go Database.\n};
			print qq{\tName in tournament results file (if any): "$Players->{$player}->{name} $Players->{$player}->{last_name}"\n\n};
			$abort = 1;
			next;
		}
		elsif ($exactness =~ m/:agaid:/ and $fetch[0] != $player ) {
			print qq{check_players(): $results_file: agaid does not match: '$fetch[0]' vs '$player': database vs results fails: },
			Dumper($Players->{$player}), "\n";
			return 0;
		}

#		if ($player == 0) {		# Player 0 loses by forfeit to all other players
#			$Players{$player}->{rating} = $fetch[6];
#			$Players{$player}->{rank} = $fetch[7];
#		}

		if ($exactness =~ m/:last_name:/ 
			and lc($fetch[1]) ne lc($Players->{$player}->{last_name}) ) {
			my @result = player_variations($dbh, $player, "Last_Name", 
				$Players->{$player}->{last_name}, $fetch[1]);
			if (not $result[0]) {
# DANGEROUS...could be due to a typo in the AGAID rather than forgetting to list the person
#				if (not exists $Players->{$player}->{last_name}) {
#					$Players->{$player}->{last_name} = $fetch[1];
#					$Players->{$player}->{name} = $fetch[2];
#					$Players->{$player}->{tot_tournaments} = $fetch[3];
#					$Players->{$player}->{tot_games} = $fetch[4];
#					$Players->{$player}->{last_appearance_date} = $fetch[5];
#					$Players->{$player}->{rating} = $fetch[6];
#					$Players->{$player}->{rank} = $fetch[7];
#				}
#				else {
				print qq{ERROR:\tUnknown Player: "$player" is not listed among the players in this tournament.\n};
				print qq{\tDid "$fetch[2] $fetch[1]" with AGA ID "$player" play in this tournament?\n};
				print qq{\tCould it be that the name does not match the name in the Go Database?\n};
				print qq{\tName in tournament results file (if any): "$Players->{$player}->{name} $Players->{$player}->{last_name}"\n};
				print qq{\tName in AGA Go Database:                  $fetch[2] $fetch[1]\n\n};
				$abort = 1;
				$abort = 1;
#				}
			}
		}
		$Players->{$player}->{last_name} = $fetch[1]; # get exact capitalization from database
		if ($exactness =~ m/:first_name:/ and 
			substr(lc($fetch[2]), 0, $length) ne substr(lc($Players->{$player}->{name}), 0, $length) ) {
			print qq{check_players(): $results_file: first_name: database vs results fails: '$fetch[0]' vs $player, '$fetch[1]' vs '$Players->{$player}->{last_name}', '$fetch[2]' vs '$Players->{$player}->{name}': },
			Dumper($Players->{$player}), "\n";
			return 0;
		}
		if ($fetch[7] != '' and $fetch[7] < $Tournament{start}) {								# 2015-03-29
			 print qq{check_players('$player', $Players->{$player}->{last_name}, $Players->{$player}->{name}): expired '$fetch[7] (dues_last_paid: $fetch[8], last_changed: $fetch[9]) before $Tournament{start}'\n} ;	# 2015-03-29
                         $abort = 1;
		}												# 2015-03-29
		$PlayersDB->{$fetch[0]}{last_name} = $fetch[1];
		$PlayersDB->{$fetch[0]}{name} = $fetch[2];
		$PlayersDB->{$fetch[0]}{tot_tournaments} = $fetch[3];
		$PlayersDB->{$fetch[0]}{tot_games} = $fetch[4];
		$PlayersDB->{$fetch[0]}{last_appearance_date} = $fetch[5];
		if ($Players{$player}->{rank} eq "0d") { 
			my $rank = parse_rank($fetch[6]);
			print "check_players(): ", Dumper($Players{$player}), "\n$fetch[6]\n$rank\n" if ($verbose =~ m/:check_players:/);; 
			$Players{$player}->{rank} = $rank;
		}
		$PlayersDB->{$fetch[0]}{last_tournament} = $fetch[8];
		print "check_players(): PlayersDB->{$fetch[0]} = ", Dumper($PlayersDB->{$fetch[0]}), "\n" if ($verbose =~ m/:check_players:/);
	}
	# print Dumper($PlayersDB);
	return 1;
}

sub check_tournament($$$$$) {
	my $dbh = shift;
	my $Tournament = shift;
	my $total_players = shift;
	my $total_games = shift;
	my $revert = shift;
	my $found = 0;

	my $count = keys %{$Tournament};
	print "Tournament:($count) ", Dumper($Tournament), "\n" if ($verbose =~ m/:check_tournament:/);

	if ( $Tournament->{start} eq "" ) {
		print "check_tournament(): no tournament start date, going to next query\n"
			 if ($verbose =~ m/:check_tournament:/);
	}
	else {
		my $rounds = $Tournament->{rounds} +1;
		my $players = $total_players +1;
		my $query = qq{ SELECT t.Tournament_Code, t.Tournament_Descr, t.Tournament_Date, t.Rounds, t.Total_Players, count(*) FROM tournaments t, games g WHERE t.Tournament_Date = "$Tournament->{start}" AND t.Rounds = $rounds AND t.Total_Players = $players AND t.Tournament_Code = g.Tournament_Code };
		my $sth = $dbh->prepare($query) || do {print "check_tournament(): $query\n"; die "$DBI::errstr";} ;
		print "check_tournament(): Query: $query\n" if ($verbose =~ m/:check_tournament:/);
		$sth->execute() || do {print "check_tournament(): $query\n"; die "$DBI::errstr";} ;
		while (my @fetch = $sth->fetchrow_array()) {
			$found++ if ($fetch[5] == $total_games);
#			print Dumper(\@fetch), "\n";
		}
		$sth->finish;
		if ( not $revert and $found ) {
			$dbh->disconnect();
			return 0;
		}
	}

	my $query = qq{ SELECT Tournament_Code FROM tournaments WHERE Tournament_Code = "$Tournament->{code}" };
	print "check_tournament(): Query: $query\n" if ($verbose =~ m/:check_tournament:/);
	my $sth = $dbh->prepare($query) || do {print "check_tournament(): $query\n"; die "$DBI::errstr";} ;

	$sth->execute() || do {print "$query\n"; die "check_tournament(): $query\n $DBI::errstr";} ;
	my @fetch = $sth->fetchrow_array();
	$sth->finish;
	if ( not $revert and $#fetch != -1 ) {
		# want to add the tournament but it aleady exists
		print "$results_file: check_tournament(): Tournament \'$Tournament->{code}\' already exists\n" 
			if ($verbose =~ m/:check_tournament:/);
		$dbh->disconnect();
		return 0;
	}
	elsif ( $revert and $#fetch == -1 ) {
		# want to revert the tournament but it does not exist
		print "$results_file: check_tournament(): Tournament \'$Tournament->{code}\' does not exist\n" 
			if ($verbose =~ m/:check_tournament:/);
		$dbh->disconnect();
		return 0;
	}
	printf "%d rows fetched from database\n", scalar(@fetch) if ($verbose =~ m/:check_tournament:/);
	return 1;
}

sub create_games($$$$$) {
	my $dbh = shift;
	my $Tournament = shift;
	my $Players = shift;
	my $Games = shift;
	my $revert = shift;
	print "create_games(): ", Dumper($Games) if ($verbose =~ m/:create_games:/);

	my $sql = "";
	if (not $revert) {
#		for my $y (0..$#{@{$Games}}) {
		for my $y (0..$#{$Games}) {
			print "$Games[$y]\n" if ($verbose =~ m/:create_games:/);
			# $sql = qq{ INSERT INTO games (Tournament_Code, Game_Date, Round, Pin_Player_1, Color_1, Rank_1, Pin_Player_2, Color_2, Rank_2, Handicap, Komi, Result, Online) VALUES ("$Tournament->{code}", "$Tournament->{start}", $Games[$y]->{round}, $Games[$y]->{white}, "W", "$Players->{$Games[$y]->{white}}{rank}", $Games[$y]->{black}, "B", "$Players->{$Games[$y]->{black}}{rank}", $Games[$y]->{handicap}, "$Games[$y]->{komi}", "$Games[$y]->{result}", "$online" ) };   				20131017
			$sql = qq{ INSERT INTO games (Tournament_Code, Game_Date, Round, Pin_Player_1, Color_1, Rank_1, Pin_Player_2, Color_2, Rank_2, Handicap, Komi, Result, Online, Exclude) VALUES ("$Tournament->{code}", "$Tournament->{start}", $Games[$y]->{round}, $Games[$y]->{white}, "W", "$Players->{$Games[$y]->{white}}{rank}", $Games[$y]->{black}, "B", "$Players->{$Games[$y]->{black}}{rank}", $Games[$y]->{handicap}, "$Games[$y]->{komi}", "$Games[$y]->{result}", "$online", $Games[$y]->{exclude} ) };

			# used to fix rounds in cong20101008open due to using "==" in place of "eq" in parse_games()
			# print qq{UPDATE games set round = $Games[$y]->{round} WHERE Tournament_Code = "$Tournament->{code}" AND Pin_Player_1 =  $Games[$y]->{white} and Pin_Player_2 =  $Games[$y]->{black};\n};

			print "F'ed Game: $sql\n" if ($$Games[$y]->{exclude} );			# 20131017
			print "$sql\n" if ($verbose =~ m/:create_games:/ or $verbose =~ m/:insert_games:/);
# FIX results of Paul Matthews change from "rating=" to "seed=" in njop2013.... 
# print qq{UPDATE games set Rank_1 = "$Players->{$Games[$y]->{white}}{rank}", Rank_2 = "$Players->{$Games[$y]->{black}}{rank}" where Tournament_Code = "$Tournament->{code}" and Round = $Games[$y]->{round} and Pin_Player_1 = $Games[$y]->{white} and Pin_Player_2 = $Games[$y]->{black} and Handicap = $Games[$y]->{handicap} and Komi = $Games[$y]->{komi} and Result = "$Games[$y]->{result}";\n};
			do {
				my $sth = $dbh->prepare($sql) || do {print "$sql\n"; die "create_games(): $sql\n $DBI::errstr";} ;
				$sth->execute() || do {print "$sql\n"; die "create_games(): $sql\n $DBI::errstr";} ;
				$new_games++;
			} unless ($shoot_blanks);
		}
	}
	else {
		$sql = qq{DELETE FROM games where Tournament_Code = "$Tournament->{code}"};
		print "$sql\n" if ($verbose =~ m/:create_games:/ 
				or $verbose =~ m/:insert_games:/
				or $verbose =~ m/:revert:/);
		do {
			my $sth = $dbh->prepare($sql) || do {print "$sql\n"; die "create_games(): $sql\n $DBI::errstr";} ;
			$sth->execute() || do {print "$sql\n"; die "create_games(): $sql\n $DBI::errstr";} ;
			$new_games++;
		} unless ($shoot_blanks);
	}
	print "\n" if ($verbose =~ m/:create_games:/ or $verbose =~ m/:insert_games:/);
}

sub create_tournament($$$$) {
	my $dbh = shift;
	my $Tournament = shift;
	my $total_players = shift;
	my $revert = shift;

	my $sql = "";
	print "create_tournament()\n" if ($verbose =~ m/:create_tournament:/);
	print Dumper($Tournament), "\n"; # XXX 20130225

	if (not exists $Tournament->{start}) {
		if ($Tournament->{code} =~ m/^\w*(\d{4})(\d{2})(\d{2})\w*$/) {
			my $y = $1;
			my $m = $2;
			my $d = $3;
			$Tournament->{start} = $1."-".$2."-".$3;
			print "Code to date: $Tournament->{code} -> $Tournament->{start}\n" 
				if ($verbose =~ m/:create_tournament:/);
		}
		elsif ($Tournament->{stop} =~ m/^\w*(\d{4})(\d{2})(\d{2})\w*$/ ) {
			my $y = $1;
			my $m = $2;
			my $d = $3;
			$Tournament->{start} = $1."-".$2."-".$3;
			print "Stop to start: $$Tournament->{code}: $Tournament->{stop} -> $Tournament->{start}\n" 
				if ($verbose =~ m/:create_tournament:/);
		}
		elsif ($Tournament->{date} =~ m/^\w*(\d{4})(\d{2})(\d{2})\w*$/) {
			my $y = $1;
			my $m = $2;
			my $d = $3;
			$Tournament->{start} = $1."-".$2."-".$3;
			print "Date to start: $$Tournament->{code}: $Tournament->{date} -> $Tournament->{start}\n"
				 if ($verbose =~ m/:create_tournament:/);
		}
	}
	if (not exists $Tournament->{date}) {
		$Tournament->{date} = $Tournament->{start};
	}
	print Dumper(\%Tournament) if ($verbose =~ m/:create_tournament:/);

	if (not $revert) {
		$Tournament->{rounds}++;
		my $fields = qq{ Country_Code, Tournament_Code, Tournament_Descr, Tournament_Date, Rounds, Total_Players};
		my $values = qq{ "US", "$Tournament->{code}", "$Tournament->{name}", "$Tournament->{start}", "$Tournament->{rounds}", "$total_players"};

		if ($Tournament->{city}) { 
			$fields .= qq{, City}; 
			$values .= qq{, "$Tournament->{city}"}; 
		}
		if ($Tournament->{state}) { 
			$fields .= qq{, State_Code}; 
			$values .= qq{, "$Tournament->{state}"}; 
		}
		$fields .= qq{, Wallist}; 
		$values .= qq{, "$Tournament->{crosstab}" };

		my $insert = qq{ INSERT INTO tournaments ($fields) VALUES ($values) };
		print "$insert\n\n" if ($verbose =~ m/:create_tournament:/ or $verbose =~ m/:update_tournament:/);
		$sql = $insert;
		$Tournament->{rounds}--;
	}
	else {
		$sql = qq{DELETE FROM tournaments WHERE Tournament_Code = "$Tournament->{code}"};
		print "$sql\n\n" if ($verbose =~ m/:update_tournament:/ or $verbose =~ m/:revert:/);
	}
	return 1 if ($shoot_blanks);

	my $sth = $dbh->prepare($sql) || do {print "$sql\n"; die "create_tournament(): $sql\n $DBI::errstr";} ;
	$sth->execute() || do {print "$sql\n"; die "create_tournament(): $sql\n $DBI::errstr";} ;
	$new_tournaments++;
}

# Code created by Philip Waldron, 2008
# adapted for use here, with permission of Philip Waldrom, by Jonathan M. Bresler, 2008
sub print_crosstab($$$$$$) {
	my $Tournament = shift;
	my $Players = shift;
	my $CrosstabGames = shift;
	my $PlayersSorted = shift;
	my $Order = shift;
	my $summary_only = shift;
	# Print out the tournament grid
	for my $ID ( @{$PlayersSorted} ) {

		# The player number plus name is currently allowed 30 characters of space
		my $Name = "$Players{$ID}{last_name}, $Players{$ID}{name} ";
#		$Tournament{crosstab} .= sprintf ("%3d: %-28s\t%5d   %3s\t", 
#			$Order{$ID}, $Name, 
#			$ID, lc($Players{$ID}{rank}) );
		$Tournament{crosstab} .= sprintf ("%3d: %-25s %5s ", 
			$Order->{$ID}, $Name, lc($Players{$ID}{rank}) );

		# Keep track of a player's record
		my $WinCount      = 0;
		my $LossCount     = 0;
		my $TieOrByeCount = 0;

		# Print out round by round data
		# An opponent ID of -1 will be left over from the earlier data input
		# Since it hasn't been overwritten, treat it as a bye.
		for (my $i = 0; $i <= $Tournament->{rounds}; $i++) {	# BYE_XXX
#		for (my $i = 0; $i < $Tournament->{rounds}; $i++) {
			if (not defined ($CrosstabGames{$i}{$ID}{opponent}) ) {
				$Tournament->{crosstab} .= sprintf("      bye ")
					if (not $summary_only);
				$TieOrByeCount++;
			}
			elsif ($CrosstabGames{$i}{$ID}{opponent} != -1) {
				$Tournament->{crosstab} .= sprintf ("%5d%s/%s%d ", 
					$Order->{$CrosstabGames{$i}{$ID}{opponent}}, 
					$CrosstabGames{$i}{$ID}{result},
					$CrosstabGames{$i}{$ID}{color},
					$CrosstabGames{$i}{$ID}{handicap})
						if (not $summary_only);
				if ($CrosstabGames{$i}{$ID}{result} eq "+") {
					$WinCount++;
				}
				else {
					$LossCount++;
				}
			}
		}
		if (not $summary_only) {
			$Tournament->{crosstab} .= sprintf(" %d-%d-%d\n", $WinCount, $LossCount, $TieOrByeCount);
		} 
		else {
			$Tournament->{crosstab} .= sprintf(" %d-%d\n", $WinCount, $LossCount);
		}
	}
}


# Code created by Philip Waldron, 2008
# adapted for use here, with permission of Philip Waldron, by Jonathan M. Bresler, 2008
sub generate_crosstab($$$) {
	my $Tournament = shift;
	my $Players = shift;
	my $CrosstabGames = shift;
	print "generate_crosstab()\n" if ($verbose =~ m/:generate_crosstab:/);

	# add a 'rating' to each player to the Players hash which uses AGA IDs for its keys
	# each player now has 'name', 'last_name', 'rank', 'score', 'tot_games', and 'rating'
	foreach my $id (keys %{$Players}) {
		my $rank = $Players->{$id}{rank};
		if ($rank =~ m/(\d+)k/i) {
			$Players->{$id}{rating} = -1.0 * ( $1+0.5 );
		}
		elsif ($rank =~ m/(\d+)d/i) {
			$Players->{$id}{rating} = $1+0.5;
		}
		else {
			# If we get this far, then the data must have come in as a real rating
			$Players->{$id}{rating} = $rank;
		}
	}

# print Dumper{%Players};	20110703
# exit;
	my %Order;
	my @PlayersSorted = sort {
		# sort by rating first
#		if ( ($Players{$b}{rating} <=> $Players{$a}{rating}) != 0 ) {
#			$Players{$b}{rating} <=> $Players{$a}{rating};
#		}
#		# then number of wins
#		elsif ( ($Players{$b}{score} <=> $Players{$a}{score}) != 0 ) {
#			$Players{$b}{score} <=> $Players{$a}{score};
#		}
		if ( ( ($Players{$b}{rating} + $Players{$b}{score}) <=> ($Players{$a}{rating} + $Players{$a}{score}) ) != 0 ) {
			($Players{$b}{rating} + $Players{$b}{score}) <=> ($Players{$a}{rating} + $Players{$a}{score});
		}
		elsif ( ($Players{$b}{rating} <=> $Players{$a}{rating}) != 0 ) {
			$Players{$b}{rating} <=> $Players{$a}{rating};
		}
		else {
#			for (my $i = 0; $i <= $Tournament->{rounds}; $i++) 
			for (my $i = 0; $i < $Tournament->{rounds}; $i++) {
				my $id = $CrosstabGames{$i}{$a}{opponent};
				$Players{$a}{sos} += $Players{$id}{score};
				my $id = $CrosstabGames{$i}{$b}{opponent};
				$Players{$b}{sos} += $Players{$id}{score};
			}
			# then Sun of Opponents Scores
			if ( ($Players{$b}{sos} <=> $Players{$a}{sos}) != 0 ) {
				$Players{$b}{sos} <=> $Players{$a}{sos};
			}
			else {
				my $result = 0;
#				for (my $i = 0; $i <= $Tournament->{rounds}; $i++) 
				for (my $i = 0; $i < $Tournament->{rounds}; $i++) {
					my $id = $CrosstabGames{$i}{$a}{opponent};
					# then Direct Confrontation
					if ($id == $b and $CrosstabGames{$i}{$a}{result} eq "+") {
						$result = -1;
					}
					else {
						$result = 1;
					}
				}
				$result;
			}
		}
	} keys %Players;

	my $place = 0;
	foreach (@PlayersSorted) {
	        $Order{$_} = ++$place;
	}

	print "PlayersSorted: ".Dumper(\@PlayersSorted) if ($verbose =~ m/:generate_crosstab:/);
	print "Order: ".Dumper(\%Order) if ($verbose =~ m/:generate_crosstab:/);

	$Tournament{crosstab} = "\n";
	print_crosstab($Tournament, $Players, $CrosstabGames, \@PlayersSorted, \%Order, 0);

	print "$Tournament->{crosstab}" if ($verbose =~ m/:generate_crosstab:/ or $verbose =~ m/:print_crosstab:/);
	chomp $Tournament->{crosstab};
#	print "generate_crosstab: Players", Dumper($Players), "\n";
#	print "generate_crosstab: Tournament", Dumper($Tournament), "\n";
	
	if ( $Tournament->{rounds} < 0 ) {
		move_file($toofewrounds);
		die qq{$results_file: Too few rounds: $Tournament->{rounds}};
	}
	
	if ( $Tournament->{rounds} > $MAX_ROUNDS ) {
		move_file($processed);
		$Tournament{crosstab} = "\nCross table truncated due to number of rounds\n\n";
		print_crosstab($Tournament, $Players, $CrosstabGames, \@PlayersSorted, \%Order, 1);
		print "$Tournament->{crosstab}" if ($verbose =~ m/:generate_crosstab:/);
		print qq{$results_file: Too many rounds: $Tournament->{rounds}, truncated crosstab\n\n};
		$Tournament->{rounds} = -1;
	}
}

# get a connection to the database so that we can query for the data needed
#
sub getdbiconnection() {
	print "getdbiconnection()\n" if ($verbose =~ m/:getdbiconnection:/);
	my $dsn = "DBI:mysql:usgo_agagd_db:localhost";
	my $user = "...";
	my $password = "...";

	my $dbh = DBI->connect($dsn, $user, $password, { RaiseError => 1 });
	print Dumper($dbh) if ($verbose =~ m/:getdbiconnection:/);
	return $dbh;
#     my $dbh = DBI->connect($dsn, $user, "", { RaiseError => 1 });
}

# 2003-01-12
# 10/12/02
# 2/18/2006
# April 18, 1993
sub convert_date($$$) {
	my $file = shift;
	my $date = shift;
	my $check_day = shift;
	my @base = fileparse($ARGV[0], qr/\.[^.]*/);
	my ($a, $b, $c, $year, $month, $day, $f_year, $f_month, $f_day);
	$file = $base[0];
	$file =~ s/^.*?([0-9]+).*?$/$1/;
	if ( length($file) == 8 ) {
		$f_year =  substr ($file, 0, 4);
		$f_month = substr ($file, 4, 2);
		$f_day =   substr ($file, 6, 2);
	}
	elsif ( length($file) == 3 ) {
		$f_month = substr ($file, 1, 2);
		$f_year = $f_day = -1;
	}
# noca20050611: June 11, 12: 2005 06 11 ->    -> Year: , Month: , Day:  -> 0000-00-00
	print qq{convert_date("$file", "$date", "$check_day")\n} if ($verbose =~ m/:convert_date:/);
	print "\t$base[0]: $date: $f_year $f_month $f_day" if ($verbose =~ m/:convert_date:/);
	$date =~ s:/:-:g;
	if ( $date =~ m/^([0-9]{1,4})-([0-9]{1,2})-([0-9]{1,4})/ ) {
		($a, $b, $c) = ($1, $2, $3); # split (/-/, $date);
		if ($c > 31) {
			$year = $c;
			$month = $a;
			$day = $b;
		} 
		elsif ($a > 31) {
			$year = $a;
			$month = $b;
			$day = $c;
		}
		else {
			$year = $f_year;
			$month = $f_month;
			$day = $f_day;
		}
	}
	elsif ( $date =~ m/^([A-Za-z]+) ([0-9]{1,2})-[0-9]{1,2}, ([0-9]{1,4})/ or
		$date =~ m/^([A-Za-z]+) ([0-9]{1,2}), ([0-9]{1,4})/ ) {
		($a, $b, $c) = ($1, $2, $3); # split (/ /, $date);
		my $months = { 'jan' => 1, 'feb' => 2, 'mar' => 3,
				'apr' => 4, 'may' => 5, 'jun' => 6, 
				'jul' => 7, 'aug' => 8, 'sep' => 9,
				'oct' => 10, 'nov' => 11, 'dec' => 12 };
		$month = $months->{ lc( substr($a, 0, 3) ) };
		($day = $b) =~ s/,//;
		$year = $c;
	}
	if ($f_year != $year and $exactness =~ m/:year:/) {
		move_file($yearmismatch);
		die qq{$ARGV[0]: Year Mismatch $f_year vs $year};
	}
	if ($f_month != $month and $exactness =~ m/:month:/) {
		move_file($monthmismatch);
		die qq{$ARGV[0]: Month Mismatch $f_month vs $month};
	}
	if ($f_day != $day and $check_day and $exactness =~ m/:day:/) {
		move_file($daymismatch);
		die qq{$ARGV[0]: Day Mismatch $f_day vs $day};
	}

	$date = sprintf "%04d-%02d-%02d", $year, $month, $day;
	print " -> $a $b $c -> Year: $year, Month: $month, Day: $day -> $date\n" if ($verbose =~ m/:convert_date:/);
	return $date;
}

sub parse_game($$$$$$$$$) {
	my $file = shift;
	my $line = shift;
	my $white = shift;
	my $black = shift;
	my $result = shift;
	my $handicap = shift;
	my $komi = shift;
	my $round = shift;
	my $exclude = shift;		# 20131017

#	$white =~ s/[A-Za-z]*//;	# USA12345 -> 12345
#	$white = int($white);
#	$black =~ s/[A-Za-z]*//;	# TMP12345 -> 12345
#	$black = int($black);
	$handicap = 0 if (not int($handicap));
	$komi = 0 if (not int($komi));
	print qq{parse_game(): input: '$line' vs '$white', '$black', '$result', '$handicap', '$komi'\n} 
		if ($verbose =~ m/:Found_Game:/);
	$white = $TempIDs{$white} if (exists($TempIDs{$white}));
	$white = $IdChanges{$white} if (exists($IdChanges{$white}));	# IdChanges: PinChanges -> Player_Variations
	$black = $TempIDs{$black} if (exists($TempIDs{$black}));
	$black = $IdChanges{$black} if (exists($IdChanges{$black}));	# IdChanges: PinChanges -> Player_Variations
	$result = uc($result);
	print qq{parse_game(): after TempIDs and IdChanges: '$line' vs '$white', '$black', '$result', '$handicap', '$komi'\n} 
		if ($verbose =~ m/:Found_Game:/);


# parse_game(): input: '10535 8885 B 3 0' vs '10535', '8885', 'B', '3', '0'
# parse_game(): after TempIDs and IdChanges: '10535 8885 B 3 0' vs 't001', '8885', 'B', '3', '0'
# Game: $VAR1 = {
#           'white' => 't001',
#           'black' => 8885,
#           'round' => 26,
#           'komi' => 0,
#           'handicap' => 3,
#           'result' => 'B'
#         };


	unless ($result eq "B" or $result eq "W") {
		move_file($badresult);
		die qq{$results_file: parse_game(): $file: Unknown result: $result\n}
	}
	if ($komi < $MIN_KOMI or $komi > $MAX_KOMI) {
		move_file($badkomi);
		die qq{$file: Komi: $komi out of range ($MIN_KOMI - $MAX_KOMI)\n\n};
	}

	if (not $round) {
		my $w = 1;
		my $b = 1;
		foreach my $game (@Games) {
			# was "==" in place of "eq"...caused the number of rounds 
			# to exceed 255 due to comparing usa##### to usa####
			$w++ if ( $game->{white} eq $white or $game->{black} eq $white );
			$b++ if ( $game->{white} eq $black or $game->{black} eq $black );
		}
		$round = ($w > $b ? $w : $b);
                printf ("White: %5d, Black: %5d, \$w: %2d, \$b: %2d, \$round: %2d\n", $white, $black, $w, $b, $round);	# jmb 20141212
	}
	my $game = {	
		white => $white, black => $black, result => $result, 
		handicap => $handicap, komi => $komi, round => $round,
		exclude => $exclude,
	};
	if ($game->{exclude}) {					# 20131017
		print "F'ed game: ", Dumper($game), "\n";
	}
	push @Games, $game;
	print "parse_game(): Game: ", Dumper($game), "\n" if ($verbose =~ m/:Found_Game:/);
}

sub parse($$) {
	my $file = shift;
	my $line = shift;
	print "$file: $line\n" if ($verbose =~ m/:parse:/);

        utf8::decode($line);    # 20150425
	$line =~ s/\\/ /g;	# seat20080302.in: Name:     SGC Monthly Ratings Tournament\
	$line =~ s/"/'/g;	# mga20080720.in: Jeffrey O"Connell
        $line =~ s/\s+/ /g;

#END
# end of file
#	if (	$line =~ m/^END$/ or
#		$line =~ m/# end of file/ ) {
	if (	$line =~ m/^[#]*\s*END$/ ) {
		last;
	}
	if (	$ignore_all_hashes and $line =~ m/^#.*/) {
		next;
	}
	elsif ( $line =~ m/^\w*\d+\s+\w*\d+\s+(\?)\s+\d\s+[-]*\d+.*/) {
		print "parse(): Invalid game result: $1. Moving results file to $checkgames.\n" 
			if ($verbose =~ m/:parse:/);
		move_file($badresult);
		print qq{$results_file: $Tournament{name}: Invalid game results: '$line'\n};
		$abort_after_parse = 1;
#		die qq{$results_file: $Tournament{name}: Invalid game results: '$line'};
	}
	elsif ( $line =~ m/^\w*\d+\s+\w*\d+\s+(N)\s+\d\s+[-]*\d+.*/i) {
		print qq{Skipping "N" result: "$line"\n};
		next;
	}
# 15845	16089	N	0	7		feng20090124.in
# 16085	0	WF	0	0		feng20090124.in
	elsif (    $line =~ m/^\w*\d+\s+\w*\d+\s+\w*\d+\s+\w*\d+\s+(B)\s+\d\s+[-]*\d+.*/i
		or $line =~ m/^\w*\d+\s+\w*\d+\s+\w*\d+\s+\w*\d+\s+(W)\s+\d\s+[-]*\d+.*/i
		# or $line =~ m/^\w*\d+\s+\w*\d+\s+\w*\d+\s+\w*\d+\s+(BF)\s+\d\s+[-]*\d+.*/i
		# or $line =~ m/^\w*\d+\s+\w*\d+\s+\w*\d+\s+\w*\d+\s+(WF)\s+\d\s+[-]*\d+.*/i 
											) {
		print "parse(game_1): found game: $line\n" if ($verbose =~ m/:parse:/ or $verbose =~ m/:parse_games:/);
		my ( $round, $board, $white, $black, $result, $handicap, $komi, @etc) = split(/ /, $line);
		my $exclude = 0;					# 20131017
#print "$round, $board, $white, $black, $result, $handicap, $komi, @etc\n";					#	XXX 20101008
		if ($result =~ m/[WB]F/) {
			print "Found F'ed game (1): $line\n";		# 20131017
			$exclude = 1;
		}
		$result =~ s/([WB])F/$1/i;
		$white = lc($white);
		$white = int($white) if ($white =~ m/^[0-9]+$/);		# remove leading zeros
		$black = lc($black);
		$black = int($black) if ($black =~ m/^[0-9]+$/);		# remove leading zeros
		parse_game($file, $line, $white, $black, $result, $handicap, $komi, $round, $exclude);		# 20131017
	}
# 3502 7109 W 3 0^M
# 9425 tmp1 W 0 0
# 5591 6207 w 0  0  # board 12^M
#3508 4676 w 0
#3508 1769 b
#6169 5298 B #
#14508   14028   3       0       W
	elsif (	$line =~ m/^\w*\d+\s+\w*\d+\s+[WwBb]\s+\d\s+[-]*\d+.*\s+#.*/ or
		$line =~ m/^\w*\d+\s+\w*\d+\s+[WwBb]\s+\d\s+[-]*\d+/ or
		$line =~ m/^\w*\d+\s+\w*\d+\s+[WwBb]F\s+\d\s+[-]*\d+/ or
		$line =~ m/^\w*\d+\s+\w*\d+\s+[WwBb]\s+\d/ or
		$line =~ m/^\w*\d+\s+\w*\d+\s+[WwBb][\s#
]*$/ ) {
		print "parse(game_2): found game: $line\n" if ($verbose =~ m/:parse:/ or $verbose =~ m/:parse_games:/);
		my ( $white, $black, $result, $handicap, $komi, @etc) = split(/ /, $line);
		my $exclude = 0;					# 20131017
		if ($result =~ m/[WB]F/) {
			print "Found F'ed game (2): $line\n";		# 20131017
			$exclude = 1;
		}
		$result =~ s/([WB])F/$1/i;
		$white = lc($white);
		$white = int($white) if ($white =~ m/^[0-9]+$/);		# remove leading zeros
		$black = lc($black);
		$black = int($black) if ($black =~ m/^[0-9]+$/);		# remove leading zeros
		# parse_game($file, $line, $white, $black, $result, $handicap, $komi, 0);
		parse_game($file, $line, $white, $black, $result, $handicap, $komi, $round, $exclude);	# 20131017
	}
# ceda20060302:           13720 13887  9  0  b
	elsif ( $line =~ m/^\w*\d+\s+\w*\d+\s+\d\s+\d\s+[WwBb]/ ) {
		print "parse(game_3): found game: $line\n" if ($verbose =~ m/:parse:/ or $verbose =~ m/:parse_games:/);
		my ( $white, $black, $handicap, $komi, $result, @etc) = split(/ /, $line);
		my $exclude = 0;					# 20131017
		if ($result =~ m/[WwBb]F/) {
			print "Found F'ed game (3): $line\n";		# 20131017
			$exclude = 1;
		}
		$result =~ s/([WwBb])F/$1/i;
		# parse_game($file, $line, $white, $black, $result, $handicap, $komi, 0);
		parse_game($file, $line, $white, $black, $result, $handicap, $komi, $round, $exclude);		# 20131017
	}
#5213 3856 w -5		phil19920301a.in
	elsif ( $line =~ m/^\w*\d+\s+\w*\d+\s+[WwBb]\s+-*\d+/ ) {
		print "parse(game_4): found game: $line\n" if ($verbose =~ m/:parse:/ or $verbose =~ m/:parse_games:/);
		my ( $white, $black, $result, $komi) = split(/ /, $line);
		my $exclude = 0;					# 20131017
		if ($result =~ m/[WwBb]F/) {
			print "Found F'ed game (4): $line\n";		# 20131017
			$exclude = 1;
		}
		$result =~ s/([WwBb])F/$1/i;
		# parse_game($file, $line, $white, $black, $result, 0, $komi, 0);
		parse_game($file, $line, $white, $black, $result, 0, $komi, $round, $exclude);		# 20131017
	}
# 3502 Kim, Edward 7.8 CLUB=SEAG
# 2894 St. Stringfellow Jr. Esq., Steve C. 6D
# TMP2 Noyes, Joe 5K
# 1144 Casey, Eva W.                        3K
# 5591 Cordingley, Robert James     13k none^M
# USA11170 Gabelman, Joel -5.0
# CIN04 Martin, Peter 4k
# 01664 Pearson, David -4
# 15016 Ben-Ezri Ravin, Lihu 4k
# 15016 Ravin, Lihu Ben-Ezri 8 kyu					verm20070929.in
#NEW3 NAME="Ball, Chris" RATING=-2.50000 SIGMA=0.80000
#3831 NAME="Feldman, Teddy" RATING=-4.70000 SIGMA=0.80000
#9063 NAME="Park, Chang" RATING=4.70000 SIGMA=0.80000
#3190 Fusselman, Jerry rating=4.0
#2445	Arnold, Keith L.	  3.4 #	MD	9503			phil19950301.in
# 16802   Copley, Tim							tria20080920.in
#7257 Lafleche, Pierre-Yves rating=-4.5					jujo19940501.in
# 15227 Herwitt, Nathan          5.1k					spri20080503.in
#7473 Wu, Jianming (Jimmy) 7d						spri20080503.in

#TournamentArchive/phil19950301.in 2445 Arnold, Keith L. 3.4 # MD 9503
#parse(): found player: 2445:Arnold, Keith L. 3.4 # MD:9503
#Player: ID: 81, Fullname: Arnold, Keith L. 3.4 # MD, Rank: 9503
#Last Name: 'Arnold', Name: 'Keith L. 3.4 # Md'
#81: $VAR1 = {
#          'name' => 'Keith L. 3.4 # Md',
#          'rank' => '9503d',
#          'last_name' => 'Arnold'
#        };
# 16881 O'Leary, P.J. -24.4
# 3393  Lee, Xian [Shane]   5D                 10 ./TournamentArchive/nyws19981201.
#
# Elad Idan 16106 6k current
#
# 	BEWARE order of matching matters
	elsif ( $line =~ s/^([A-Za-z]*[0-9]+)\s+NAME=["']+(.+)["']+\s+RATING=\s*([-]*[0-9]+\.{0,}[0-9]*).*$/$1:$2:$3/i or
		$line =~ s/^([-A-Za-z,\.'_ ()]+)\s([A-Za-z]*[0-9]+)\s([0-9]+)[ ]*([pdk]).*$/$2:$1:$3$4/i or
		$line =~ s/^([A-Za-z]*[0-9]+)\s([-A-Za-z,\.'_ ()]+)\s([0-9]+)[ ]*([pdk]).*$/$1:$2:$3$4/i or
		$line =~ s/^([A-Za-z]*[0-9]+)\s([-A-Za-z,\.'_ ()]+)\s([-]*[0-9]+\.{0,}[0-9]*[pdk]{0,1}).*$/$1:$2:$3/i or 
		$line =~ s/^([A-Za-z]*[0-9]+)\s+([-A-Za-z,\.'_ ()]+)\s+([-]*[0-9]+\.{0,}[0-9]*[pdk]{0,1}).*$/$1:$2:$3/i or 	
		$line =~ s/^([A-Za-z]*[0-9]+)\s([-A-Za-z,\.'_ ()\[\]]+)\s([-]*[0-9]+\.{0,}[0-9]*[pdk]{0,1}).*$/$1:$2:$3/i or 
		$line =~ s/^([A-Za-z]*[0-9]+)\s([-A-Za-z,\.'_ ()]+)\srating=([-]*[0-9]+\.{0,}[0-9]*).*$/$1:$2:$3/i or 
		$line =~ s/^([A-Za-z]*[0-9]+)\s([-A-Za-z,\.'_ ()]+)[\s]*$/$1:$2:/i ) {
		$ignore_all_hashes = 1;		# ugly hack
		print "parse(): found player: $line\n" if ($verbose =~ m/:parse:/);
#3105 Peter Schumer 2K
		my ($id, $fullname, $rank) = split /:/, $line;
		$id = lc($id);
		$id = int($id) if ($id =~ m/^[0-9]+$/);		# remove leading zeros
		$id = $TempIDs{$id} if (exists($TempIDs{$id}));
		$id = $IdChanges{$id} if (exists($IdChanges{$id}));	# IdChanges: PinChanges -> Player_Variations
		print "Player: ID: $id, Fullname: $fullname, Rank: $rank\n" if ($verbose =~ m/:parse_players:/);

		my ($last_name, $name) = parse_name($id, $line, $fullname);
		if (not exists($Players{$id})) {
			$Players{$id}{name} = $name;
			$Players{$id}{last_name} = $last_name;
		}
		#print "$id: $Players{$id}{name} ne $name!\n" if ($Players{$id}{name} ne $name);
		#print "$id: $Players{$id}{last_name} ne $last_name!\n" if ($Players{$id}{last_name} ne $last_name);
		$rank = parse_rank($rank);
		$Players{$id}{rank} = lc($rank);	# for ranks from [PDKpdk] matches
	}
	# elsif ($line =~ s/^([A-Za-z]*[0-9]+)\s+([-A-Za-z,\.'_ ()]+)\s+/$1:$2/i) {
#	elsif ($line =~ s/^([A-Za-z]*[0-9]+)\s+([A-Za-z,]*)/$1:$2/i) {
#		print "new parse stanza: %s $line\n";
#		my ($id, $fullname, $rank) = split /:/, $line;
#		print "Player: ID: $id, Fullname: $fullname, Rank: $rank\n";
#	}
#14160^M
#        Name="Chu, David"^M
#        Rating="-22.00000"^M
#        Sigma="2.97321"^M
#        Date="12/17/2006"^M
#        MExp="12/27/2006"^M
#        MType="Youth"^M
#        Chapter="FYGS"^M
#        State="NY"^M
#        TRandom="0.8434367"^M
#1319^M
#  name="Sanet, Joel S."^M
#        rank=3.5^M
#^M
#before outer while : 'USA3129'
#after outer while: 'USA3129'
#Unparsed line: ./toyo20011203sb.in: 'NAME="Mendenhall, Robert"'
#Unparsed line: ./toyo20011203sb.in: 'RATING=-1.10000'
#Unparsed line: ./toyo20011203sb.in: 'SIGMA=0.60000'
#before outer while : 'USA9149'
#after outer while: 'USA9149'
#
	elsif (	$line =~ m/^[A-Za-z]*\d+
{0,}$/ ) {
		while ( $line =~ m/^([A-Za-z]*\d+)
{0,}$/ ) {
			my $id = $1;
			print "parse(): found multi-line player: $line\n" if ($verbose =~ m/:parse:/);
			$id = lc($id);
			# $id = int($id);		# remove leading zeros
			$id = $TempIDs{$id} if (exists($TempIDs{$id}));
			$id = $IdChanges{$id} if (exists($IdChanges{$id})); # IdChanges: PinChanges -> Player_Variations
			while ($line = <FILE>) {
				$line = prep_line($line);
				if ( $line =~ m/^(\d+)
{0,}$/ ) {
					$id = $1;
					# print "id: '$id', '$line'\n";
					# print "TempIDs: " . Dumper(\%TempIDs) . "\n";
					$id = int($id);		# remove leading zeros
					$id = $TempIDs{$id} if (exists($TempIDs{$id}));
					$id = $IdChanges{$id} if (exists($IdChanges{$id})); # IdChanges: PinChanges -> Player_Variations
					print "id: '$id', '$line'\n" if ($verbose =~ m/:parse:/);
				}
				elsif ($line =~ m/^\s*name="([^"]+)"/i) {
					# print "name= $line\n";
					my ($last_name, $name) = parse_name($id, $line, $1);
					if (not exists($Players{$id})) {
						$Players{$id}{name} = $name;
						$Players{$id}{last_name} = $last_name;
					}
				}
				elsif (	$line =~ m/^\s*rank=([-]*[0-9]+\.[0-9]+|[-]*[0-9]+[pdk])/i or
					$line =~ m/^\s*Rating=["]*([-]*[0-9]+\.[0-9]+)/i or
					# Paul Matthews change from "rating=" to "seed=" in njop2013....
					$line =~ m/^\s*Seed=["]*([-]*[0-9]+\.[0-9]+)/i ) {
					# print "rank|rating=: $line\n";
					my $rank = parse_rank($1);
					$Players{$id}{rank} = lc($rank);	# for ranks from [PDKpdk] matches
				} 
				elsif (	$line =~ m/\s*Sigma=/i or
					$line =~ m/\s*Date=/i or
					$line =~ m/\s*MExp=/i or
					$line =~ m/\s*MType=/i or
					$line =~ m/\s*Chapter=/i or
					$line =~ m/\s*State=/i or
					$line =~ m/\s*TRandom=/ ) {
					# print "...= $line\n";
					next;
				}
				else {
					# die "$results_file, $fix_file: parse() else: $line\n";	whee
					last;
				}
			}
			print "parse(): Player: $id: ", Dumper($Players{$id}), "\n" if ($verbose =~ m/:parse_players:/);
		}
	}
#tmp1	USA10922	Nan, Frank
#96866 USA4773 Hopkins, Mark
#8025     8025   Egan, Aidan
#NEW_01	USA12227	Chaput, Eric
	elsif (	$line =~ m/^\s*([A-Za-z]*[0-9]+)\s+([A-Za-z]*[0-9]+)\s+(.*)/ or
		$line =~ m/^\s*([A-Za-z_]*[0-9]+)\s+([A-Za-z]*[0-9]+)\s+(.*)/) {
		my $tempid = lc($1); #int($1);	# remove leading zeros
		my $id = lc($2); #int($2);	# remove leading zeros
		my $rest = $3;

		next if ($tempid =~ m/[0-9]{3}/ and $id =~ m/[0-9]{3}/ and $rest =~ m/[0-9]{4}/); # phone number
		print "parse(): found fix file entry: $line\n" if ($verbose =~ m/:parse:/);
		print "parse_fix_players()\n" if ($verbose =~ m/:parse_fix_players:/);
		$rest =~ s/, /,/;
		my @etc = split / /, $rest;

		my $fullname = join ' ', @etc;
		print "Temp ID: $tempid, ID: $id, Full Name: $fullname\n" if ($verbose =~ m/:parse_fix_players:/);

#		next if ($tempid == $id);	# 8025     8025   Egan, Aidan

		$id = $IdChanges{$id} if (exists($IdChanges{$id}));		# 20090330 midnight # IdChanges: PinChanges -> Player_Variations
#		my ($last_name, $name) = split(/,/, $fullname);
		my ($last_name, $name) = parse_name($id, $line, $fullname);
		print "Last Name: $last_name, Name: $name\n" if ($verbose =~ m/:parse_fix_players:/);
#		$id =~ s/^USA//;
		# some silly input files have duplicate lines
		if (exists ($Players{$id}) and 
				$name ne $Players{$id}{name} and 
				$last_name ne $Players{$id}{last_name}) {
			print qq{$results_file: Duplicate ID ($id), different name '$name' :: '$Players{$id}{name}' and '$last_name' :: '$Players{$id}{last_name}'\n}, Dumper($Players{$id}), "\n", "\t$line\n";
			move_file($duplicateplayer);
			die qq{$results_file: parse_fix_players() failed: Duplicate ID: Player tmpid or AGAID ($id)};
		}
		$Players{$id}{name} = $name;
		$Players{$id}{last_name} = $last_name;
		$TempIDs{$tempid} = $id;
		print "=>Temp ID: $tempid: ",Dumper(\%TempIDs),"\n" if ($verbose =~ m/:parse_fix_players:/);
		print "=>Players: ",Dumper(\%Players),"\n" if ($verbose =~ m/:parse_fix_players:/);
	}
#Location: Seattle, WA
# Location: Middlebury, VT
# Tournament Location: Hacienda Hotel, El Segundoo, California
# Location="Piscataway, NJ"
	elsif (	$line =~ m/#\s*Location: (.*)/i or
		$line =~ m/#\s*Location="(.*)"/i or
		$line =~ m/[#\s]{0,}Location='(.*)'/i or
		$line =~ m/[#\s]{0,}Location="(.*)"/i or
		$line =~ m/#\s*Tournament\s+Location: (.*)/i) {
		$Tournament{location} = $1 if (length $1);
		my ($city, $state) = split /,/, $1;
		$city =~ s/^\s+//;	$city =~ s/\s+$//; $city =~ s/(\w+)/\u\L$1/g; $city =~ s/_/ /g;
		$state =~ s/^\s+//;	$state =~ s/\s+$//; $state =~ s/(\w+)/\u\u$1/g; $state =~ s/_/ /g;
		$Tournament{city} = $city;
		$Tournament{state} = $state;
		print "parse(): found location: $line; city:$city; state:$state\n" if ($verbose =~ m/:parse:/);
		print Dumper(\%Tournament), "\n"; # XXX 20130225
	}
# Base listing made by the Tournament Management Program, TourMan, v1.1
## Output Generated by MacTD.  Program copyright Jared Roach 1995-1999.
#Program: ATP V1.0 - jon@airsltd.com
# Program: WinGoTD Version 2.0
#	Tournament software was PyTD
	elsif (	$line =~ m/#\s*Program:\s*(.*)/ or
		$line =~ m/#\s*Base listing made by the Tournament Management Program,\s*(.*)/ or
		$line =~ m/#\s*Tournament software was\s*(.*)/ or
		$line =~ m/## Output Generated by (\w+)\.{0,}/ ) {
		$Tournament{program} = $1 if (length $1);
	}
#Promotor:
# Promotor: () -
	elsif ($line =~ m/#\s*Promotor:\s*(.*)/ ) {
		$Tournament{promotor} = $1 if (length $1);
	}
## RULES AGA
#	rules=AGA
	elsif (	$line =~ m/## RULES\s+(\w+)/ or
		$line =~ m/\s*rules=(\w+)/) {
		$Tournament{rules} = $1;
	}
#June 23, 2001
# Date="12/3/2006"
# Tournament Dates: October 21-22, 2000
	elsif (	$line =~ m/#\s*(\w+\s+\d{2},\s+\d{2,4})/ or
		$line =~ m/\s*Date="([-0-9\/]+)"/ or
		$line =~ m/#\s*Tournament Dates:\s+(\w+\s+\d{2}[-0-9]*,\s+\d{4})/ ) {
		$Tournament{start} = convert_date($results_file, $1, $CHECK_DAY) if (not exists $Tournament{start});
		$Tournament{stop} = convert_date($results_file, $1, $DONT_CHECK_DAY) if (not exists $Tournament{stop});
	}
#Start: 2008-01-06
# Start: 04/19/2003
# Start="01/20/2007"
# start=12/9/2007
# start=2008-01-12
	elsif (	$line =~ m/#\s*Start:\s*([-0-9\/]+)/i or
		$line =~ m/[#]*\s*start=\s*["]*([-0-9\/]+)["]*/i ) {
		$Tournament{start} = convert_date($results_file, $1, $CHECK_DAY);
	}
#Stop: 2008-01-06
# Stop: 04/19/2003
# Finish="01/21/2007"
# finish=12/9/2007
# finish=2008-01-12
	elsif (	$line =~ m/#\s*Stop:\s*([-0-9\/]+)/i or
		$line =~ m/[#]*\s*finish=["]*([-0-9\/]+)["]*/i ) {
		$Tournament{stop} = convert_date($results_file, $1, $DONT_CHECK_DAY);
	}
## TD:  Jared Roach, jedroach@alumni.washington.edu
# TD: Clay H Smith - email=71564,623
# TDs: Clay H Smith & Jason Taft
# Td: Fred Hopkins hm: 916-965-0478 wk:916-636-8758
# TD information: Larry Gross, Richard Dolen.
#TD: Don Broersma
#Tournament Director Andy Kochis, 713-335-6881
#Director: Chris Kirschner 206-579-8071 chrisk.aga@comcast.net
# Director: Peter Schumer (802) 443-5560 schumer@middlebury.edu
	elsif (	$line =~ m/##\s*TD[s]*: (.*)/i or
		$line =~ m/#\s*TD: (.*)/i or
		$line =~ m/#\s*TDs: (.*)/i or
		$line =~ m/#\s*TD information:\s*(.*)/ or
		$line =~ m/#\s*Tournament Director[:]*\s*(.*)/ or
		$line =~ m/#\s*Director:\s*(.*)/ ) {
		$Tournament{director} = $1 if (length $1);
	}
#TOURNEY	Northern California Open, NAMT/Ing Qualifier, San Francisco, CA, June 16-17, 2007
#TOURNEY Cincinnati Go Club, Winter Handicap Tournament, Cin
## TOURNEY Seattle  Winter Tournament, January 26, 2002
#Chicago UIC WINTER WARMER TOURNAMENT JANUARY 22, 1994
##S Chicago Handicap Feb 03 2/22/03 Chicago Robert Barber
##S Blue Hen Tournament 21 Oct 95 Newark DE Suzuki
#Name: Seattle Go Center January Rati
# Name: George Sporzynski Memorial
# Name="Feng Yun Youth Go Tournament, Junior Division"
#	Name="Feng Yun Go School Monthly Rated Games"
# TOURNEY Salt City Spring Tournament, Manlius Pebble Hill, Dewitt NY
	elsif (	$line =~ m/^\s*TOURNEY\s+(.*)[,]+\s+(\w+\s+\d{1,2}[,]*\s+\d{4})/i or
		$line =~ m/^\s*TOURNEY\s+(.*)[,]+\s+.*/i or
		$line =~ m/^TOURNEY\s+(.*)/i or
		$line =~ m/## TOURNEY\s+(.*)[,]+\s+(\w+\s+\d{1,2}[,]*\s+\d{4})/i or
		$line =~ m/## TOURNEY\s+(.*)[,]+\s+(\w+\s+)/i or
		$line =~ m/## TOURNEY\s+([A-Za-z0-9 ]+)/i or
		$line =~ m/#\s*(.*)[,]*\s+(\w+\s+\d{2}[,]*\s+\d{4})/ or
		$line =~ m/##S\s+(.*)\s+([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{1,4})/ or
		$line =~ m/##S\s+(.*)\s+([0-9]{1,2} Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec\w* [0-9]{2,4})/ or
		$line =~ m/#\s*Name:\s*(.*)/ or
		$line =~ m/#\s*Name="(.*)"/i or
		$line =~ m/#TOURNEY \s*label=['"](.*)['"]/i ) {
		$Tournament{name} = $1 unless (length $Tournament{name});
		$Tournament{date} = convert_date($results_file, $2, $CHECK_DAY) if (length ($2));
	}
# TOURNEY
#	label="Strong Players Open 2011"
	elsif ( $line =~ m/^\s*label=['"](.*)['"]/i ) {
		$Tournament{name} = $1 unless (length $Tournament{name});
	}
# Add a b (BYE) at end of the line for any player skips that round.'
## ATTRIBUTES tmpid id name
# Comments: Tournament Software TOURMAN by Richard Dolen
# Contact: 
# Copyright (C) by Richard Dolen, 1995
#Divisions
#Game 1^M
# Game record:
# games:
# Generated 17:25:00 10/22/00
#Group 1^M
#Note: club must be a one word with no spaces.'
# One line/player:
#Players
## Players:
# REGISTERED PLAYERS
## Results:^M
# Upper and lower case do not matter: all names get converted anyway.'
#0
# 0
###
	elsif (	$line =~ m/^\s*$/ or
		$line =~ m/^
$/ or
		$line =~ m/^$/ or
		$line =~ m/^#*$/ or
		$line =~ m/^#\s*0\s*$/ or
		$line =~ m/^# Add a b/ or
		$line =~ m/## ATTRIBUTES/ or
		$line =~ m/#AWARDS/ or
		$line =~ m/# REGISTERED PLAYERS/ or
		$line =~ m/# Comments:/ or
		$line =~ m/^[#]*\s*Contact\b/i or
		$line =~ m/# Copyright/ or
		$line =~ m/#Divisions/ or
		$line =~ m/#\s*EMail=/i or
		$line =~ m/#\s*Game/ or
		$line =~ m/[#]*\s*games[:]*/i or
		$line =~ m/# Generated/ or
		$line =~ m/#\s*Group/ or
		$line =~ m/#\s*Komi=/i or
		$line =~ m/#\s*Last/i or
		$line =~ m/#\s*Note:/ or
		$line =~ m/#\s*One line\/player:/ or
		$line =~ m/#\s*Phone=/i or
		$line =~ m/[#]*\s*Players/i or
		$line =~ m/## Results:/	or
		$line =~ m/#ROUNDS/ or
		$line =~ m/#RULES/ or
		$line =~ m/#\s*Tiebreak/ or
		$line =~ m/#TOURNEY/ or
		$line =~ m/# Upper and lower case/ ) {
		next;
	}
	else {
		print qq{Unparsed line: '$line'\n} if ($verbose =~ m/:Unparsed:/);
		print qq{Unparsed line: $file: '$line'\n} if ($verbose =~ m/:Unparsedline:/);
	}
}

sub parse_name($$$) {
	my $id = shift;
	my $line = shift;
	my $fullname = shift;

	my ($last_name, $name) = split(/,/, $fullname);
	printf "parse_name(): fullname: $fullname, last_name: $last_name, name: $name\n" if ($verbose =~ m/:parse_name:/);
	if ($last_name eq $fullname) {
		print "parse_name(): $fullname does not contain a ','...reversing\n" if ($verbose =~ m/:parse_name:/);
		$fullname =~ s/(.*)\s+([A-Za-z-']+)$/$2, $1/;
		($last_name, $name) = split(/,/, $fullname);
	}
	printf "parse_name(): fullname: $fullname, last_name: $last_name, name: $name\n" if ($verbose =~ m/:parse_name:/);
	# Remove leading spaces and trailing spaces. Capitalize name.
	$last_name =~ s/^\s+//;	$last_name =~ s/\s+$//; $last_name =~ s/(\w+)/\u\L$1/g; $last_name =~ s/_/ /g;
	$name =~ s/^\s+//;	$name =~ s/\s+$//; $name =~ s/(\w+)/\u\L$1/g; $name =~ s/_/ /g;
	print qq{Last Name: '$last_name', Name: '$name'\n} if ($verbose =~ m/:parse_name:/);
	return ($last_name, $name);
# subseceded by player_variations ? XXX XXX

	$id =~ s/^USA//;
	if (exists($TempIDs{$id})) {
		$id = $TempIDs{$id};
	}
	if (exists($Players{$id})) {
		print "parse_name(): $id: ", Dumper($Players{$id}), "\n" if ($verbose =~ m/:parse_name:/);
		if (lc($Players{$id}{last_name}) ne lc($last_name) and $exactness =~ m/:last_name:/) {
			move_file($mismatchednames);
			my @base = fileparse($ARGV[0], qr/\.[^.]*/);
			die qq{$results_file: parse_name: name ($last_name) in '$results_file' does not match name ($Players{$id}{last_name}) in '$fix_file'\n      \'$line\'\n};
		};
		if (lc($Players{$id}{name}) ne lc($name) and $exactness =~ m/:first_name:/) {
			move_file($mismatchednames);
			my @base = fileparse($ARGV[0], qr/\.[^.]*/);
			die qq{$results_file: parse_name: name ($name) in '$results_file' does not match name ($Players{$id}{name}) in '$fix_file'\n      \'$line\'\n};
		}
	}
	return ($last_name, $name);
}

sub parse_rank($) {
	my $rank = shift;
	print "parse_rank(): rank: $rank\n" if ($verbose =~ m/:parse_rank:/);
	$rank =~ s/(\d+)\.\d+([pdk])/$1$2/i;
	print "parse_rank(): rank: $rank\n" if ($verbose =~ m/:parse_rank:/);
	if ($rank =~ m/[-]*(\d+k)/i) {
		print "$rank -> m/[-]*(\d+k)/i -> $1\n" if ($verbose =~ m/:parse_rank_detail:/);
		return $1;
	}
	elsif ($rank =~ m/(\d+d)/i) {
		print "$rank -> m/(\d+d)/i -> $1\n" if ($verbose =~ m/:parse_rank_detail:/);
		return $1;
	}
	else {
		$rank = int($rank);
		print "$rank\n"		 if ($verbose =~ m/:parse_rank_detail:/);
		if ($rank < 0) {
			$rank = (-1 * $rank) . "k";
		}
		else {
			$rank = $rank . "d";
		}
		print "$rank\n" if ($verbose =~ m/:parse_rank_detail:/);
	}
	print "parse_rank(): rank: $rank\n" if ($verbose =~ m/:parse_rank:/);
	return $rank;
}

sub move_file($) {
	my $directory = shift;
#	my @base = fileparse($ARGV[0], qr/\.[^.]*/);
	my @base = fileparse($results_file, qr/\.[^.]*/);
	mkdir "$base[1]/$directory" unless (-d "$base[1]/$directory");
	die qq{$0: Can't write to "$base[1]/$directory"} unless (-w "$base[1]/$directory");
	rename "$base[1]$base[0].in", "$base[1]/$directory/$base[0].in" unless ($shoot_blanks);
	rename "$base[1]$base[0].fix", "$base[1]/$directory/$base[0].fix" unless ($shoot_blanks);
}

sub access_and_parse($$) {
	my $file = shift;
	my $open_fatal = shift;
	print "access_and_parse($file, $open_fatal)\n" if ($verbose =~ m/:access_and_parse:/);
	open FILE, "<", $file or do {
		die qq{$0: Failed to open file: $file: $!\n} if ($open_fatal);
		return;
	};
	while (my $line = <FILE>) {
		$line = prep_line($line);
		parse($file, $line);
	}
	close FILE;
}

#-------------------------------------------------------------------------------- 

parse_fix_file($fix_file);
parse_results_file($results_file, \%Tournament);
my $dbh       = getdbiconnection();
validate_players(\%Players, \%PlayersDB, \%Rejects);
generate_crosstab(\%Tournament, \%Players, \%CrosstabGames);
#create_tournament(\%Tournament, scalar(keys %Players) );
#create_games(\%Tournament, \%Players, \@Games);
update_players(\%Tournament, \%Players, \%PlayersDB, $revert);

$dbh->disconnect() unless ($shoot_blanks);

printf "File: %20s,     Tournaments: %4d,         Players: %4d,     Games: %4d\n", 
	$results_file, scalar(%Tournament), scalar(%Players), scalar(@Games);
printf "File: %20s, New Tournaments: %4d, Updated Players: %4d, New Games: %4d\n",
	$results_file, $new_tournaments, $updated_players, $new_games;

exit;


#-------------------------------------------------------------------------------- 

open RESULTS, "+<", $ARGV[0] or die "$0: Failed to open file \"$results_file\": $!\n";

exit;

sub find_next_round(*) {
	my $fh = shift;
	my $line;
	my $round;
	print "find_next_round()\n" if ($verbose =~ m/:find_next_round:/);

	while ($line = <$fh>) {
		$line = prep_line($line);
		# '# Round  1'
		# '#GAMES 1'
		if ($line =~ m/^#\s+Round\s+(\d+)|^#\s+GAMES\s+(\d+)/) {
			$round = $1;
			last;
		}
	}
	return $round;
}

sub parse_args_and_obtain_filenames() {
	my $help = 0;
	my $result = GetOptions(
			"foo|f!"	=> \$foo,
			"help|h!"	=> \$help,
			"revert|r!"	=> \$revert,
			"shoot_blanks|n:i" => \$shoot_blanks,
#			"online|n:i"	=> \$online,
			"verbose:s"	=> \$verbose,
		);

	usage() if ($help);
	usage() unless ($#ARGV == 0);
	if ($verbose =~ m/:parse_args:/) {
		print "parse_args():\n";
		print "\trevert:	$revert\n";
		print "\tshoot_blanks:	$shoot_blanks\n";
		print "\tonline:	$online\n";
		print "\tverbose:	$verbose\n";
	}

	my @base = fileparse($ARGV[0], qr/\.[^.]*/);

	opendir DIR, $base[1] or die qq{$0: Unable to open directory: $base[1]: $!};
	my @files = grep { /^$base[0]\.\w+/ and -f "$base[1]/$_" } readdir DIR;

	if ( scalar @files > 2) {
		print "Too many files: Files: ", Dumper(\@files), "\n";
		print "Too many files: base: ", Dumper(\@base), "\n";
		die qq{Too many files beginning with $base[1]$base[0]};
	}
	foreach my $file (@files) {
#		my $new = $base[1] . lc($file);
		my $new = $base[1] . $file;
		if ( not -f $new ) {
			print qq{rename "$base[1]$file", "$new"\n} if ($verbose =~ m/:parse_args:/);
			rename "$base[1]$file", "$new" unless ($shoot_blanks);
		}
	}

#	my $results_file = $base[1] . lc($base[0]) . ".in";
#	my $fix_file  = $base[1] . lc($base[0]) . ".fix";
	my $results_file = $base[1] . $base[0] . ".in";
	my $fix_file  = $base[1] . $base[0] . ".fix";

	$Tournament{code} = lc($base[0]);

	print "Fix:$fix_file, Results:$results_file\n" if ($verbose =~ m/:parse_args:/);

	return ($fix_file, $results_file);
}

sub usage() {
	print "<progname>:\n";
	print "\thelp (-h)		print this message\n";
	print "\trevert (-r)		remove the tournament from the database\n";
	print "\tshoot_blanks (-n)	do NOT modify the database unless this value is 0\n";
	print "\tonline (-o)		online tournament, not to be rated\n";
	print "\t			default value is $shoot_blanks\n";
	print "\tverbose (-s)		print additional debugging messages as selected\n";
	print "\t			example: \"-s :parse_fix_file:\"\n";
	die "$0: no filename\n";
}

sub parse_fix_file($$) {
	my $f = shift;
	print "parse_fix_file()\n" if ($verbose =~ m/:parse_fix_file:/);

	open FIX, "<", $f or die "$0: Failed to open fix file: \"$f\": $!\n";
	
	parse_header(\%Tournament, *FIX{IO});
	parse_fix_players(\%Players, \%TempIDs, *FIX{IO});

	print "Tournament header: ", Dumper(\%Tournament) if ($verbose =~ m/:parse_fix_file:/);
	print "TempIDs: ", Dumper(\%TempIDs) if ($verbose =~ m/:parse_fix_file:/);

	close FIX;
}

# THERE MUST be a blank line between the header and the tournaments results, 
# lest this function discard the first results line
sub parse_header($*) {
	my $Tournament = shift;
	my $fh = shift;
	my $line;
	print "parse_header()\n" if ($verbose =~ m/:parse_header:/);

	while ($line = <$fh>) {
		$line = prep_line($line);
		print qq{line: '$line'\n} if ($verbose =~ m/:parse_header:/);
		last unless ($line =~ m/^#\s*/);

		#Program: ATP V1.0 - jon@airsltd.com
		if ($line =~ s/#\s*Program:\s*(.*)/$1/) {
			die qq{$results_file: parse_header() Header does not match: "$Tournament->{program}" :: "$line"\n}
				if (exists $Tournament->{program} and $Tournament->{program} != $line);
			$Tournament->{program} = $line;
		} 
		elsif ($line =~ s/#\s*Name:\s*(.*)/$1/) {
			die qq{$results_file: parse_header() Header does not match: "$Tournament->{name}" :: "$line"\n}
				if (exists $Tournament->{name} and $Tournament->{name} != $line);
			$Tournament->{name} = $line;
		}
		elsif ($line =~ s/#\s*Location:\s*(.*)/$1/) {
			die qq{parse_header() Header does not match: "$Tournament->{location}" :: "$line"\n}
				if (exists $Tournament->{location} and $Tournament->{location} != $line);
			$Tournament->{location} = $line;
		}
		elsif ($line =~ s/#\s*Start:\s*(.*)/$1/) {
			my ($month, $day, $year);
			if ($line =~ m/\d{1,2}\/\d{1,2}\/\d\d\d\d/) {
				($month, $day, $year) = split(/\//, $line);
			}
			elsif ($line =~ m/\d{1,2}\/\d{1,2}\/\d\d/) {
				($month, $day, $year) = split(/\//, $line);
				if ($year > 50) {
					$year = 1900 + $year;
				}
				else {
					$year = 2000 + $year;
				}
			}
			elsif ($line =~ m/\d\d\d\d-\d\d-\d\d/) {
				($year, $month, $day) = split(/-/, $line);
			}
			$line = sprintf "%4d-%02d-%02d", $year, $month, $day;
			die qq{parse_header() Header does not match: "$Tournament->{start_date}" :: "$line"\n} 
				if (exists $Tournament->{start_date} and $Tournament->{start_date} != $line);
			$Tournament->{start_date} = $line;
		}
		elsif ($line =~ s/#\s*Stop:\s*(.*)/$1/) {
			my ($month, $day, $year);
			if ($line =~ m/\d\d\/\d\d\/\d\d\d\d/) {
				($month, $day, $year) = split(/\//, $line);
			}
			elsif ($line =~ m/\d\d\d\d-\d\d-\d\d/) {
				($year, $month, $day) = split(/-/, $line);
			}
			$line = sprintf "%4d-%02d-%02d", $year, $month, $day;
			die qq{parse_header() Header does not match: "$Tournament->{stop_date}" :: "$line"\n}
				if (exists $Tournament->{stop_date} and $Tournament->{stop_date} != $line);
			$Tournament->{stop_date} = $line;
		}
		elsif ($line =~ s/#\s*Director:\s*(.*)/$1/) {
			die qq{$results_file: parse_header() Header does not match: "$Tournament->{director}" :: "$line"\n}
				if (exists $Tournament->{director} and $Tournament->{director} != $line);
			$Tournament->{director} = $line;
		}
		elsif ($line =~ s/#\s*Promotor:\s*(.*)/$1/) {
			die qq{$results_file: parse_header() Header does not match: "$Tournament->{promotor}" :: "$line"\n}
				if (exists $Tournament->{promotor} and $Tournament->{promotor} != $line);
			$Tournament->{promotor} = $line;
		}
	}
		die "$results_file: parse_header() failed: ?? Missing blank line between header and player list ??\n            \'$line\'\n" unless ($line =~ m/^\s*$/);
}

sub parse_fix_players($$*) {
	my $Players = shift;
	my $TempIDs = shift;
	my $fh = shift;
	my $line;
	print "parse_fix_players()\n" if ($verbose =~ m/:parse_fix_players:/);

        utf8::decode($line);    # 20150425
	while ($line = <$fh>) {
		$line = prep_line($line);
		next if ($line =~ m/^#/);

		$line =~ s/, /,/;
		my ($tempid, $id, @etc) = split(/ /, $line);
		my $fullname = join ' ', @etc;
		# print "Temp ID: $tempid, ID: $id, Full Name: $fullname\n"; if ($verbose =~ m/:parse_fix_players:/);
		my ($last_name, $name) = split(/,/, $fullname);
		# print "Last Name: $last_name, Name: $name\n" if ($verbose =~ m/:parse_fix_players:/);
		$id =~ s/^USA//;
		die "$0: parse_fix_players() failed: Player tmpid or id number listed twice" if (exists ($Players->{$id}));
		$Players->{$id}{name} = $name;
		$Players->{$id}{last_name} = $last_name;
		$TempIDs->{$tempid} = $id;
	}
}

# THERE MUST be a blank line between the player list and the tournaments results, 
# lest this function discard the first results line
sub parse_players($$*) {
	my $Players = shift;
	my $TempIDs = shift;
	my $fh = shift;
	my $line;
	print "parse_players()\n" if ($verbose =~ m/:parse_players:/);

	while ($line = <$fh>) {
		$line = prep_line($line);
		last if ($line =~ m/^\s*$/);
		next if ($line =~ m/^#/);
		$line =~ s/, /,/;

#		11948   Brownell, Landon        7d
#		13593 Zhuang,Guozhong 6.5
# 		6623 Conyngham,Jim 9K none
#		tmp3 Dewey,Brian -17.5 CLUB=SEAG
		$line =~ s/^(tmp[0-9]+|[0-9]+)\s(.+)\s([-]*[0-9]+\.[0-9]+|[0-9]+[PDKpdk]).*$/$1|$2|$3/;

		my ($id, $fullname, $rank) = split /\|/, $line;		# 2894 St. Stringfellow Jr. Esq., Steve C. 6D
		print "ID: $id, Fullname: $fullname, Rank: $rank\n" if ($verbose =~ m/:parse_players:/);

		my ($last_name, $name) = split(/,/, $fullname);

		print "Last Name: $last_name, Name: $name\n" if ($verbose =~ m/:parse_players:/);
		$id =~ s/^USA//;
		if (exists($TempIDs->{$id})) {
			$id = $TempIDs->{$id};
		}
		if (exists($Players->{$id})) {
			die qq{$0: parse_players: family name ($last_name) in '.in' file does not match name ($Players->{$id}{last_name}\n      \'$line\'\n) in '.fix' file} if ($Players->{$id}{last_name} ne $last_name);
			die qq{$0: parse_players: name ($name) in '.in' file does not match name ($Players->{$id}{name}) in '.fix' file\n      \'$line\'\n} if ($Players->{$id}{name} ne $name);
		}
		else {
			$Players->{$id}{name} = $name;
			$Players->{$id}{last_name} = $last_name;
		}
		if ($rank =~ m/[-]*[0-9]+\.[0-9]+/) {
			$rank = int($rank);
			if ($rank < 0) {
				$rank = (-1 * $rank) . "k";
			}
			else {
				$rank = $rank . "d";
			}
		}
		$Players->{$id}{rank} = lc($rank);	# for ranks from [PDKpdk] matches
	}
	print Dumper(\%Players) if ($verbose =~ m/:parse_players:/);
}

sub parse_results_file($$) {
	my $f = shift;
	my $Tournament = shift;
	my %T;

	print "parse_results_file()\n" if ($verbose =~ m/:parse_results_file:/);

	open RESULTS, "<", $f or die "$0: Failed to open fix file: \"$f\": $!\n";
	
	$T{code} = $Tournament->{code};
	parse_header(\%T, *RESULTS{IO});
	print "T: ", Dumper(\%T), "\n", if ($verbose =~ m/:parse_results_file:/);
	foreach my $key (keys %{$Tournament} ) {
		die qq{IN file and FIX headers different: "$key" -> "$T{code}" vs "$Tournament->{code}"}
			unless ($T{code} eq $Tournament->{code});
	}
	parse_players(\%Players, \%TempIDs, *RESULTS{IO});

#		my $round = find_next_round(*RESULTS{IO});
	while (my $line = <RESULTS>) {
		my $round;
		$line = prep_line($line);
		if ($line =~ m/^#\s+Round\s+(\d+)/) {
			$round = $1;
			$Tournament{rounds} = parse_round(\%Players, \%TempIDs, \@Games, \%CrosstabGames, $round, *RESULTS{IO});
		}
	}
	print Dumper(\@Games) if ($verbose =~ m/:parse_results_file:/);
	
	close RESULTS;
}

sub prep_line($) {
	my $line = shift;
	chomp $line;
#	$line =~ s/[ ]+/ /g;
#	$line =~ s/[	]+/ /g;
	$line =~ s/^\s+//;
	$line =~ s/\s+$//;
	$line =~ s/\s+/ /g;
	$line =~ s/
//;
	return ($line);
}

sub parse_round($$$$$*) {
	my $Players = shift;
	my $TempIDs = shift;
	my $Games = shift;
	my $CrosstabGames = shift;
	my $round = shift;
	my $fh = shift;
	my $line;
	my $first_line = 1;
	print "parse_round()\n" if ($verbose =~ m/:parse_round:/);

	while ($line = <$fh>) {
		$line = prep_line($line);
		next if ($line =~ m/^#/);
		last if ($line =~ m/^\s*$/ and $first_line != 1);			# XXX
		$first_line = 0;
		next if ($line =~ m/^\s*$/);
		my ( $white, $black, $result, $handicap, $komi) = split(/ /, $line);
		if (exists($TempIDs->{$white})) {
			$white = $TempIDs->{$white};
		}
		if (exists($TempIDs->{$black})) {
			$black = $TempIDs->{$black};
		}
		die "$results_file: parse_round(): Player $white from round $round not found in player list\n"
			unless (exists($Players->{$white}));
		die "$results_file: parse_round(): Player $black from round $round not found in player list\n"
			unless (exists($Players->{$black}));
		die "$results_file: parse_round(): Unknown result: $result\n" 
			unless ($result eq "B" or $result eq "b" or $result eq "W" or $result eq "w");
		die "$results_file: parse_round(): Unknown handicap: $handicap\n" 
			unless ($handicap > $MIN_HANDICAP and $handicap < $MAX_HANDICAP);
		die "$results_file: parse_round(): Unknown komi: $komi\n" 
			unless ($komi > $MIN_KOMI and $komi < $MAX_KOMI);
		die "$results_file: parse_round(): White Player($Players->{$white}) same as Black Player($Players->{$black})\n"
			if ($Players->{$white} == $Players->{$black});
		$Players->{$white}{tot_games} = $Players->{$white}{tot_games} + 1;
		$Players->{$black}{tot_games} = $Players->{$black}{tot_games} + 1;
		print Dumper(\$Players->{$white}) if ($verbose =~ m/:parse_round:/);
		print Dumper(\$Players->{$black}) if ($verbose =~ m/:parse_round:/);
		print qq{'$line' vs '$white', '$black', '$result', '$handicap', '$komi'\n} 
			if ($verbose =~ m/:parse_round:/);
		my $line = "$white $black $result $handicap $komi";
		push @{$Games[$round-1]}, $line;

		$CrosstabGames{$round-1}{$white}{opponent} = $black;
		$CrosstabGames{$round-1}{$white}{color} =  "w";
		$CrosstabGames{$round-1}{$black}{handicap} =  0;

		$CrosstabGames{$round-1}{$black}{opponent} = $white;
		$CrosstabGames{$round-1}{$black}{color} =  "b";
		$CrosstabGames{$round-1}{$black}{handicap} =  $handicap;

		if  ($result eq "W" or $result eq "w")  {
			$Players->{$white}{score}++;
			$CrosstabGames{$round-1}{$white}{result} = "+";
		}
		else {
			$CrosstabGames{$round-1}{$white}{result} = "-";
		}
		if  ($result eq "B" or $result eq "b")  {
			$Players->{$black}{score}++;
			$CrosstabGames{$round-1}{$black}{result} = "+";
		}
		else {
			$CrosstabGames{$round-1}{$black}{result} = "-";
		}
#		$CrosstabGames{$round-1}{$white}{result} = ($result eq "W" or $result eq "w") ? "+" : '-';
#		$CrosstabGames{$round-1}{$black}{result} = ($result eq "B" or $result eq "b") ? "+" : '-';
	}
	return $round;
}

sub validate_players($$$$) {
	my $Players = shift;
	my $PlayersDB = shift;
	my $Rejects = shift;
	return if ($shoot_blanks);
	print "validate_players()\n" if ($verbose =~ m/:validate_players:/);

	# fetch every player from the database
	# acceptable till the database reaches a certain size
	# currently 2009...about 20,000 players
	my $query = qq{ SELECT Pin_Player, Last_Name, Name, Tot_Tournaments, Tot_Games FROM players };
	my $query = qq{ SELECT m.member_id, m.family_name, m.given_names, p.Tot_Tournaments, p.Tot_Games FROM members m, players p WHERE m.member_id = p.Pin_Player };		# 20151109
	my $sth = $dbh->prepare($query) || die "validate_players(): $query\n $DBI::errstr";
	$sth->execute() || die "validate_players(): $query\n $DBI::errstr";
	while (my @fetch = $sth->fetchrow_array()) {
		print "Pin_Player: $fetch[0], Last Name: $fetch[1], Name: $fetch[2], Tournaments: $fetch[3], Games: $fetch[4]\n";
		$PlayersDB->{$fetch[0]}{last_name} = $fetch[1];
		$PlayersDB->{$fetch[0]}{name} = $fetch[2];
		$PlayersDB->{$fetch[0]}{tot_tournaments} = $fetch[3];
		$PlayersDB->{$fetch[0]}{tot_games} = $fetch[4];
	}

	# fetch last tournament for each player
	my $query = qq{ SELECT p.Pin_Player, t.Tournament_Date FROM players p, tournaments t WHERE p.Last_Appearance = t.Tournament_Code };
	my $sth = $dbh->prepare($query) || die "validate_players(): $query\n $DBI::errstr";
	$sth->execute() || die "validate_players(): $query\n $DBI::errstr";
	while (my @fetch = $sth->fetchrow_array()) {
		#print "Pin_Player: $fetch[0], Tournament Date: $fetch[1]\n";
		$PlayersDB->{$fetch[0]}{last_appearance_date} = $fetch[1];
	}

	foreach my $id (keys %{$Players}) {
		if (not exists $PlayersDB->{$id} or not defined $PlayersDB->{$id}) {
			$Rejects->{$id} = 1;
			print "$results_file: validate_players(): Player \'$id\' does not exist in database\n";
			die;
			next;
		}
		if ($Players->{$id}{last_name} ne $PlayersDB->{$id}{last_name}) {
			$Rejects->{$id} = 1;
			print "$results_file: validate_players(): Player does not match database: \'$Players->{$id}{last_name}\' vs \'$PlayersDB->{$id}{last_name}\'\n";
		}
	}
	print Dumper($Players) if($verbose =~ m/:validate_players:/);
	print "Reject: ".Dumper($Rejects) if($verbose =~ m/:validate_players:/);
}

my $fix_file_sample = q{
# Program:  WinGoTD Version 2.0
# Name:     Portland Fall 2005
# Location: Lewis and Clark College
# Start:    10/22/2005
# Stop:     10/23/2005
# Director: Glenn Peters                       (503) 771-3233 glenn@aenigma.com
# Promotor: Peter Drake                        () -

## ATTRIBUTES tmpid id name
99969   USA13955        Chen, Yung-Pin
99968   USA13956        Strong, Katie
99966   USA13957        Mullinax, Michel
99967   USA13958        Walling, Steven
99970   USA13959        Calder, Merrick
};

my $results_file_example = q{
# Program:  WinGoTD Version 2.0
# Name:     Portland Fall 2005
# Location: Lewis and Clark College
# Start:    10/22/2005
# Stop:     10/23/2005
# Director: Glenn Peters                       (503) 771-3233 glenn@aenigma.com
# Promotor: Peter Drake                        () - 
 
 2802 Boley, Jon                           6D
 2894 Stringfellow, Steve C.               6D
12376 Smith, Ryan                          4D
 7752 Burnham, Nial                        4D
13216 Fu, Zhe                              3D
 9412 Tsukamoto, Masaya                    2D
 3658 Levenick, Jim                        2D
 6079 Hodges, Tom                          1D
11728 Bazzano, Justin                      1K
13527 Negishi, Akane                       3K
 9473 Peters, Glenn                        3K
 7647 O'Malley, Robert                     3K
 2275 Kraft, Barry                         4K
13220 Britt, Phil                          5K
10332 Howard, John                         6K
10283 Malveaux, Mike                       6K
 7046 Collins, Truman                      6K
 7759 Riehl, Dave                          7K
13499 Reimer, Chance                       7K
 7750 Drake, Peter                         8K
10123 Long, Kevin                          8K
 6096 Brown, Frank                         8K
11655 Sharvy, Ben                          8K
99969 Chen, Yung-Pin                       9K
10209 Lassahn, Jeff                        9K
13889 Gehrkin, Jonathan                    9K
 3049 Lynch, Kevin                         9K
11400 Jiang, Hongda                       10K
12298 Gum, Josh                           10K
11278 Gaty, Cynthia                       13K
12647 Tomlin, Brett                       14K
11539 Levenick, Sam                       16K
 2493 Hall, Jim                           18K
 3132 Solovay, Robert                     18K
99968 Strong, Katie                       20K
13911 Nelson, Heber                       24K
99966 Mullinax, Michel                    25K
99967 Walling, Steven                     26K
13921 Gibbon, Kimberley                   27K
99970 Calder, Merrick                     28K
13568 Reimer, Cyndi                       30K
 
# END_PLAYER_LIST
 
# RULES AGA
 
 
# Round  1
 
 2802 12376 B 2 0
 2894  7752 B 0 0
13216  3658 W 0 7
 9412  6079 B 0 0
11728  9473 W 0 0
13220 13527 B 2 0
 7647 10332 W 0 0
13889  2275 B 0 0
13499  7046 W 0 0
10283  7759 W 0 0
10123  6096 W 0 7
11655  3049 W 0 7
10209 11278 W 0 0
99969 12298 W 0 0
12647 11539 W 0 0
 2493 99968 W 5 0
13911 99967 W 2 0
99966 13921 B 0 0
99970 13568 B 0 0
 
# Round  2
 
 2802  7752 B 0 0
 2894 13216 W 2 0
12376  9412 W 2 0
 3658 11728 B 0 0
 6079 13220 B 0 0
 9473  7647 B 0 7
13527  2275 B 0 0
10332 13499 B 0 7
13889 10283 B 0 0
 7046 10123 W 0 0
 7759  7750 B 0 0
 6096 11655 B 0 7
 3049 10209 B 0 7
12298  2493 W 3 0
12647  3132 W 4 0
11539 99968 W 4 0
13921 99970 W 2 0
99967 13568 W 2 0
 
# Round  3
 
 2802 13216 B 2 0
 2894 12376 W 0 0
 7752  9412 B 2 0
 3658  6079 W 0 7
11728 13220 W 0 0
 7647 13527 B 0 7
 9473  2275 W 0 0
13499 10283 W 0 0
13889  7046 W 0 -5
10332  7759 B 2 0
10209 10123 W 0 7
11655 12647 B 0 7
 6096 11400 W 0 0
 3049 11278 B 0 0
12298 11539 W 4 0
 2493  3132 W 3 0
99968 99967 B 5 0
13911 99970 W 5 0
13921 13568 W 3 0
 
# Round  4
 
 7752 11728 W 2 0
13499  7759 W 3 0
10283 12647 B 2 0
 7046 10209 B 0 0
11539 13921 W 9 0
 2894  9412 W 3 0
12376 13216 W 0 0
13220  7647 W 0 0
 2275 10332 B 0 0
99969 11655 W 0 7
11400  2493 B 4 0
99967 99970 W 2 0
 2802  3658 W 3 0
 6079  9473 W 0 -5
10123  3049 B 0 0
 6096 11278 W 0 0
99968 13568 W 8 0
 
# Round  5
 
 2894  2802 W 0 7
12376  7752 B 0 7
13216  9412 W 0 0
 3658 13220 W 0 -7
11728  6079 W 0 7
13527  9473 B 0 7
 7647 13499 B 0 7
 2275 10283 B 0 -7
10332 12647 W 0 -7
 7759  7046 B 0 7
 7750 10209 B 0 7
11655 10123 B 0 7
 3049  6096 W 0 7
11278 11400 B 0 7
 2493 11539 W 0 -7
 3132 13568 W 9 0
99968 99970 W 7 0
99967 13921 W 0 7
 
# END
# AGA - One Tournament Fee
# AGA Fee:     5
# AGA Number:  12376
# Name:        Smith, Ryan
# Address 1:   
# Address 2:   
# City:        
# State:       
# Zip:         
# AGA Club:    PORT
 
# AGA - One Tournament Fee
# AGA Fee:     5
# AGA Number:  7752
# Name:        Burnham, Nial
# Address 1:   
# Address 2:   
# City:        
# State:       
# Zip:         
# AGA Club:    PORT
 
# AGA - One Tournament Fee
# AGA Fee:     5
# AGA Number:  99969
# Name:        Chen, Yung-Pin
# Address 1:   Mathematical Sciences
# Address 2:   Lewis and Clark
# City:        Portland
# State:       OR
# Zip:         97219
# Work Email:  ychen@lclark.edu
# AGA Club:              
 
# AGA - Full Membership
# AGA Fee:     30
# AGA Number:  11400
# Name:        Jiang, Hongda
# Address 1:   MSC 238
# Address 2:   0615 SW Palatine Hill Road
# City:        Portland
# State:       OR
# Zip:         97219
# AGA Club:    GOOR
 
# AGA - One Tournament Fee
# AGA Fee:     5
# AGA Number:  12647
# Name:        Tomlin, Brett
# Address 1:   
# Address 2:   
# City:        
# State:       
# Zip:         
# AGA Club:    LCCG
 
# AGA - Youth Membership
# AGA Fee:     10
# AGA Number:  11539
# Name:        Levenick, Sam
# Address 1:   
# Address 2:   
# City:        
# State:       
# Zip:         
# AGA Club:    CORV
 
# AGA - One Tournament Fee
# AGA Fee:     5
# AGA Number:  99968
# Name:        Strong, Katie
# Address 1:   3449 SE 8th
# Address 2:   
# City:        Portland
# State:       OR
# Zip:         97202
# Home Email:  strong@lclark.edu
# AGA Club:              
 
# AGA - One Tournament Fee
# AGA Fee:     5
# AGA Number:  99966
# Name:        Mullinax, Michel
# Address 1:   4648 SW 39th Dr
# Address 2:   
# City:        Portland
# State:       OR
# Zip:         97206?
 
# AGA - One Tournament Fee
# AGA Fee:     5
# AGA Number:  99967
# Name:        Walling, Steven
# Address 1:   to come
# Address 2:   
# City:        portland
# State:       or
# Zip:         97205
# AGA Club:              
 
# AGA - One Tournament Fee
# AGA Fee:     5
# AGA Number:  99970
# Name:        Calder, Merrick
# Address 1:   6344 N. Michigan Ave.
# Address 2:   
# City:        Portland
# State:       OR
# Zip:         97217
# AGA Club:              
 

};
